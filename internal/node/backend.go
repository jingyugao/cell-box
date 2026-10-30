// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/homevolume"
	"github.com/jingyugao/cell-box/internal/inventory"
	"github.com/jingyugao/cell-box/internal/objectstorage"
	"github.com/jingyugao/cell-box/internal/portwait"
	"github.com/jingyugao/cell-box/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	core "k8s.io/api/core/v1"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type Backend struct {
	Base, Runsc, Root    string
	Shim, RuntimeHandler string
	Socket               string
	// HostMountNamespace runs runsc in the node's mount/root namespace when the
	// controller itself is running in a hostPID privileged DaemonSet Pod.
	HostMountNamespace bool
	Runtime            cri.RuntimeServiceClient
	Images             cri.ImageServiceClient
	connection         *grpc.ClientConn
	portsMu            sync.Mutex
	sandboxPIDs        map[string]int
	ports              *portwait.Monitor
	closed             bool
	// Objects stores archive payloads. Checkpoint payloads remain on their
	// assigned node under Base; Checkpoints stores only their inventory records.
	Objects     objectstorage.Objects
	Checkpoints inventory.CheckpointStore
}

func (b *Backend) runtimeHandler() string {
	if b.RuntimeHandler != "" {
		return b.RuntimeHandler
	}
	return DefaultRuntimeHandler
}

func (b *Backend) shim() string {
	if b.Shim != "" {
		return b.Shim
	}
	return DefaultShim
}

func New(socket string) (*Backend, error) {
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Backend{Base: DefaultBase, Runsc: DefaultRunsc, Root: DefaultRoot, Shim: DefaultShim, RuntimeHandler: DefaultRuntimeHandler, Socket: socket, Runtime: cri.NewRuntimeServiceClient(conn), Images: cri.NewImageServiceClient(conn), connection: conn}, nil
}

func (b *Backend) Close() error {
	b.portsMu.Lock()
	b.closed = true
	if b.ports != nil {
		_ = b.ports.Close()
	}
	b.portsMu.Unlock()
	if b.connection != nil {
		return b.connection.Close()
	}
	return nil
}
func (b *Backend) command(ctx context.Context, args ...string) error {
	commandName := "unknown"
	if len(args) > 0 {
		commandName = args[0]
	}
	ctx, span := telemetry.Start(ctx, "node.runsc", attribute.String("runsc.command", commandName))
	var resultErr error
	defer func() { telemetry.End(span, resultErr) }()
	timeout := 45 * time.Second
	if len(args) > 0 && args[0] == "checkpoint" {
		// Saving several GiB of pages can outlast the ordinary runtime command
		// budget. Killing the CLI while the sandbox is still saving leaves an
		// ambiguous .pending checkpoint and prevents subsequent recovery.
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	command := b.Runsc
	commandArgs := append([]string{"--root=" + b.Root}, args...)
	if len(args) > 1 && len(args[len(args)-1]) == 64 && ValidID(args[len(args)-1]) {
		dir := filepath.Join(b.Base, "logs", args[len(args)-1])
		if err := os.MkdirAll(dir, 0700); err != nil {
			resultErr = fmt.Errorf("create controller runtime logs: %w", err)
			return resultErr
		}
		commandArgs = append([]string{"--debug-log=" + filepath.Join(dir, "controller.%TIMESTAMP%.%COMMAND%.log"),
			"--panic-log=" + filepath.Join(dir, "controller-panic.%TIMESTAMP%.%COMMAND%.log")}, commandArgs...)
	}
	if b.HostMountNamespace {
		command = "nsenter"
		commandArgs = append([]string{"--target=1", "--mount", "--root", "--", b.Runsc}, commandArgs...)
	}
	c := exec.CommandContext(ctx, command, commandArgs...)
	c.WaitDelay = 2 * time.Second
	output, err := c.CombinedOutput()
	if err != nil {
		resultErr = fmt.Errorf("runsc %s: %w: %.2000s", commandName, err, output)
		return resultErr
	}
	return nil
}
func (b *Backend) sandboxes(ctx context.Context, p *core.Pod) ([]*cri.PodSandbox, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	res, err := b.Runtime.ListPodSandbox(ctx, &cri.ListPodSandboxRequest{Filter: &cri.PodSandboxFilter{LabelSelector: map[string]string{"io.kubernetes.pod.uid": string(p.UID)}}})
	if err != nil {
		return nil, err
	}
	var out []*cri.PodSandbox
	for _, s := range res.Items {
		m := s.Metadata
		if m == nil || m.Uid != string(p.UID) || m.Namespace != p.Namespace || m.Name != p.Name {
			return nil, fmt.Errorf("%w: CRI sandbox identity mismatch", ErrExecutionLost)
		}
		if s.RuntimeHandler != b.runtimeHandler() {
			return nil, fmt.Errorf("%w: %w (handler %q)", ErrExecutionLost, ErrForeignRuntime, s.RuntimeHandler)
		}
		out = append(out, s)
	}
	return out, nil
}
func (b *Backend) imageID(ctx context.Context, image string) (string, error) {
	resolved, err := b.imageStatus(ctx, image)
	if err != nil {
		return "", err
	}
	return resolved.Id, nil
}
func (b *Backend) imageStatus(ctx context.Context, image string) (*cri.Image, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	res, err := b.Images.ImageStatus(ctx, &cri.ImageStatusRequest{Image: &cri.ImageSpec{Image: image}})
	if err != nil {
		return nil, err
	}
	if res.Image == nil || res.Image.Id == "" {
		return nil, fmt.Errorf("image not present locally: %s", image)
	}
	return res.Image, nil
}
func runningImageMatches(actual string, image *cri.Image) bool {
	actual = strings.TrimPrefix(actual, "containerd://")
	if actual == image.Id {
		return true
	}
	for _, digest := range image.RepoDigests {
		if actual == digest {
			return true
		}
	}
	return false
}
func (b *Backend) Checkpoint(ctx context.Context, r *api.ResumablePod, p *core.Pod) (resultErr error) {
	ctx, span := telemetry.Start(ctx, "node.checkpoint")
	defer func() { telemetry.End(span, resultErr) }()
	mounts, err := b.prepareVolumeMounts(r, false)
	if err != nil {
		return err
	}
	path, err := SnapshotPath(b.Base, string(r.UID), r.Status.Snapshot)
	if err != nil {
		return err
	}
	if _, err = os.Stat(path); err == nil {
		m, err := VerifyCached(path, string(r.UID), r.Status.SpecHash, b.Runsc)
		if err != nil {
			return err
		}
		if err = b.verifySnapshotHome(r, m); err != nil {
			return err
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	pending := path + ".pending"
	// Never repeat a destructive checkpoint after an ambiguous process crash.
	if _, err = os.Stat(pending); err == nil {
		return fmt.Errorf("interrupted checkpoint: pending directory requires inspection; refusing replay")
	}
	ss, err := b.sandboxes(ctx, p)
	if err != nil {
		return err
	}
	var live []*cri.PodSandbox
	for _, s := range ss {
		if s.State == cri.PodSandboxState_SANDBOX_READY {
			live = append(live, s)
		}
	}
	if len(live) != 1 {
		return fmt.Errorf("expected one ready sandbox, got %d", len(live))
	}
	image, err := b.imageStatus(ctx, r.Spec.Container.Image)
	if err != nil {
		return err
	}
	// Detect local tag replacement since the running container was created.
	// Kubernetes may report a repository manifest digest, while CRI Image.Id is
	// the config digest. Both must resolve to this same local image.
	if len(p.Status.ContainerStatuses) != 1 {
		return fmt.Errorf("missing container status")
	}
	actual := p.Status.ContainerStatuses[0].ImageID
	if !runningImageMatches(actual, image) {
		return fmt.Errorf("running image %s differs from local image %s", actual, image.Id)
	}
	binaryHash, err := Digest(b.Runsc)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(pending, 0700); err != nil {
		return err
	}
	if err = AtomicJSON(filepath.Join(pending, "intent.json"), map[string]string{"podUID": string(p.UID), "sandboxID": live[0].Id}); err != nil {
		return err
	}
	if err = b.command(ctx, "checkpoint", "--image-path="+pending, live[0].Id); err != nil {
		return err
	}
	checkpointMounts := uniqueVolumeMounts(mounts)
	if r.Spec.Mounts == nil && len(checkpointMounts) > 1 {
		checkpointMounts = checkpointMounts[:1]
	}
	for _, mount := range checkpointMounts {
		if mount.Legacy {
			err = homevolume.Checkpoint(b.Base, mount.UID, mount.HomeID, string(p.UID), r.Status.Snapshot)
		} else {
			err = homevolume.SyncVolume(b.Base, mount.UID, r.Spec.NodeName, mount.HomeID)
		}
		if err != nil {
			return fmt.Errorf("checkpoint volume %s: %w", mount.UID, err)
		}
	}
	manifest := Manifest{OwnerUID: string(r.UID), SpecHash: r.Status.SpecHash, ImageID: image.Id, RunscHash: binaryHash}
	if r.Spec.Mounts != nil {
		manifest.Mounts = mounts
	} else if len(mounts) > 0 {
		manifest.HomeID = mounts[0].HomeID
	}
	if err = Seal(pending, manifest); err != nil {
		return err
	}
	if err = os.Rename(pending, path); err != nil {
		return err
	}
	if err = SyncDir(filepath.Dir(path)); err != nil {
		return err
	}
	if _, err = VerifyCached(path, string(r.UID), r.Status.SpecHash, b.Runsc); err != nil {
		return err
	}
	return nil
}

func (b *Backend) verifySnapshotImage(ctx context.Context, r *api.ResumablePod, path string) (*Manifest, error) {
	m, err := VerifyCached(path, string(r.UID), r.Status.SpecHash, b.Runsc)
	if err != nil {
		return nil, err
	}
	if err = b.verifySnapshotHome(r, m); err != nil {
		return nil, err
	}
	id, err := b.imageID(ctx, r.Spec.Container.Image)
	if err != nil {
		return nil, err
	}
	if id != m.ImageID {
		return nil, fmt.Errorf("image changed since checkpoint")
	}
	return m, nil
}

func (b *Backend) verifySnapshotHome(r *api.ResumablePod, m *Manifest) error {
	if r.Spec.Mounts != nil {
		mounts, err := b.prepareVolumeMounts(r, false)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(m.Mounts, mounts) || m.HomeID != "" {
			return fmt.Errorf("checkpoint volume mount identity mismatch")
		}
		for _, mount := range uniqueVolumeMounts(mounts) {
			if mount.Legacy {
				if err := homevolume.ValidateCheckpoint(b.Base, mount.UID, mount.HomeID, r.Status.Snapshot); err != nil {
					return err
				}
			} else if _, err := homevolume.Verify(b.Base, mount.UID, r.Spec.NodeName, homevolume.VolumeSpecHash, mount.HomeID); err != nil {
				return fmt.Errorf("checkpoint volume %s identity mismatch: %w", mount.UID, err)
			}
		}
		return nil
	}
	mounts, err := b.prepareVolumeMounts(r, false)
	if err != nil {
		return err
	}
	var homeID string
	if len(mounts) > 0 {
		homeID = mounts[0].HomeID
	}
	if m.HomeID != homeID || len(m.Mounts) != 0 {
		return fmt.Errorf("checkpoint persistent HOME identity mismatch")
	}
	if homeID != "" {
		if mounts[0].Legacy {
			return homevolume.ValidateCheckpoint(b.Base, mounts[0].UID, homeID, r.Status.Snapshot)
		}
		_, err := homevolume.Verify(b.Base, mounts[0].UID, r.Spec.NodeName, homevolume.VolumeSpecHash, homeID)
		return err
	}
	return nil
}

func (b *Backend) prepareSnapshot(ctx context.Context, r *api.ResumablePod) error {
	if r.Status.Snapshot != "" {
		if b.snapshotConsumed(string(r.UID), r.Status.Snapshot) {
			return fmt.Errorf("checkpoint was consumed by a previous execution")
		}
		path, err := SnapshotPath(b.Base, string(r.UID), r.Status.Snapshot)
		if err != nil {
			return err
		}
		if _, err = os.Stat(path); err != nil {
			return fmt.Errorf("node-local checkpoint unavailable: %w", err)
		}
		if _, err = b.verifySnapshotImage(ctx, r, path); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backend) Prepare(ctx context.Context, r *api.ResumablePod, p *core.Pod) (resultErr error) {
	mounts, err := b.prepareVolumeMounts(r, r.Status.Cycle == 1 && r.Status.Snapshot == "")
	if err != nil {
		return err
	}
	if err := b.prepareSnapshot(ctx, r); err != nil {
		return err
	}
	ticket := Ticket{OwnerUID: string(r.UID), NodeName: r.Spec.NodeName, HomeVolumeUID: r.Spec.HomeVolumeUID, PodUID: string(p.UID), Namespace: p.Namespace, PodName: p.Name, Snapshot: r.Status.Snapshot, SpecHash: r.Status.SpecHash}
	if r.Spec.Mounts != nil {
		ticket.Mounts = mounts
	} else if len(mounts) > 0 {
		ticket.HomeID = mounts[0].HomeID
	}
	return EphemeralJSON(filepath.Join(b.Base, "tickets", string(p.UID)+".json"), ticket)
}

func homeOwner(r *api.ResumablePod) string {
	if r.Spec.HomeVolumeUID != "" {
		return r.Spec.HomeVolumeUID
	}
	return string(r.UID)
}

func (b *Backend) persistentHome(r *api.ResumablePod, initialize bool) (string, error) {
	if !r.Spec.PersistentHome || r.Spec.Mounts != nil {
		return "", nil
	}
	mounts, err := b.prepareVolumeMounts(r, initialize)
	if err != nil {
		return "", fmt.Errorf("persistent HOME: %w", err)
	}
	if len(mounts) == 0 {
		return "", nil
	}
	return mounts[0].HomeID, nil
}
func (b *Backend) Cleanup(ctx context.Context, p *core.Pod) error {
	if p.UID == "" {
		return nil
	}
	// Look before touching anything. A sandbox that CRI holds under another
	// runtime handler is not ours: retiring its lease or revoking its ticket would
	// damage the owner, and stopping it is not our call.
	if _, err := b.sandboxes(ctx, p); errors.Is(err, ErrForeignRuntime) {
		return err
	}
	// Retire a warm lease before revoking tickets or touching the runtime.
	if err := RetireWarm(b.Base, string(p.UID)); err != nil {
		return err
	}
	// Revoke tickets before terminating runtimes; failed/retried creates cannot restore again.
	if err := os.Remove(filepath.Join(b.Base, "tickets", string(p.UID)+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	ss, err := b.sandboxes(ctx, p)
	if err != nil {
		return err
	}
	ids := map[string]bool{}
	for _, s := range ss {
		ids[s.Id] = true
	}
	// A failed Create may never be registered in CRI. The adapter records the
	// identity before invoking runsc, so these sandboxes can also be reclaimed.
	entries, err := os.ReadDir(filepath.Join(b.Base, "requests"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		var t Ticket
		if err = ReadJSON(filepath.Join(b.Base, "requests", entry.Name()), &t); err != nil {
			return err
		}
		if t.PodUID == string(p.UID) && t.PodName == p.Name && t.Namespace == p.Namespace {
			id := strings.TrimSuffix(entry.Name(), ".json")
			if len(id) != 64 || !ValidID(id) {
				return fmt.Errorf("invalid request sandbox ID")
			}
			ids[id] = true
		}
	}
	for id := range ids {
		children, err := b.cleanupContainerIDs(ctx, p, id)
		if err != nil {
			return err
		}
		// runsc delete bypasses the shim's failed-start wait/IO deadlock. CRI still owns CNI teardown.
		if err = b.command(ctx, "delete", "--force", id); err != nil {
			return err
		}
		// A no-root warm slot also precreates child tasks. Destroying its sandbox
		// does not unblock those shims, and CRI cannot remove a running task.
		if err = b.cleanupContainers(ctx, children); err != nil {
			return err
		}
		stopCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		_, err = b.Runtime.StopPodSandbox(stopCtx, &cri.StopPodSandboxRequest{PodSandboxId: id})
		cancel()
		// Also reap after a successful Stop: CRI may report NOTREADY while a
		// failed-start shim still prevents RunPodSandbox from returning.
		if reapErr := b.reapShim(id); reapErr != nil {
			return reapErr
		}
		if err != nil {
			return err
		}
		rmCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		_, err = b.Runtime.RemovePodSandbox(rmCtx, &cri.RemovePodSandboxRequest{PodSandboxId: id})
		cancel()
		if err != nil {
			return err
		}
		for _, suffix := range []string{".json", ".json.started", ".json.child"} {
			if err = os.Remove(filepath.Join(b.Base, "requests", id+suffix)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	if err = b.removeChildren(string(p.UID)); err != nil {
		return err
	}
	if err = os.Remove(filepath.Join(b.Base, "claims", string(p.UID))); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
func (b *Backend) Forget(ctx context.Context, r *api.ResumablePod) error {
	if !ValidID(string(r.UID)) {
		return fmt.Errorf("invalid owner UID")
	}
	deleting := r.DeletionTimestamp != nil
	if b.Checkpoints != nil {
		if err := b.Checkpoints.DeleteCheckpoint(ctx, string(r.UID), r.Status.Snapshot); err != nil && !errors.Is(err, inventory.ErrNotFound) {
			return err
		}
	}
	if err := os.RemoveAll(filepath.Join(b.Base, "workloads", string(r.UID))); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(b.Base, "garbage", string(r.UID))); err != nil {
		return err
	}
	// Runtime/snapshot cleanup also calls Forget while the Box still exists.
	// Independent volumes are untouched on Box deletion. Legacy HOME is
	// promoted in place after runtime cleanup so its data remains independently managed.
	if deleting && len(configuredVolumeMounts(r)) > 0 {
		if err := b.detachVolumeMounts(r, configuredVolumeMounts(r)); err != nil {
			return err
		}
	}
	return nil
}
