// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/objectstorage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	core "k8s.io/api/core/v1"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type Backend struct {
	Base, Runsc, Root string
	Socket            string
	// HostMountNamespace runs runsc in the node's mount/root namespace when the
	// controller itself is running in a hostPID privileged DaemonSet Pod.
	HostMountNamespace bool
	Runtime            cri.RuntimeServiceClient
	Images             cri.ImageServiceClient
	// Objects is optional for local-only installations. When configured, sealed
	// snapshots are durable in object storage and Base is only a node cache.
	Objects           objectstorage.Objects
	forgetMu          sync.Mutex
	forgetSuccess     map[forgetKey]struct{}
	inventoryMu       sync.Mutex
	inventoryLocks    sync.Map
	inventoryState    map[string]string
	inventoryVersions map[string]string
	durableMu         sync.Mutex
	durableSnapshots  map[string]bool
}

type forgetKey struct {
	owner string
	cycle int64
}

func New(socket string) (*Backend, error) {
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Backend{Base: DefaultBase, Runsc: DefaultRunsc, Root: DefaultRoot, Socket: socket, Runtime: cri.NewRuntimeServiceClient(conn), Images: cri.NewImageServiceClient(conn)}, nil
}
func (b *Backend) command(ctx context.Context, args ...string) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	command := b.Runsc
	commandArgs := append([]string{"--root=" + b.Root}, args...)
	if b.HostMountNamespace {
		command = "nsenter"
		commandArgs = append([]string{"--target=1", "--mount", "--root", "--", b.Runsc}, commandArgs...)
	}
	c := exec.CommandContext(ctx, command, commandArgs...)
	c.WaitDelay = 2 * time.Second
	output, err := c.CombinedOutput()
	if err != nil {
		return fmt.Errorf("runsc %s: %w: %.2000s", args[0], err, output)
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
		if m == nil || m.Uid != string(p.UID) || m.Namespace != p.Namespace || m.Name != p.Name || s.RuntimeHandler != api.RuntimeClass {
			return nil, fmt.Errorf("CRI sandbox identity/runtime mismatch")
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
func (b *Backend) Checkpoint(ctx context.Context, r *api.ResumablePod, p *core.Pod) error {
	path, err := SnapshotPath(b.Base, string(r.UID), r.Status.Snapshot)
	if err != nil {
		return err
	}
	if _, err = os.Stat(path); err == nil {
		if _, err = VerifyCached(path, string(r.UID), r.Status.SpecHash, b.Runsc); err != nil {
			return err
		}
		return b.uploadSnapshot(ctx, r, path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// The controller may have restarted after the object upload committed but
	// before its status transition. Adopt the durable snapshot before considering
	// another destructive runsc checkpoint.
	if b.Objects != nil {
		if err = b.downloadSnapshot(ctx, r, path); err == nil {
			if _, err = b.verifySnapshotImage(ctx, r, path); err != nil {
				return err
			}
			b.clearForgetSuccess(r)
			return nil
		} else if !errors.Is(err, objectstorage.ErrNotFound) {
			return err
		}
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
	if err = Seal(pending, Manifest{OwnerUID: string(r.UID), SpecHash: r.Status.SpecHash, ImageID: image.Id, RunscHash: binaryHash}); err != nil {
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
	return b.uploadSnapshot(ctx, r, path)
}

func (b *Backend) verifySnapshotImage(ctx context.Context, r *api.ResumablePod, path string) (*Manifest, error) {
	m, err := VerifyCached(path, string(r.UID), r.Status.SpecHash, b.Runsc)
	if err != nil {
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

func (b *Backend) prepareSnapshot(ctx context.Context, r *api.ResumablePod) error {
	if r.Status.Snapshot != "" {
		if b.snapshotConsumed(string(r.UID), r.Status.Snapshot) {
			return fmt.Errorf("checkpoint was consumed by a previous execution")
		}
		path, err := SnapshotPath(b.Base, string(r.UID), r.Status.Snapshot)
		if err != nil {
			return err
		}
		if _, err = os.Stat(path); errors.Is(err, os.ErrNotExist) && b.Objects != nil {
			if err = b.downloadSnapshot(ctx, r, path); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if _, err = b.verifySnapshotImage(ctx, r, path); err != nil {
			return err
		}
		// The cache may be the only surviving copy after an interrupted upload.
		// Do not authorize restore until the remote durable copy is committed.
		if err = b.uploadSnapshot(ctx, r, path); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backend) Prepare(ctx context.Context, r *api.ResumablePod, p *core.Pod) error {
	if err := b.prepareSnapshot(ctx, r); err != nil {
		return err
	}
	return AtomicJSON(filepath.Join(b.Base, "tickets", string(p.UID)+".json"), Ticket{OwnerUID: string(r.UID), PodUID: string(p.UID), Namespace: p.Namespace, PodName: p.Name, Snapshot: r.Status.Snapshot, SpecHash: r.Status.SpecHash})
}
func (b *Backend) Cleanup(ctx context.Context, p *core.Pod) error {
	if p.UID == "" {
		return nil
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
		// runsc delete bypasses the shim's failed-start wait/IO deadlock. CRI still owns CNI teardown.
		if err = b.command(ctx, "delete", "--force", id); err != nil {
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
		for _, suffix := range []string{".json", ".json.started"} {
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
	key := forgetKey{owner: string(r.UID), cycle: r.Status.Cycle}
	deleting := r.DeletionTimestamp != nil
	b.forgetMu.Lock()
	_, alreadyCleaned := b.forgetSuccess[key]
	if deleting {
		b.clearForgetOwnerLocked(key.owner)
		alreadyCleaned = false
	}
	b.forgetMu.Unlock()
	if b.Objects != nil && !alreadyCleaned {
		if err := b.Objects.DeletePrefix(ctx, "checkpoints/"+string(r.UID)); err != nil {
			return err
		}
		b.clearInventoryState(key.owner)
	}
	b.durableMu.Lock()
	for k := range b.durableSnapshots {
		if strings.HasPrefix(k, string(r.UID)+"/") {
			delete(b.durableSnapshots, k)
		}
	}
	b.durableMu.Unlock()
	if err := os.RemoveAll(filepath.Join(b.Base, "workloads", string(r.UID))); err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(b.Base, "garbage", string(r.UID))); err != nil {
		return err
	}
	if b.Objects != nil && !deleting {
		b.forgetMu.Lock()
		if b.forgetSuccess == nil {
			b.forgetSuccess = map[forgetKey]struct{}{}
		}
		b.clearForgetOwnerLocked(key.owner)
		b.forgetSuccess[key] = struct{}{}
		b.forgetMu.Unlock()
	}
	return nil
}

func (b *Backend) clearForgetSuccess(r *api.ResumablePod) {
	key := forgetKey{owner: string(r.UID), cycle: r.Status.Cycle}
	b.forgetMu.Lock()
	delete(b.forgetSuccess, key)
	b.forgetMu.Unlock()
	b.clearInventoryState(key.owner)
}

func (b *Backend) clearForgetOwnerLocked(owner string) {
	for key := range b.forgetSuccess {
		if key.owner == owner {
			delete(b.forgetSuccess, key)
		}
	}
}
