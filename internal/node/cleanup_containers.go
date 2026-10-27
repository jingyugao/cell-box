package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Include adapter records: failed StartContainer calls can leave a task that
// is absent from CRI's container list. Validate every identity before deletion.
func (b *Backend) cleanupContainerIDs(ctx context.Context, pod *core.Pod, sandboxID string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	containers, err := b.Runtime.ListContainers(ctx, &cri.ListContainersRequest{Filter: &cri.ContainerFilter{PodSandboxId: sandboxID}})
	if err != nil {
		return nil, err
	}
	ids := map[string]bool{}
	for _, c := range containers.Containers {
		if c.PodSandboxId != sandboxID || c.Labels["io.kubernetes.pod.uid"] != string(pod.UID) {
			return nil, fmt.Errorf("CRI cleanup container identity mismatch")
		}
		ids[c.Id] = true
	}
	dir := filepath.Join(b.Base, "children")
	entries, err := os.ReadDir(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		var child ChildRecord
		if err := ReadJSON(filepath.Join(dir, entry.Name()), &child); err != nil {
			return nil, err
		}
		if child.SandboxID != sandboxID {
			continue
		}
		t := child.Ticket
		if t.PodUID != string(pod.UID) || t.PodName != pod.Name || t.Namespace != pod.Namespace {
			return nil, fmt.Errorf("adapter cleanup container identity mismatch")
		}
		ids[strings.TrimSuffix(entry.Name(), ".json")] = true
	}
	out := make([]string, 0, len(ids))
	for id := range ids {
		if len(id) != 64 || !ValidID(id) || id == sandboxID {
			return nil, fmt.Errorf("invalid cleanup container ID")
		}
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// The parent sandbox has already been destroyed. Reap only exact owned child
// shims, then let CRI retire its tasks and metadata. Keep records on any failure
// so the next pool pass can finish the same cleanup after a crash or timeout.
func (b *Backend) cleanupContainers(ctx context.Context, ids []string) error {
	for _, id := range ids {
		if err := b.command(ctx, "delete", "--force", id); err != nil {
			return err
		}
		if err := b.reapShim(id); err != nil {
			return err
		}
		stopCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		_, err := b.Runtime.StopContainer(stopCtx, &cri.StopContainerRequest{ContainerId: id})
		cancel()
		if err != nil && status.Code(err) != codes.NotFound {
			return err
		}
		// Shim exit is observed asynchronously by containerd. Retain all local
		// authorization records until CRI confirms that removal has completed.
		err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
			_, removeErr := b.Runtime.RemoveContainer(ctx, &cri.RemoveContainerRequest{ContainerId: id})
			if removeErr == nil || status.Code(removeErr) == codes.NotFound {
				return true, nil
			}
			if removalPending(removeErr) {
				return false, nil
			}
			return false, removeErr
		})
		if err != nil {
			return fmt.Errorf("remove owned container %s: %w", id, err)
		}
	}
	return nil
}

// removalPending reports a RemoveContainer failure that clears on its own. A warm
// slot's StartContainer RPC is still blocked in the wrapper when the slot is
// retired, so containerd reports the container as starting, with code Unknown
// rather than FailedPrecondition, until that RPC unwinds.
func removalPending(err error) bool {
	return status.Code(err) == codes.FailedPrecondition || strings.Contains(err.Error(), "is in starting state")
}
