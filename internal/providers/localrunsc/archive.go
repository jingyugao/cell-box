package localrunsc

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/objectstorage"
	runtimebackend "cellbox.local/cellbox/internal/runtime"
	"cellbox.local/cellbox/internal/workspacearchive"
)

// CaptureWorkspaceArchive exports the configured workspace from a stopped
// local sandbox. The caller receives the same archive key and byte format as
// the Kubernetes provider.
func (p *Provider) CaptureWorkspaceArchive(ctx context.Context, h boxprovider.Handle, id string) (int64, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	r, err := p.owned(h)
	if err != nil {
		return 0, "", err
	}
	if r.Phase != "suspended" && r.Phase != "failed" {
		return 0, "", errors.New("workspace archive source must be suspended or failed")
	}
	if p.objects == nil {
		return 0, "", errors.New("object storage is required for suspended workspace archives")
	}
	state, err := p.state(ctx, h.ID)
	if err == nil {
		return 0, "", fmt.Errorf("workspace archive source still has runsc execution %q in state %q", state.ID, state.Status)
	}
	if !errors.Is(err, boxprovider.ErrNotFound) {
		return 0, "", err
	}
	if r.Phase == "suspended" {
		if !identifier.MatchString(r.Snapshot) {
			return 0, "", errors.New("workspace archive checkpoint is unavailable")
		}
		if err := p.validateSnapshot(r); err != nil {
			return 0, "", fmt.Errorf("workspace archive checkpoint changed: %w", err)
		}
	}

	homeUID, err := workspaceHomeUID(r)
	if err != nil {
		return 0, "", err
	}
	workspaceRel, err := workspacearchive.WorkspaceRelative(r.Spec.Config.Workspace)
	if err != nil {
		return 0, "", err
	}
	if err := checkWorkspaceMountOverlaps(r, homeUID, r.Spec.Config.Workspace); err != nil {
		return 0, "", err
	}
	returnSize, returnDigest := int64(0), ""
	err = p.withStoppedVolume(ctx, homeUID, func(root string) error {
		if _, err := homevolume.Verify(p.base, homeUID, "local", homevolume.VolumeSpecHash, ""); err != nil {
			return fmt.Errorf("%w: %v", boxprovider.ErrStaleVolume, err)
		}
		tempDir := filepath.Join(p.dir(r.Handle.Name), "archives")
		result, err := workspacearchive.Capture(ctx, root, workspaceRel, id, tempDir, time.Time{}, p.objects)
		if err != nil {
			return err
		}
		returnSize, returnDigest = result.Size, result.SHA256
		return nil
	})
	if err != nil {
		if errors.Is(err, objectstorage.ErrUnavailable) {
			err = fmt.Errorf("%w: %w", runtimebackend.ErrRetryableStorage, err)
		}
		return 0, "", err
	}
	return returnSize, returnDigest, nil
}

func workspaceHomeUID(r record) (string, error) {
	if r.Handle.ID == "" {
		return "", errors.New("workspace archive requires persistent HOME")
	}
	uid := r.Spec.HomeVolumeUID
	hasMounts := len(r.Spec.Mounts) > 0
	if hasMounts {
		uid = ""
		readOnly := false
		for _, mount := range r.Spec.Mounts {
			if mount.MountPath == "/home/agent" {
				uid, readOnly = mount.UID, mount.ReadOnly
			}
		}
		if uid == "" || readOnly {
			return "", errors.New("workspace archive requires a writable /home/agent volume mount")
		}
	} else if uid == "" {
		return "", errors.New("workspace archive requires a writable /home/agent volume mount")
	}
	return uid, nil
}

func checkWorkspaceMountOverlaps(r record, homeUID, workspace string) error {
	workspacePath := path.Clean(workspace)
	for _, mount := range r.Spec.Mounts {
		if mount.UID == homeUID && mount.MountPath == "/home/agent" {
			continue
		}
		if mount.MountPath == "/home/agent" || mount.MountPath == workspacePath ||
			strings.HasPrefix(mount.MountPath, workspacePath+"/") ||
			strings.HasPrefix(workspacePath, strings.TrimSuffix(mount.MountPath, "/")+"/") {
			return errors.New("workspace archive is unsupported when another volume overlaps the workspace")
		}
	}
	return nil
}
