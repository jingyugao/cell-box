package node

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/guestapi"
	"github.com/jingyugao/cell-box/internal/homevolume"
	"github.com/jingyugao/cell-box/internal/workspacearchive"
)

const (
	workspaceArchiveEntries = workspacearchive.MaxEntries
	workspaceArchiveDepth   = workspacearchive.MaxDepth
	workspaceMount          = "/home/agent"
)

var workspaceArchiveID = regexp.MustCompile(`^arc-[0-9a-f]{32}$`)

func configuredWorkspace(r *api.ResumablePod) (string, error) {
	args := r.Spec.Container.Args
	for i := range args {
		if args[i] != "--config-base64" || i+1 >= len(args) {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(args[i+1])
		if err != nil {
			return "", errors.New("invalid embedded guest configuration")
		}
		var config guestapi.Config
		if err = json.Unmarshal(data, &config); err != nil {
			return "", errors.New("invalid embedded guest configuration")
		}
		return workspacearchive.WorkspaceRelative(config.Workspace)
	}
	return "", errors.New("embedded guest configuration is missing")
}

func writableWorkspaceHomeMount(r *api.ResumablePod) (VolumeMountAuthorization, error) {
	if r == nil {
		return VolumeMountAuthorization{}, errors.New("workspace archive requires a writable /home/agent volume mount")
	}
	var home *VolumeMountAuthorization
	homeIndex := -1
	mounts := configuredVolumeMounts(r)
	for i := range mounts {
		mount := mounts[i]
		if mount.MountPath == homevolume.MountPath {
			copy := mount
			home = &copy
			homeIndex = i
		}
	}
	if home == nil || home.ReadOnly {
		return VolumeMountAuthorization{}, errors.New("workspace archive requires a writable /home/agent volume mount")
	}
	workspaceRel, err := configuredWorkspace(r)
	if err != nil {
		return VolumeMountAuthorization{}, err
	}
	workspacePath := filepath.ToSlash(filepath.Join(homevolume.MountPath, workspaceRel))
	for i, mount := range mounts {
		if i == homeIndex {
			continue
		}
		if mount.MountPath == homevolume.MountPath || mount.MountPath == workspacePath ||
			strings.HasPrefix(mount.MountPath, workspacePath+"/") ||
			strings.HasPrefix(workspacePath, strings.TrimSuffix(mount.MountPath, "/")+"/") {
			return VolumeMountAuthorization{}, errors.New("workspace archive is unsupported when another volume overlaps the workspace")
		}
	}
	return *home, nil
}

func (b *Backend) CaptureWorkspaceArchive(ctx context.Context, r *api.ResumablePod, request api.ArchiveCaptureRequest) (api.ArchiveCaptureResult, error) {
	result := api.ArchiveCaptureResult{ID: request.ID, Snapshot: request.Snapshot}
	if r != nil {
		result.SourceUID, result.SourceGeneration = string(r.UID), r.Generation
	}
	if b.Objects == nil {
		return result, errors.New("object storage is required for suspended workspace archives")
	}
	if !workspaceArchiveID.MatchString(request.ID) {
		return result, errors.New("invalid workspace archive identifier")
	}
	if r == nil || r.Status.PodUID != "" || r.Status.PodName != "" || r.DeletionTimestamp != nil {
		return result, errors.New("workspace archive source must be stopped")
	}
	homeMount, err := writableWorkspaceHomeMount(r)
	if err != nil {
		return result, err
	}
	if r.Status.Phase == "Suspended" {
		if r.Spec.DesiredState != "Suspended" || r.Status.Snapshot == "" || r.Status.Snapshot != request.Snapshot {
			return result, errors.New("workspace archive checkpoint changed")
		}
	} else if r.Status.Phase != "Failed" || request.Snapshot != r.Status.Snapshot {
		return result, errors.New("workspace archive source must be suspended or failed")
	}
	workspaceRel, err := configuredWorkspace(r)
	if err != nil {
		return result, err
	}
	if homeMount.Legacy {
		_, err = homevolume.Verify(b.Base, homeMount.UID, r.Spec.NodeName, r.Status.SpecHash, "")
	} else {
		_, err = homevolume.Verify(b.Base, homeMount.UID, r.Spec.NodeName, homevolume.VolumeSpecHash, "")
	}
	if err != nil {
		return result, err
	}
	if r.Status.Phase == "Suspended" {
		snapshotPath, pathErr := SnapshotPath(b.Base, string(r.UID), r.Status.Snapshot)
		if pathErr != nil {
			return result, pathErr
		}
		manifest, verifyErr := VerifyCached(snapshotPath, string(r.UID), r.Status.SpecHash, b.Runsc)
		if verifyErr == nil {
			verifyErr = b.verifySnapshotHome(r, manifest)
		}
		if verifyErr != nil {
			return result, verifyErr
		}
	}
	root, err := homevolume.Path(b.Base, homeMount.UID)
	if err != nil {
		return result, err
	}
	archiveDir := filepath.Join(b.Base, "workloads", string(r.UID))
	captured, err := workspacearchive.Capture(ctx, root, workspaceRel, request.ID, archiveDir, request.ExpiresAt, b.Objects)
	if err != nil {
		return result, retryableStorage(err)
	}
	result.Size, result.SHA256 = captured.Size, captured.SHA256
	return result, nil
}

func openWorkspaceRoot(homePath, rel string) (*os.File, error) {
	return workspacearchive.OpenWorkspace(homePath, rel)
}
