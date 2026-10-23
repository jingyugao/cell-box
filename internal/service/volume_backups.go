package service

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/volumebackup"
)

type volumeBackupEngine interface {
	Backup(context.Context, string, string, string) (volumebackup.Result, error)
	List(context.Context, string, string) ([]volumebackup.Snapshot, error)
	Restore(context.Context, string, string, string, string) error
}

type volumeBackupSourceState struct {
	Phase string    `json:"phase"`
	Error *APIError `json:"error,omitempty"`
}

func restoreBackupSourceState(volume *volumeRecord) {
	if volume.BackupSourceState != nil {
		volume.Volume.Phase = volume.BackupSourceState.Phase
		volume.Volume.Error = volume.BackupSourceState.Error
		volume.BackupSourceState = nil
	} else if volume.Volume.Phase == "backing-up" {
		volume.Volume.Phase, volume.Volume.Error = "available", nil
	}
}

// VolumeBackup describes a complete filesystem version, independently of Box
// identities, images, and same-node process checkpoints.
type VolumeBackup struct {
	volumebackup.Snapshot
	VolumeID  string `json:"volumeId"`
	OwnerKey  string `json:"ownerKey"`
	ProfileID string `json:"profileId"`
}

type restoreVolumeRequest struct {
	ID             string `json:"id,omitempty"`
	SourceVolumeID string `json:"sourceVolumeId"`
	BackupID       string `json:"backupId"`
	OwnerKey       string `json:"ownerKey"`
}

var backupIDPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (s *Service) requireVolumeBackups() error {
	if s.backups == nil {
		return apiError("UNSUPPORTED_CAPABILITY", "Volume backups require volumeBackups.enabled and configured object storage")
	}
	return nil
}

// The Volume record serializes its own mutations. Box creation already checks
// it in resolveBoxVolume; resume and activation must use the same admission
// transaction, so a stopped volume cannot acquire a writer during backup.
func requireAvailableBoxVolumes(tx *storeTx, b boxRecord) error {
	for _, mount := range boxVolumeMounts(b) {
		v, err := tx.Volume(b.ClientID, mount.VolumeID)
		if err != nil {
			return err
		}
		if v.Volume.OperationID != "" {
			return apiError("BUSY", "A mounted Volume has an operation in progress")
		}
		if v.Volume.Phase != "available" {
			return apiError("CONFLICT", "A mounted Volume is not available")
		}
	}
	return nil
}

func requireStoppedVolumeBoxes(tx *storeTx, client, volumeID string) error {
	boxes, err := listRecords[boxRecord](tx.ctx, tx.tx, "boxes", "AND client_id=?", client)
	if err != nil {
		return err
	}
	for _, box := range boxes {
		attached := false
		for _, mount := range boxVolumeMounts(box) {
			attached = attached || mount.VolumeID == volumeID
		}
		if !attached || box.Box.Phase == "deleted" {
			continue
		}
		if err := busy(tx, box); err != nil {
			return err
		}
		if box.Box.Phase != "suspended" && box.Box.Phase != "failed" {
			return apiError("BUSY", "All Boxes referencing the Volume must be stopped before backup")
		}
		active, err := tx.HasActiveExecution(box.Box.ID)
		if err != nil {
			return err
		}
		if active {
			return apiError("BUSY", "A Box referencing the Volume has an active execution")
		}
	}
	return nil
}

func (s *Service) backupVolume(client, id string) (Operation, error) {
	if err := s.requireVolumeBackups(); err != nil {
		return Operation{}, err
	}
	var volume volumeRecord
	var access boxprovider.StoppedVolumeProvider
	op, err := s.prepareOperation(client, "volume-backup", "volume:"+id, func(tx *storeTx, op *Operation) error {
		op.TargetID = id
		var err error
		volume, err = tx.Volume(client, id)
		if err != nil {
			return err
		}
		var supported bool
		access, supported = s.providers[volume.Provider].(boxprovider.StoppedVolumeProvider)
		if !supported {
			return boxprovider.ErrUnsupported
		}
		if volume.Volume.OperationID != "" {
			return apiError("BUSY", "Volume has an operation in progress")
		}
		if volume.Volume.Phase != "available" && volume.Volume.Phase != "failed" {
			return apiError("CONFLICT", "Volume must be available or failed before backup")
		}
		if err = requireStoppedVolumeBoxes(tx, client, id); err != nil {
			return err
		}
		volume.BackupSourceState = &volumeBackupSourceState{Phase: volume.Volume.Phase, Error: volume.Volume.Error}
		volume.Volume.OperationID, volume.Volume.Phase = op.ID, "backing-up"
		volume.Volume.Error = nil
		volume.Volume.Version++
		return tx.PutVolume(volume)
	})
	if err != nil {
		return op, err
	}
	s.launch(op, func(ctx context.Context) (map[string]string, error) {
		result := map[string]string{"volumeId": id}
		err := access.WithStoppedVolume(ctx, volume.spec(), func(root string) error {
			backup, err := s.backups.Backup(ctx, client, id, root)
			if backup.Snapshot.ID != "" {
				result["backupId"] = backup.Snapshot.ID
			}
			if backup.RetentionError != "" {
				result["retentionError"] = backup.RetentionError
			}
			return err
		})
		return result, volumeBackupError(err)
	})
	return op, nil
}

func finishVolumeBackupOperation(tx *storeTx, op Operation, failure error) error {
	volumes, err := listRecords[volumeRecord](tx.ctx, tx.tx, "volumes", "AND json_extract(record,'$.volume.operationId')=?", op.ID)
	if err != nil {
		return err
	}
	if len(volumes) == 0 {
		return boxprovider.ErrStaleTransition
	}
	for _, volume := range volumes {
		volume.Volume.OperationID = ""
		volume.Volume.Version++
		// Restoring reads the source repository; its source Volume may already
		// be deleted. Only the new target's filesystem changes.
		if op.Kind == "volume-backup" {
			restoreBackupSourceState(&volume)
		} else if volume.Volume.ID == op.TargetID {
			volume.Volume.Phase, volume.Volume.Error = "available", nil
			if failure != nil {
				volume.Volume.Phase, volume.Volume.Error = "failed", cloneError(failure)
			}
		}
		if err := tx.PutVolume(volume); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) listVolumeBackups(ctx context.Context, client, id string) ([]VolumeBackup, error) {
	volume, err := s.store.VolumeRecord(client, id)
	if err != nil {
		return nil, err
	}
	if err := s.requireVolumeBackups(); err != nil {
		return nil, err
	}
	snapshots, err := s.backups.List(ctx, client, id)
	if err != nil {
		return nil, volumeBackupError(err)
	}
	result := make([]VolumeBackup, 0, len(snapshots))
	for _, snapshot := range snapshots {
		result = append(result, VolumeBackup{Snapshot: snapshot, VolumeID: id,
			OwnerKey: volume.Volume.OwnerKey, ProfileID: volume.Volume.ProfileID})
	}
	return result, nil
}

func (s *Service) restoreVolume(client string, input restoreVolumeRequest) (Operation, error) {
	if !backupIDPattern.MatchString(input.BackupID) || input.SourceVolumeID == "" || input.OwnerKey == "" || len(input.OwnerKey) > 256 {
		return Operation{}, apiError("INVALID_REQUEST", "sourceVolumeId, a full backupId, and ownerKey (1..256 bytes) are required")
	}
	if input.ID == "" {
		input.ID = randomID("vol-")
	}
	if err := validateResourceID(input.ID); err != nil {
		return Operation{}, err
	}
	// This read validates ownership and snapshot existence without requiring
	// the original disk or Box to exist, and without creating a target on 404.
	backups, err := s.listVolumeBackups(s.ctx, client, input.SourceVolumeID)
	if err != nil {
		return Operation{}, err
	}
	found := false
	for _, backup := range backups {
		found = found || backup.ID == input.BackupID
	}
	if !found {
		return Operation{}, apiError("NOT_FOUND", "Volume backup not found")
	}
	var source, candidate volumeRecord
	var provider boxprovider.VolumeProvider
	var access boxprovider.StoppedVolumeProvider
	op, err := s.prepareOperation(client, "volume-restore", "volume:"+input.SourceVolumeID, func(tx *storeTx, op *Operation) error {
		op.TargetID = input.ID
		var err error
		source, err = tx.Volume(client, input.SourceVolumeID)
		if err != nil {
			return err
		}
		if source.Volume.OperationID != "" {
			return apiError("BUSY", "Source Volume has an operation in progress")
		}
		var supported bool
		provider, supported = s.providers[source.Provider].(boxprovider.VolumeProvider)
		if !supported {
			return boxprovider.ErrUnsupported
		}
		access, supported = s.providers[source.Provider].(boxprovider.StoppedVolumeProvider)
		if !supported {
			return boxprovider.ErrUnsupported
		}
		if err = tx.reserveID("volumes", input.ID, client); err != nil {
			return err
		}
		candidate = source
		candidate.UID = randomID("vol-")
		candidate.BackupSourceState = nil
		candidate.Volume = Volume{ID: input.ID, OwnerKey: input.OwnerKey, ProfileID: source.Volume.ProfileID,
			NodeName: source.Volume.NodeName, Phase: "creating", Version: 1, CreatedAt: time.Now().UTC(), OperationID: op.ID}
		if err = tx.PutVolume(candidate); err != nil {
			return err
		}
		// Reserve the source repository against backup retention while reading
		// its snapshot; its original disk state is otherwise unchanged.
		source.Volume.OperationID = op.ID
		source.Volume.Version++
		return tx.PutVolume(source)
	})
	if err != nil {
		return op, err
	}
	s.launch(op, func(ctx context.Context) (map[string]string, error) {
		result := map[string]string{"volumeId": input.ID, "sourceVolumeId": input.SourceVolumeID, "backupId": input.BackupID}
		if err := provider.CreateVolume(ctx, candidate.spec()); err != nil {
			return result, err
		}
		err := access.WithStoppedVolume(ctx, candidate.spec(), func(root string) error {
			return s.backups.Restore(ctx, client, input.SourceVolumeID, input.BackupID, root)
		})
		return result, volumeBackupError(err)
	})
	return op, nil
}

func volumeBackupError(err error) error {
	if errors.Is(err, volumebackup.ErrUnknownOutcome) {
		return apiError("UNKNOWN_OUTCOME", "Volume backup result is unknown; inspect its backups before submitting another operation")
	}
	if errors.Is(err, volumebackup.ErrBusy) {
		return apiError("BUSY", "Volume backup repository is in use")
	}
	if errors.Is(err, volumebackup.ErrDisabled) {
		return apiError("UNSUPPORTED_CAPABILITY", "Volume backups are not configured")
	}
	if errors.Is(err, volumebackup.ErrNotFound) {
		return apiError("NOT_FOUND", "Volume backup not found")
	}
	if errors.Is(err, volumebackup.ErrInvalid) {
		return apiError("INVALID_REQUEST", "Invalid volume backup request")
	}
	if errors.Is(err, volumebackup.ErrOutputTooBig) {
		return apiError("INVALID_REQUEST", "Backup file exceeds the 16 MiB REST transfer limit")
	}
	if errors.Is(err, boxprovider.ErrNotReady) {
		return apiError("BUSY", "Volume still has a runtime or storage operation in progress")
	}
	if errors.Is(err, boxprovider.ErrStaleVolume) {
		return apiError("CONFLICT", "Volume identity differs from its stored filesystem")
	}
	return err
}

func (s *Service) registerVolumeBackups(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/backup-policy", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"engine": "restic", "scope": "volume",
			"keepLast": s.config.VolumeBackups.KeepLast, "enabled": s.backups != nil})
	})
	mux.HandleFunc("POST /v1/volumes/{id}/backups", func(w http.ResponseWriter, r *http.Request) {
		if err := decodeOptionalLimit(w, r, &struct{}{}, 1024); err != nil {
			fail(w, err)
			return
		}
		op, err := s.backupVolume(clientID(r), r.PathValue("id"))
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, op)
	})
	mux.HandleFunc("GET /v1/volumes/{id}/backups", func(w http.ResponseWriter, r *http.Request) {
		backups, err := s.listVolumeBackups(r.Context(), clientID(r), r.PathValue("id"))
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, backups)
	})
	mux.HandleFunc("GET /v1/volumes/{id}/backups/{backupId}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("backupId")
		if !backupIDPattern.MatchString(id) {
			fail(w, apiError("INVALID_REQUEST", "A full backup ID is required"))
			return
		}
		backups, err := s.listVolumeBackups(r.Context(), clientID(r), r.PathValue("id"))
		if err != nil {
			fail(w, err)
			return
		}
		for _, backup := range backups {
			if backup.ID == id {
				writeJSON(w, http.StatusOK, backup)
				return
			}
		}
		fail(w, apiError("NOT_FOUND", "Volume backup not found"))
	})
	mux.HandleFunc("GET /v1/volumes/{id}/backups/{backupId}/files", s.backupFile)
	mux.HandleFunc("POST /v1/volumes:restore", func(w http.ResponseWriter, r *http.Request) {
		var input restoreVolumeRequest
		if err := decode(w, r, &input); err != nil {
			fail(w, err)
			return
		}
		op, err := s.restoreVolume(clientID(r), input)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, op)
	})
}
