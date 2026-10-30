package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/directoryarchive"
	"github.com/jingyugao/cell-box/internal/homevolume"
	"golang.org/x/sys/unix"
)

const volumeExportLifetime = time.Hour

var volumeExportIDPattern = regexp.MustCompile(`^exp-[a-f0-9]{32}$`)

// VolumeExport is an expiring, node-local copy. It is not a retained backup.
type VolumeExport struct {
	ID        string    `json:"id"`
	VolumeID  string    `json:"volumeId"`
	Path      string    `json:"path"`
	Format    string    `json:"format"`
	SizeBytes int64     `json:"sizeBytes"`
	SHA256    string    `json:"sha256"`
	CreatedAt time.Time `json:"createdAt"`
	ExpiresAt time.Time `json:"expiresAt"`
}

type volumeExportRecord struct {
	ClientID string       `json:"clientId"`
	Export   VolumeExport `json:"export"`
}

type createVolumeExportRequest struct {
	Path string `json:"path"`
}

func volumeExportPath(value string) (string, error) {
	if value == "" {
		return ".", nil
	}
	if len(value) > 4096 || path.IsAbs(value) || path.Clean(value) != value ||
		value == ".." || strings.HasPrefix(value, "../") ||
		strings.ContainsRune(value, '\\') ||
		strings.IndexFunc(value, unicode.IsControl) >= 0 || len(strings.Split(value, "/")) > 64 {
		return "", apiError("INVALID_REQUEST", "Export path must be a clean volume-relative directory path")
	}
	return value, nil
}

func (s *Service) createVolumeExport(client, id string, input createVolumeExportRequest) (Operation, error) {
	relative, err := volumeExportPath(input.Path)
	if err != nil {
		return Operation{}, err
	}
	var volume volumeRecord
	var access boxprovider.StoppedVolumeProvider
	op, err := s.prepareOperation(client, "volume-export", "volume:"+id, func(tx *storeTx, op *Operation) error {
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
		if volume.Volume.Phase != "available" {
			return apiError("CONFLICT", "Export requires an available Volume")
		}
		if err := requireStoppedVolumeBoxes(tx, client, id); err != nil {
			return err
		}
		// The same durable reservation is checked by Box create/resume/activate,
		// Volume delete/backup/restore, and cleared by startup recovery.
		volume.Volume.OperationID = op.ID
		volume.Volume.Version++
		return tx.PutVolume(volume)
	})
	if err != nil {
		return op, err
	}
	export := VolumeExport{ID: randomID("exp-"), VolumeID: id, Path: relative, Format: "tar.gz"}
	s.launchWithCommitContext(s.ctx, op, func(ctx context.Context) (map[string]string, error) {
		err := access.WithStoppedVolume(ctx, volume.spec(), func(root string) error {
			return s.captureVolumeExport(ctx, root, &export)
		})
		if err != nil {
			_ = os.Remove(s.volumeExportFilename(export.ID))
			if errors.Is(err, homevolume.ErrBusy) {
				err = apiError("BUSY", "Volume has a runtime or storage operation in progress")
			}
			return nil, volumeBackupError(err)
		}
		return map[string]string{"volumeId": id, "exportId": export.ID}, nil
	}, func(tx *storeTx) error {
		return tx.put("volume_exports", export.ID, client, "", volumeExportRecord{ClientID: client, Export: export})
	})
	return op, nil
}

func (s *Service) discardVolumeExport(result map[string]string) {
	if id := result["exportId"]; volumeExportIDPattern.MatchString(id) {
		_ = os.Remove(s.volumeExportFilename(id))
	}
}

func finishVolumeExportOperation(tx *storeTx, op Operation) error {
	volume, err := tx.Volume("", op.TargetID)
	if err != nil {
		return err
	}
	if volume.Volume.OperationID != op.ID {
		return boxprovider.ErrStaleTransition
	}
	volume.Volume.OperationID = ""
	volume.Volume.Version++
	return tx.PutVolume(volume)
}

func (s *Service) volumeExportFilename(id string) string {
	return filepath.Join(s.exportDir, id+".tar.gz")
}

func (s *Service) captureVolumeExport(ctx context.Context, root string, export *VolumeExport) error {
	file, err := os.CreateTemp(s.exportDir, ".export-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	result, err := directoryarchive.Write(ctx, root, export.Path, file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return apiError("NOT_FOUND", "Export directory does not exist")
		}
		if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) {
			return apiError("INVALID_REQUEST", "Export path must be a directory without symlink traversal")
		}
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), s.volumeExportFilename(export.ID)); err != nil {
		return err
	}
	export.SizeBytes, export.SHA256 = result.SizeBytes, result.SHA256
	export.CreatedAt = time.Now().UTC()
	export.ExpiresAt = export.CreatedAt.Add(volumeExportLifetime)
	return nil
}

func (s *Service) getVolumeExport(ctx context.Context, client, volumeID, id string) (volumeExportRecord, error) {
	if !volumeExportIDPattern.MatchString(id) {
		return volumeExportRecord{}, apiError("INVALID_REQUEST", "Invalid export ID")
	}
	if _, err := s.store.VolumeRecord(client, volumeID); err != nil {
		return volumeExportRecord{}, err
	}
	record, err := readRecord[volumeExportRecord](ctx, s.store.db, "volume_exports", id)
	if errors.Is(err, sql.ErrNoRows) || err == nil && (record.ClientID != client || record.Export.VolumeID != volumeID || !time.Now().Before(record.Export.ExpiresAt)) {
		return volumeExportRecord{}, apiError("NOT_FOUND", "Volume export is unavailable or expired")
	}
	if err != nil {
		return volumeExportRecord{}, err
	}
	// A filesystem unlink cannot participate in the metadata transaction.
	// If a DELETE loses its commit, its remaining record must not advertise
	// the already-removed artifact as available; a retry/retention clears it.
	info, err := os.Lstat(s.volumeExportFilename(id))
	if errors.Is(err, os.ErrNotExist) {
		return volumeExportRecord{}, apiError("NOT_FOUND", "Volume export is unavailable")
	}
	if err != nil {
		return volumeExportRecord{}, err
	}
	if !info.Mode().IsRegular() || info.Size() != record.Export.SizeBytes {
		return volumeExportRecord{}, apiError("ARCHIVE_CORRUPT", "Volume export size does not match its metadata")
	}
	return record, nil
}

func (s *Service) deleteVolumeExport(ctx context.Context, client, volumeID, id string, expiredBefore *time.Time) error {
	if !volumeExportIDPattern.MatchString(id) {
		return apiError("INVALID_REQUEST", "Invalid export ID")
	}
	unlock := s.admissions.lock("export:" + id)
	defer unlock()
	return s.store.Write(ctx, func(tx *storeTx) error {
		if _, err := tx.Volume(client, volumeID); err != nil {
			return err
		}
		record, err := readRecord[volumeExportRecord](ctx, tx.tx, "volume_exports", id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if record.ClientID != client || record.Export.VolumeID != volumeID {
			return apiError("NOT_FOUND", "Volume export not found")
		}
		if expiredBefore != nil && record.Export.ExpiresAt.After(*expiredBefore) {
			return nil
		}
		if err := os.Remove(s.volumeExportFilename(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return tx.exec("DELETE FROM runtime_records WHERE kind='volume_exports' AND id=?", id)
	})
}

func (s *Service) downloadVolumeExport(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("exportId")
	unlock := s.admissions.lock("export:" + id)
	record, err := s.getVolumeExport(r.Context(), clientID(r), r.PathValue("id"), id)
	var file *os.File
	if err == nil {
		file, err = openArchive(s.volumeExportFilename(id))
		if errors.Is(err, os.ErrNotExist) {
			err = apiError("NOT_FOUND", "Volume export is unavailable")
		}
	}
	// On Linux, deletion unlinks the name while this open descriptor retains
	// the bytes until the in-flight download closes it.
	unlock()
	if err != nil {
		fail(w, err)
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != record.Export.SizeBytes {
		fail(w, apiError("ARCHIVE_CORRUPT", "Volume export size does not match its metadata"))
		return
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.tar.gz"`, id))
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("ETag", `"`+record.Export.SHA256+`"`)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, id+".tar.gz", record.Export.CreatedAt, file)
}

// Use the existing data disk: the server's system /tmp may be a small
// emptyDir. The data-directory lock makes startup removal of orphans safe.
func (s *Service) initVolumeExports() error {
	base, err := filepath.EvalSymlinks(s.config.DataDir)
	if err != nil {
		return err
	}
	base, err = filepath.Abs(base)
	if err != nil {
		return err
	}
	for _, component := range []string{"tmp", "volume-exports"} {
		base = filepath.Join(base, component)
		if err := os.Mkdir(base, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		var stat unix.Stat_t
		if err := unix.Lstat(base, &stat); err != nil {
			return err
		}
		if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Mode&0777 != 0700 || stat.Uid != uint32(os.Geteuid()) {
			return errors.New("volume export temporary directory must be private and owned by Cellbox")
		}
	}
	s.exportDir = base
	return s.cleanupVolumeExports()
}

func (s *Service) cleanupVolumeExports() error {
	if s.exportDir == "" {
		return nil
	}
	if err := removeRegularTemps(s.exportDir, func(name string) bool {
		return strings.HasPrefix(name, ".export-") || strings.HasPrefix(name, ".backup-export-") ||
			strings.HasSuffix(name, ".tar.gz") && volumeExportIDPattern.MatchString(strings.TrimSuffix(name, ".tar.gz"))
	}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.store.Write(ctx, func(tx *storeTx) error {
		return tx.exec("DELETE FROM runtime_records WHERE kind='volume_exports'")
	})
}

func (s *Service) pruneVolumeExports(ctx context.Context, now time.Time) error {
	records, err := listRecords[volumeExportRecord](ctx, s.store.db, "volume_exports", "AND julianday(json_extract(record,'$.export.expiresAt'))<=julianday(?)", now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := s.deleteVolumeExport(ctx, record.ClientID, record.Export.VolumeID, record.Export.ID, &now); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) registerVolumeExports(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/volumes/{id}/exports", func(w http.ResponseWriter, r *http.Request) {
		var input createVolumeExportRequest
		if err := decodeOptionalLimit(w, r, &input, 16<<10); err != nil {
			fail(w, err)
			return
		}
		op, err := s.createVolumeExport(clientID(r), r.PathValue("id"), input)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, op)
	})
	mux.HandleFunc("GET /v1/volumes/{id}/exports/{exportId}", func(w http.ResponseWriter, r *http.Request) {
		record, err := s.getVolumeExport(r.Context(), clientID(r), r.PathValue("id"), r.PathValue("exportId"))
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, record.Export)
	})
	mux.HandleFunc("GET /v1/volumes/{id}/exports/{exportId}/content", s.downloadVolumeExport)
	mux.HandleFunc("DELETE /v1/volumes/{id}/exports/{exportId}", func(w http.ResponseWriter, r *http.Request) {
		if err := s.deleteVolumeExport(r.Context(), clientID(r), r.PathValue("id"), r.PathValue("exportId"), nil); err != nil {
			fail(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}
