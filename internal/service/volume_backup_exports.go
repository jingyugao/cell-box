package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/jingyugao/cell-box/internal/directoryarchive"
	"github.com/jingyugao/cell-box/internal/volumebackup"
)

type volumeBackupDirectoryExporter interface {
	ExportDirectory(context.Context, string, string, string, string, io.Writer) (directoryarchive.Result, error)
}

func (s *Service) exportVolumeBackup(w http.ResponseWriter, r *http.Request) {
	client, volumeID, backupID := clientID(r), r.PathValue("id"), r.PathValue("backupId")
	if !backupIDPattern.MatchString(backupID) {
		fail(w, apiError("INVALID_REQUEST", "A full 64-character backup ID is required"))
		return
	}
	relative, err := volumeExportPath(r.URL.Query().Get("path"))
	if err != nil {
		fail(w, err)
		return
	}
	if err := s.requireVolumeBackups(); err != nil {
		fail(w, err)
		return
	}
	exporter, ok := s.backups.(volumeBackupDirectoryExporter)
	if !ok {
		fail(w, apiError("UNSUPPORTED_CAPABILITY", "Volume backup directory exports are unavailable"))
		return
	}
	if s.exportDir == "" {
		fail(w, apiError("UNSUPPORTED_CAPABILITY", "Volume backup export storage is unavailable"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Minute)
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	defer cancel()
	file, err := os.CreateTemp(s.exportDir, ".backup-export-*")
	if err != nil {
		fail(w, err)
		return
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	result, err := exporter.ExportDirectory(ctx, client, volumeID, backupID, relative, file)
	if err != nil {
		_ = file.Close()
		if ctx.Err() != nil {
			fail(w, apiError("TIMEOUT", "Volume backup export was canceled or timed out"))
			return
		}
		if errors.Is(err, volumebackup.ErrArchiveTooBig) {
			fail(w, apiError("INVALID_REQUEST", "Volume backup export exceeds archive limits"))
			return
		}
		if errors.Is(err, volumebackup.ErrOutputTooBig) {
			fail(w, apiError("INVALID_REQUEST", "Snapshot metadata exceeds the export limit"))
			return
		}
		fail(w, volumeBackupError(err))
		return
	}
	if err := ctx.Err(); err != nil {
		_ = file.Close()
		fail(w, apiError("TIMEOUT", "Volume backup export was canceled or timed out"))
		return
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		fail(w, err)
		return
	}
	if err := file.Close(); err != nil {
		fail(w, err)
		return
	}
	file, err = os.Open(temporary)
	if err != nil {
		fail(w, err)
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != result.SizeBytes || info.Size() < 1 {
		fail(w, apiError("ARCHIVE_CORRUPT", "Volume backup export is incomplete"))
		return
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", `attachment; filename="volume-backup-export.tar.gz"`)
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, result.SHA256))
	w.Header().Set("X-Cellbox-Export-SHA256", result.SHA256)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, "volume-backup-export.tar.gz", time.Now().UTC(), file)
}
