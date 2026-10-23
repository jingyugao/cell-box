package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"golang.org/x/sys/unix"
)

const volumeFileLimit = 16 << 20

type volumeFileAccess interface {
	WithStoppedVolume(context.Context, boxprovider.VolumeSpec, func(string) error) error
}

type volumeFileEntry struct {
	Name      string `json:"name"`
	Directory bool   `json:"directory"`
	Size      int64  `json:"size"`
}

func cleanVolumePath(rel string, allowRoot bool) (string, error) {
	if rel == "." && allowRoot {
		return rel, nil
	}
	if rel == "" || strings.HasPrefix(rel, "/") || strings.ContainsRune(rel, 0) || len(rel) > 4096 {
		return "", fmt.Errorf("invalid volume-relative path")
	}
	for _, part := range strings.Split(rel, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("invalid volume-relative path")
		}
	}
	if path.Clean(rel) != rel {
		return "", fmt.Errorf("invalid volume-relative path")
	}
	return rel, nil
}

func openVolumePath(root, rel string, directory bool) (*os.File, error) {
	base, err := unix.Open(root, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(base)
	flags := unix.O_RDONLY | unix.O_NONBLOCK
	if directory {
		flags |= unix.O_DIRECTORY
	}
	fd, err := unix.Openat2(base, rel, &unix.OpenHow{Flags: uint64(flags | unix.O_CLOEXEC), Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV})
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), rel), nil
}

func (s *Service) volumeFiles(w http.ResponseWriter, r *http.Request) {
	client, id := clientID(r), r.PathValue("id")
	volume, err := s.store.VolumeRecord(client, id)
	if err != nil {
		fail(w, err)
		return
	}
	if volume.Volume.OperationID != "" {
		fail(w, apiError("BUSY", "Volume has an operation in progress"))
		return
	}
	if volume.Volume.Phase != "available" && volume.Volume.Phase != "failed" {
		fail(w, apiError("CONFLICT", "Volume is not readable in its current phase"))
		return
	}
	access, ok := s.providers[volume.Provider].(volumeFileAccess)
	if !ok {
		fail(w, apiError("UNSUPPORTED_CAPABILITY", "Provider does not support stopped-volume reads"))
		return
	}
	listing := r.URL.Query().Get("list") == "1"
	rel, pathErr := cleanVolumePath(r.URL.Query().Get("path"), listing)
	if pathErr != nil {
		fail(w, apiError("INVALID_REQUEST", pathErr.Error()))
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	defer cancel()
	// The provider admits currently stopped attachments and the node lock keeps
	// this read coordinated with Cellbox backup/delete operations. A concurrent
	// managed-volume resume/runtime writer may not hold that lock, so this is not
	// an atomic snapshot.
	err = access.WithStoppedVolume(ctx, volume.spec(), func(root string) error {
		if listing {
			dir, err := openVolumePath(root, rel, true)
			if err != nil {
				return apiError("NOT_FOUND", "Directory not found")
			}
			defer dir.Close()
			entries, err := dir.ReadDir(10001)
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			if len(entries) > 10000 {
				return apiError("INVALID_REQUEST", "Directory exceeds the 10000 entry listing limit")
			}
			out := make([]volumeFileEntry, 0, len(entries))
			for _, entry := range entries {
				if entry.Type()&os.ModeSymlink != 0 {
					continue
				}
				info, err := entry.Info()
				if err != nil || (!info.IsDir() && !info.Mode().IsRegular()) {
					continue
				}
				out = append(out, volumeFileEntry{Name: entry.Name(), Directory: info.IsDir(), Size: info.Size()})
			}
			writeJSON(w, http.StatusOK, out)
			return nil
		}
		file, err := openVolumePath(root, rel, false)
		if err != nil {
			return apiError("NOT_FOUND", "File not found")
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !info.Mode().IsRegular() {
			return apiError("NOT_FOUND", "File not found")
		}
		if info.Size() > volumeFileLimit {
			return apiError("INVALID_REQUEST", "File exceeds the 16 MiB REST transfer limit")
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; sandbox")
		w.Header().Set("Cache-Control", "private, no-cache")
		http.ServeContent(w, r, path.Base(rel), info.ModTime(), file)
		return nil
	})
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			fail(w, err)
			return
		}
		if errors.Is(err, boxprovider.ErrNotReady) {
			fail(w, apiError("BUSY", "All Boxes referencing the Volume must be stopped"))
			return
		}
		if errors.Is(err, boxprovider.ErrStaleVolume) {
			fail(w, apiError("CONFLICT", "Volume identity differs from its stored filesystem"))
			return
		}
		fail(w, err)
	}
}

func (s *Service) backupFile(w http.ResponseWriter, r *http.Request) {
	client, id, snapshot := clientID(r), r.PathValue("id"), r.PathValue("backupId")
	if !backupIDPattern.MatchString(snapshot) {
		fail(w, apiError("INVALID_REQUEST", "A full 64-character backup ID is required"))
		return
	}
	if _, err := cleanVolumePath(r.URL.Query().Get("path"), false); err != nil {
		fail(w, apiError("INVALID_REQUEST", err.Error()))
		return
	}
	_, err := s.store.VolumeRecord(client, id)
	if err != nil {
		fail(w, err)
		return
	}
	if err := s.requireVolumeBackups(); err != nil {
		fail(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	defer cancel()
	snapshots, err := s.backups.List(ctx, client, id)
	if err != nil {
		fail(w, volumeBackupError(err))
		return
	}
	var snapshotTime time.Time
	for _, candidate := range snapshots {
		if candidate.ID == snapshot {
			snapshotTime = candidate.CreatedAt
			break
		}
	}
	if snapshotTime.IsZero() {
		fail(w, apiError("NOT_FOUND", "Volume backup not found"))
		return
	}
	reader, ok := s.backups.(interface {
		ReadFile(context.Context, string, string, string, string) ([]byte, error)
	})
	if !ok {
		fail(w, apiError("UNSUPPORTED_CAPABILITY", "Backup file reads are unavailable"))
		return
	}
	body, err := reader.ReadFile(ctx, client, id, snapshot, r.URL.Query().Get("path"))
	if err != nil {
		fail(w, volumeBackupError(err))
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, path.Base(r.URL.Query().Get("path")), snapshotTime, bytes.NewReader(body))
}
