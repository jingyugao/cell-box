package service

import (
	"cellbox.local/cellbox/internal/objectstorage"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/archivelimits"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/workspacearchive"
	"golang.org/x/sys/unix"
)

const (
	archiveWireLimit = archivelimits.MaxWireBytes
)

var archiveIDPattern = regexp.MustCompile(`^arc-[0-9a-f]{32}$`)

func (s *Service) archivesDir() (string, error) {
	dir := filepath.Join(s.config.DataDir, "archives")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return "", err
	}
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("archive directory is not a real directory")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return "", err
	}
	return dir, nil
}

func openArchive(name string) (*os.File, error) {
	fd, err := unix.Open(name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

// validateArchive reads every payload byte, including the gzip trailer. It
// rejects links and special entries because the guest restore accepts only
// regular files and directories within its empty workspace.
func validateArchive(r io.Reader) error {
	return workspacearchive.Validate(r)
}

func verifyArchive(f *os.File, a Archive) error {
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || fi.Mode().Perm() != 0600 || fi.Size() != a.Size || a.Size < 1 || a.Size > archiveWireLimit {
		return apiError("ARCHIVE_CORRUPT", "Archive size does not match metadata")
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.LimitReader(f, archiveWireLimit+1)); err != nil {
		return err
	}
	if hex.EncodeToString(h.Sum(nil)) != a.SHA256 {
		return apiError("ARCHIVE_CORRUPT", "Archive checksum does not match metadata")
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	if err := validateArchive(f); err != nil {
		return apiError("ARCHIVE_CORRUPT", "Archive content is invalid")
	}
	_, err = f.Seek(0, 0)
	return err
}

func (s *Service) captureArchive(w http.ResponseWriter, r *http.Request) {
	// Captures never change the source lifecycle. The caller must first suspend
	// the box, or use a failed box whose provider can verify that its writer is
	// stopped. Disk-only capture and automatic quiescing are not supported.
	if err := decodeOptionalLimit(w, r, &struct{}{}, 1024); err != nil {
		fail(w, err)
		return
	}
	client, id := clientID(r), r.PathValue("id")
	var box boxRecord
	op, err := s.prepareOperation(client, "archive", id, func(tx *storeTx, op *Operation) error {
		var err error
		box, err = tx.Box(client, id)
		if err != nil {
			return err
		}
		if err = busy(tx, box); err != nil {
			return err
		}
		active, err := tx.HasActiveExecution(id)
		if err != nil {
			return err
		}
		if active {
			return apiError("BUSY", "Box has an active execution")
		}
		if !supportsWorkspaceArchives(box.Profile.Provider, s.providers[box.Profile.Provider]) || !boxCanArchiveWorkspace(box) || !workspaceUnderPersistentHome(box.Profile.Guest.Workspace) {
			return apiError("UNSUPPORTED_CAPABILITY", "Archives require provider workspace archive support and a persistent HOME workspace")
		}
		if _, ok := s.providers[box.Profile.Provider].(boxprovider.WorkspaceArchiveProvider); !ok || s.objects == nil {
			return apiError("UNSUPPORTED_CAPABILITY", "Archives require provider workspace archive support and object storage")
		}
		if box.Box.Phase != "suspended" && box.Box.Phase != "failed" {
			return apiError("CONFLICT", "Archive requires a suspended or failed box")
		}
		if box.Box.ImageID == "" {
			return apiError("CONFLICT", "Box has no immutable image identity")
		}
		box.Box.OperationID = op.ID
		box.Box.Version++
		return tx.PutBox(box)
	})
	if err != nil {
		fail(w, err)
		return
	}
	s.launch(op, func(ctx context.Context) (map[string]string, error) {
		a, err := s.captureStoppedWorkspaceArchive(ctx, box)
		if err != nil {
			return nil, err
		}
		return map[string]string{"archiveId": a.ID}, nil
	})
	writeJSON(w, http.StatusAccepted, op)
}

func (s *Service) captureStoppedWorkspaceArchive(ctx context.Context, b boxRecord) (Archive, error) {
	if b.Box.Phase != "suspended" && b.Box.Phase != "failed" {
		return Archive{}, apiError("CONFLICT", "Archive requires a suspended or failed box")
	}
	provider, ok := s.providers[b.Profile.Provider].(boxprovider.WorkspaceArchiveProvider)
	if !ok || !supportsWorkspaceArchives(b.Profile.Provider, s.providers[b.Profile.Provider]) || !boxCanArchiveWorkspace(b) || s.objects == nil || !workspaceUnderPersistentHome(b.Profile.Guest.Workspace) {
		return Archive{}, apiError("UNSUPPORTED_CAPABILITY", "Stopped workspace archives are unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 14*time.Minute)
	defer cancel()
	id := randomID("arc-")
	size, digest, err := provider.CaptureWorkspaceArchive(ctx, b.Handle, id)
	if err != nil {
		return Archive{}, err
	}
	if size < 1 || size > archiveWireLimit || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(digest) {
		return Archive{}, apiError("ARCHIVE_CORRUPT", "Node returned invalid archive metadata")
	}
	if err = s.validateStoredCapture(ctx, id, size, digest); err != nil {
		return Archive{}, err
	}
	a := Archive{ID: id, SourceBoxID: b.Box.ID, TemplateID: b.Box.TemplateID, RevisionID: b.Box.RevisionID, TemplateName: b.Box.TemplateName, TemplateVersion: b.Box.TemplateVersion, ProfileID: b.Box.ProfileID, ImageID: b.Box.ImageID,
		Agent: b.Profile.Guest.Agent, SHA256: digest, Size: size, Consistency: "workspace-best-effort", Portable: true,
		CreatedAt: time.Now().UTC(), ManifestVersion: 1}
	if err = s.store.Write(ctx, func(tx *storeTx) error { return tx.PutArchive(archiveRecord{Archive: a, ClientID: b.ClientID}) }); err != nil {
		return Archive{}, err
	}
	return a, nil
}

func workspaceUnderPersistentHome(workspace string) bool {
	return path.IsAbs(workspace) && path.Clean(workspace) == workspace && workspace != "/home/agent" && strings.HasPrefix(workspace, "/home/agent/")
}

// validateStoredCapture makes the API validate node-produced bytes before its
// archive metadata becomes authoritative.
func (s *Service) validateStoredCapture(ctx context.Context, id string, expectedSize int64, expectedSHA256 string) error {
	body, size, _, err := s.objects.Get(ctx, "archives/"+id+".tar.gz")
	if err != nil {
		return err
	}
	defer body.Close()
	if size != expectedSize || size < 1 || size > archiveWireLimit {
		return apiError("ARCHIVE_CORRUPT", "Node archive size does not match metadata")
	}
	dir, err := s.archivesDir()
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".download-capture-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(body, archiveWireLimit+1))
	if err == nil && n > archiveWireLimit {
		err = errors.New("archive exceeds wire limit")
	}
	if err == nil && n != expectedSize {
		err = apiError("ARCHIVE_CORRUPT", "Node archive content is truncated")
	}
	if err == nil && hex.EncodeToString(h.Sum(nil)) != expectedSHA256 {
		err = apiError("ARCHIVE_CORRUPT", "Node archive checksum does not match")
	}
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err == nil {
		err = validateArchive(f)
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (s *Service) restoreArchive(ctx context.Context, boxID string, source Archive) error {
	b, err := s.rawBox(boxID)
	if err != nil {
		return err
	}
	imageChanged := source.ImageID != b.Box.ImageID
	if source.ID != b.RestoreArchiveID || source.ImageID == "" || b.Box.ImageID == "" || imageChanged && !(source.Portable && b.AcceptImageChange) || source.Agent != b.Profile.Guest.Agent {
		return apiError("ARCHIVE_INCOMPATIBLE", "Archive image or agent identity differs from target box")
	}
	if !supportsWorkspaceArchives(b.Profile.Provider, s.providers[b.Profile.Provider]) || !b.Profile.PersistentHome || !workspaceUnderPersistentHome(b.Profile.Guest.Workspace) {
		return apiError("UNSUPPORTED_CAPABILITY", "Archive restore requires a resumable Kubernetes box with persistent HOME workspace")
	}
	if b.Box.Phase != "restoring" || !b.Staged || b.RestoreComplete {
		return apiError("CONFLICT", "Box is not awaiting archive restore")
	}
	f, cleanup, err := s.openStoredArchive(ctx, source)
	if err != nil {
		return err
	}
	defer cleanup()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	res, err := s.guestRequest(ctx, b, "POST", "/v1/restore", f)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if err := guestSuccess(res); err != nil {
		return err
	}
	if err := s.setInventoryStage(ctx, b, "staged"); err != nil {
		return err
	}
	return s.store.ChangeBox(ctx, boxID, func(current *boxRecord) error {
		if current.RestoreArchiveID != source.ID || current.Box.Phase != "restoring" {
			return apiError("CONFLICT", "Restore target changed")
		}
		current.RestoreComplete = true
		current.Box.Version++
		return nil
	})
}

func (s *Service) archiveForClient(client, id string) (Archive, error) {
	rec, err := s.store.ArchiveRecord(client, id)
	if err == nil && rec.Deleting {
		return Archive{}, apiError("NOT_FOUND", "Archive not found")
	}
	return rec.Archive, err
}

func (s *Service) listArchives(w http.ResponseWriter, r *http.Request) {
	records, err := s.store.ArchiveRecords(clientID(r))
	if err != nil {
		fail(w, err)
		return
	}
	out := make([]Archive, 0, len(records))
	for _, rec := range records {
		if !rec.Deleting {
			out = append(out, rec.Archive)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	writeJSON(w, 200, out)
}

func (s *Service) getArchive(w http.ResponseWriter, r *http.Request) {
	a, err := s.archiveForClient(clientID(r), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, a)
}

func (s *Service) archiveContent(w http.ResponseWriter, r *http.Request) {
	a, err := s.archiveForClient(clientID(r), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	f, cleanup, err := s.openStoredArchive(r.Context(), a)
	if err != nil {
		fail(w, err)
		return
	}
	defer cleanup()
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", a.ID+".tar.gz"))
	w.Header().Set("Content-Length", strconv.FormatInt(a.Size, 10))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(200)
	_, _ = io.CopyN(w, f, a.Size)
}

// Downloads are private, verified temporary files so a corrupt S3 object is
// never sent to the guest or to an archive download client.
func (s *Service) openStoredArchive(ctx context.Context, a Archive) (*os.File, func(), error) {
	if s.objects == nil {
		return nil, nil, apiError("UNSUPPORTED_CAPABILITY", "Archive object storage is not configured")
	}
	dir, err := s.archivesDir()
	if err != nil {
		return nil, nil, err
	}
	body, size, _, err := s.objects.Get(ctx, "archives/"+a.ID+".tar.gz")
	if errors.Is(err, objectstorage.ErrNotFound) {
		return nil, nil, apiError("ARCHIVE_CORRUPT", "Archive content is unavailable")
	}
	if err != nil {
		return nil, nil, err
	}
	defer body.Close()
	if size != a.Size || size < 1 || size > archiveWireLimit {
		return nil, nil, apiError("ARCHIVE_CORRUPT", "Archive size differs from metadata")
	}
	file, err := os.CreateTemp(dir, ".download-*")
	if err != nil {
		return nil, nil, err
	}
	name := file.Name()
	cleanup := func() { _ = os.Remove(name) }
	n, copyErr := io.Copy(file, io.LimitReader(body, a.Size+1))
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil || n != a.Size {
		cleanup()
		return nil, nil, apiError("ARCHIVE_CORRUPT", "Archive download is incomplete")
	}
	file, err = openArchive(name)
	if err != nil {
		cleanup()
		return nil, nil, apiError("ARCHIVE_CORRUPT", "Archive content is unavailable")
	}
	if err = verifyArchive(file, a); err != nil {
		file.Close()
		cleanup()
		return nil, nil, err
	}
	return file, func() { file.Close(); cleanup() }, nil
}

func (s *Service) deleteArchive(w http.ResponseWriter, r *http.Request) {
	id, client := r.PathValue("id"), clientID(r)
	if !archiveIDPattern.MatchString(id) {
		fail(w, apiError("INVALID_REQUEST", "Invalid archive ID"))
		return
	}
	if s.objects == nil {
		fail(w, apiError("UNSUPPORTED_CAPABILITY", "Archive object storage is not configured"))
		return
	}
	err := s.store.Write(r.Context(), func(tx *storeTx) error {
		rec, err := tx.Archive(client, id)
		if err != nil {
			return err
		}
		used, err := tx.ArchiveInUse(id)
		if err != nil {
			return err
		}
		if used {
			return apiError("BUSY", "Archive is used by a running restore")
		}
		rec.Deleting = true
		return tx.PutArchive(rec)
	})
	if err != nil {
		fail(w, err)
		return
	}
	if err := s.objects.Delete(r.Context(), "archives/"+id+".tar.gz"); err != nil {
		fail(w, err)
		return
	}
	if err := s.store.Write(r.Context(), func(tx *storeTx) error { return tx.Delete("archives", id) }); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func regularTemps(dir string, matches func(string) bool) ([]string, error) {
	if dir == "" {
		return nil, nil
	}
	info, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("temporary directory must be a real directory")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := []string{}
	for _, entry := range entries {
		if !matches(entry.Name()) {
			continue
		}
		name := filepath.Join(dir, entry.Name())
		info, err := os.Lstat(name)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if info.Mode().IsRegular() {
			names = append(names, name)
		}
	}
	return names, nil
}

func removeRegularTemps(dir string, matches func(string) bool) error {
	names, err := regularTemps(dir, matches)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return nil
	}
	for _, name := range names {
		if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

// Called before any HTTP request or operation starts, while service.lock owns
// the local data directory. Valid archive files and service metadata survive.
func (s *Service) cleanupStartupTemps() error {
	if s.store.lock == nil {
		return errors.New("startup cleanup requires the data directory lock")
	}
	if err := s.initVolumeExports(); err != nil {
		return err
	}
	return removeRegularTemps(filepath.Join(s.config.DataDir, "archives"), func(name string) bool {
		return strings.HasPrefix(name, ".download-")
	})
}
