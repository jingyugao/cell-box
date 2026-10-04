package service

import (
	"archive/tar"
	"cellbox.local/cellbox/internal/objectstorage"
	"compress/gzip"
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

	"golang.org/x/sys/unix"
)

const (
	archiveFileLimit    = int64(64 << 20)
	archiveContentLimit = int64(1 << 30)
	archiveWireLimit    = int64(2 << 30)
	archiveEntryLimit   = 100000
	archiveDepthLimit   = 64
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

func archivePath(dir, id string) (string, error) {
	if !archiveIDPattern.MatchString(id) {
		return "", apiError("INVALID_REQUEST", "Invalid archive ID")
	}
	return filepath.Join(dir, id+".tar.gz"), nil
}

func openArchive(name string) (*os.File, error) {
	fd, err := unix.Open(name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func cleanArchiveEntry(name string, isDir bool) error {
	if isDir {
		name = strings.TrimSuffix(name, "/")
	}
	if name == "" || len(name) > 4096 || strings.HasPrefix(name, "/") || strings.ContainsRune(name, 0) || path.Clean(name) != name {
		return errors.New("unsafe archive entry path")
	}
	parts := strings.Split(name, "/")
	if len(parts) > archiveDepthLimit {
		return errors.New("archive entry too deep")
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return errors.New("unsafe archive entry path")
		}
	}
	return nil
}

// validateArchive reads every payload byte, including the gzip trailer. It
// rejects links and special entries because the guest restore accepts only
// regular files and directories within its empty workspace.
func validateArchive(r io.Reader) error {
	gz, err := gzip.NewReader(io.LimitReader(r, archiveWireLimit+1))
	if err != nil {
		return fmt.Errorf("gzip header: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	seen := map[string]byte{}
	var total int64
	count := 0
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar entry: %w", err)
		}
		count++
		if count > archiveEntryLimit {
			return errors.New("too many archive entries")
		}
		isDir := h.Typeflag == tar.TypeDir
		if !isDir && h.Typeflag != tar.TypeReg {
			return errors.New("archive contains a link or special entry")
		}
		if err := cleanArchiveEntry(h.Name, isDir); err != nil {
			return err
		}
		name := strings.TrimSuffix(h.Name, "/")
		if _, ok := seen[name]; ok {
			return errors.New("duplicate archive entry")
		}
		parent := path.Dir(name)
		if parent != "." && seen[parent] != tar.TypeDir {
			return errors.New("archive parent directory is missing")
		}
		if isDir {
			if h.Size != 0 {
				return errors.New("directory has payload")
			}
			seen[name] = tar.TypeDir
			continue
		}
		if h.Size < 0 || h.Size > archiveFileLimit || total+h.Size > archiveContentLimit {
			return errors.New("archive exceeds content limit")
		}
		total += h.Size
		seen[name] = tar.TypeReg
		if _, err := io.CopyN(io.Discard, tr, h.Size); err != nil {
			return fmt.Errorf("truncated archive file: %w", err)
		}
	}
	if _, err := copyArchiveTail(gz); err != nil {
		return err
	}
	return nil
}

func copyArchiveTail(r io.Reader) (int64, error) {
	n, err := io.Copy(io.Discard, io.LimitReader(r, 1<<20+1))
	if err != nil {
		return n, err
	}
	if n > 1<<20 {
		return n, errors.New("archive has excessive trailing data")
	}
	return n, nil
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
	client, id := clientID(r), r.PathValue("id")
	var box boxRecord
	op, fresh, err := s.prepareOperation(client, r.Header.Get("Idempotency-Key"), "archive", id, nil, func(st *State, op *Operation) error {
		var err error
		box, err = owned(st, client, id)
		if err != nil {
			return err
		}
		if err = busy(st, box, false); err != nil {
			return err
		}
		if activeExec(st, id) {
			return apiError("BUSY", "Box has an active execution")
		}
		if box.Box.Phase != "running" && box.Box.Phase != "staged" {
			return apiError("CONFLICT", "Archive requires an accessible box")
		}
		if box.Box.ImageID == "" {
			return apiError("CONFLICT", "Box has no immutable image identity")
		}
		box.Box.OperationID = op.ID
		box.Box.Version++
		st.Boxes[id] = box
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	if fresh {
		s.launch(op, func(ctx context.Context) (map[string]string, error) {
			a, err := s.captureArchiveHTTP(ctx, box)
			if err != nil {
				return nil, err
			}
			return map[string]string{"archiveId": a.ID}, nil
		})
	}
	writeJSON(w, http.StatusAccepted, op)
}

func (s *Service) captureArchiveHTTP(ctx context.Context, b boxRecord) (Archive, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	res, err := s.guestRequest(ctx, b, "GET", "/v1/archive", nil)
	if err != nil {
		return Archive{}, err
	}
	defer res.Body.Close()
	if err := guestSuccess(res); err != nil {
		return Archive{}, err
	}
	dir, err := s.archivesDir()
	if err != nil {
		return Archive{}, err
	}
	f, err := os.CreateTemp(dir, captureTempPrefix(b.Box.ID)+"*")
	if err != nil {
		return Archive{}, err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return Archive{}, err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(res.Body, archiveWireLimit+1))
	if err == nil && n > archiveWireLimit {
		err = errors.New("archive exceeds wire limit")
	}
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		_, err = f.Seek(0, 0)
	}
	if err == nil {
		err = validateArchive(f)
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return Archive{}, err
	}
	a := Archive{ID: randomID("arc-"), SourceBoxID: b.Box.ID, ProfileID: b.Box.ProfileID, ImageID: b.Box.ImageID, Agent: b.Profile.Guest.Agent, SHA256: hex.EncodeToString(h.Sum(nil)), Size: n, Consistency: "workspace-best-effort", Portable: true, CreatedAt: time.Now().UTC(), ManifestVersion: 1, ImportedImageID: b.Box.ImportedImageID, PreparedImage: b.Profile.Image}
	final, err := archivePath(dir, a.ID)
	if err != nil {
		return Archive{}, err
	}
	if err := os.Rename(temp, final); err != nil {
		return Archive{}, err
	}
	d, err := os.Open(dir)
	if err != nil {
		return Archive{}, err
	}
	syncErr := d.Sync()
	_ = d.Close()
	if syncErr != nil {
		return Archive{}, syncErr
	}
	if s.objects != nil {
		file, err := openArchive(final)
		if err != nil {
			return Archive{}, err
		}
		_, uploadErr := s.objects.Put(ctx, "archives/"+a.ID+".tar.gz", file, a.Size, "")
		closeErr := file.Close()
		if uploadErr != nil {
			return Archive{}, uploadErr
		}
		if closeErr != nil {
			return Archive{}, closeErr
		}
		// S3 is authoritative after upload. API-local content is temporary only.
		defer os.Remove(final)
	}
	// Commit the content before publishing metadata. A failed ledger write may
	// have committed remotely, so never remove the S3 object on that failure.
	if err := s.store.Update(func(st *State) error { st.Archives[a.ID] = archiveRecord{Archive: a, ClientID: b.ClientID}; return nil }); err != nil {
		return Archive{}, err
	}
	return a, nil
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
	return s.store.Update(func(st *State) error {
		current, ok := st.Boxes[boxID]
		if !ok || current.RestoreArchiveID != source.ID || current.Box.Phase != "restoring" {
			return apiError("CONFLICT", "Restore target changed")
		}
		current.RestoreComplete = true
		current.Box.Version++
		st.Boxes[boxID] = current
		return nil
	})
}

func (s *Service) archiveForClient(client, id string) (Archive, error) {
	var a Archive
	err := s.store.View(func(st State) error {
		rec, ok := st.Archives[id]
		if !ok || rec.ClientID != client || rec.Deleting {
			return apiError("NOT_FOUND", "Archive not found")
		}
		a = rec.Archive
		return nil
	})
	return a, err
}

func (s *Service) listArchives(w http.ResponseWriter, r *http.Request) {
	out := []Archive{}
	err := s.store.View(func(st State) error {
		for _, rec := range st.Archives {
			if rec.ClientID == clientID(r) && !rec.Deleting {
				out = append(out, rec.Archive)
			}
		}
		return nil
	})
	if err != nil {
		fail(w, err)
		return
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
	dir, err := s.archivesDir()
	if err != nil {
		return nil, nil, err
	}
	name, err := archivePath(dir, a.ID)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {}
	if s.objects != nil {
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
		name = file.Name()
		n, copyErr := io.Copy(file, io.LimitReader(body, a.Size+1))
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil || n != a.Size {
			os.Remove(name)
			return nil, nil, apiError("ARCHIVE_CORRUPT", "Archive download is incomplete")
		}
		cleanup = func() { _ = os.Remove(name) }
	}
	file, err := openArchive(name)
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
	err := s.store.Update(func(st *State) error {
		rec, ok := st.Archives[id]
		if !ok || rec.ClientID != client {
			return apiError("NOT_FOUND", "Archive not found")
		}
		for _, b := range st.Boxes {
			if b.RestoreArchiveID != id {
				continue
			}
			if b.Box.OperationID != "" {
				if op, ok := st.Operations[b.Box.OperationID]; ok && (op.Operation.Status == "queued" || op.Operation.Status == "running") {
					return apiError("BUSY", "Archive is used by a running restore")
				}
			}
		}
		rec.Deleting = true
		st.Archives[id] = rec
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	if s.objects != nil {
		if err := s.objects.Delete(r.Context(), "archives/"+id+".tar.gz"); err != nil {
			fail(w, err)
			return
		}
	} else {
		dir, err := s.archivesDir()
		if err != nil {
			fail(w, err)
			return
		}
		name, err := archivePath(dir, id)
		if err != nil {
			fail(w, err)
			return
		}
		if err = os.Remove(name); err != nil && !os.IsNotExist(err) {
			fail(w, err)
			return
		}
	}
	if err := s.store.Update(func(st *State) error { delete(st.Archives, id); return nil }); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Fixed-length ownership avoids a missing short ID matching another box's
// capture prefix. Capture scratch content is never a retained archive.
func captureTempPrefix(boxID string) string {
	digest := sha256.Sum256([]byte(boxID))
	return ".capture-" + hex.EncodeToString(digest[:]) + "-"
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

func (s *Service) captureTemps(boxID string) ([]string, error) {
	if s.config.DataDir == "" {
		return nil, nil
	}
	return regularTemps(filepath.Join(s.config.DataDir, "archives"), func(name string) bool { return strings.HasPrefix(name, captureTempPrefix(boxID)) })
}
func (s *Service) cleanupCaptureTemps(boxID string) error {
	if s.config.DataDir == "" {
		return nil
	}
	return removeRegularTemps(filepath.Join(s.config.DataDir, "archives"), func(name string) bool { return strings.HasPrefix(name, captureTempPrefix(boxID)) })
}

// Called before any HTTP request or operation starts, while service.lock owns
// the local data directory. Valid archive files and service metadata survive.
func (s *Service) cleanupStartupTemps() error {
	if s.store.lock == nil {
		return errors.New("startup cleanup requires the data directory lock")
	}
	if err := removeRegularTemps(s.config.DataDir, func(name string) bool { return strings.HasPrefix(name, ".state-") }); err != nil {
		return err
	}
	return removeRegularTemps(filepath.Join(s.config.DataDir, "archives"), func(name string) bool {
		return strings.HasPrefix(name, ".capture-") || strings.HasPrefix(name, ".download-")
	})
}
