package guest

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	"cellbox.local/cellbox/internal/archivelimits"
	"cellbox.local/cellbox/internal/guestapi"
	"golang.org/x/sys/unix"
)

const (
	resolveFlags   = unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV
	maxFileSize    = 64 << 20
	maxArchiveSize = archivelimits.MaxContentBytes
)

func cleanRelative(p string, allowRoot bool) (string, error) {
	if (p == "" || p == ".") && allowRoot {
		return ".", nil
	}
	if p == "" || strings.HasPrefix(p, "/") || strings.ContainsRune(p, 0) || len(p) > 4096 {
		return "", errors.New("invalid workspace path")
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", errors.New("unsafe workspace path")
		}
	}
	if path.Clean(p) != p {
		return "", errors.New("unclean workspace path")
	}
	return p, nil
}

func openWorkspace(root, rel string, flags int, mode uint64) (*os.File, error) {
	p, err := cleanRelative(rel, true)
	if err != nil {
		return nil, err
	}
	base, err := unix.Open(root, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(base)
	fd, err := unix.Openat2(base, p, &unix.OpenHow{Flags: uint64(flags | unix.O_CLOEXEC), Mode: mode, Resolve: resolveFlags})
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "/proc/self/fd/"+strconv.Itoa(fd)), nil
}

func workspaceDir(root, rel string) (*os.File, error) {
	if rel == "" {
		rel = "."
	}
	return openWorkspace(root, rel, unix.O_RDONLY|unix.O_DIRECTORY, 0)
}

func (s *Server) fileHandler(w http.ResponseWriter, r *http.Request) {
	rel := r.URL.Query().Get("path")
	if _, err := cleanRelative(rel, r.URL.Query().Get("list") == "1"); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if r.Method == "GET" && r.URL.Query().Get("list") == "1" {
		f, err := workspaceDir(s.cfg.Workspace, rel)
		if err != nil {
			http.Error(w, "directory unavailable", 400)
			return
		}
		defer f.Close()
		list, err := f.ReadDir(-1)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		out := make([]guestapi.FileEntry, 0, len(list))
		for _, e := range list {
			if e.Type()&os.ModeSymlink != 0 {
				continue
			}
			info, err := e.Info()
			if err != nil || (!info.IsDir() && !info.Mode().IsRegular()) {
				continue
			}
			out = append(out, guestapi.FileEntry{Name: e.Name(), Directory: info.IsDir(), Size: info.Size()})
		}
		writeJSON(w, 200, out)
		return
	}
	if r.Method == "GET" || r.Method == "HEAD" {
		f, err := openWorkspace(s.cfg.Workspace, rel, unix.O_RDONLY|unix.O_NOFOLLOW, 0)
		if err != nil {
			http.Error(w, "file unavailable", 404)
			return
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil || !info.Mode().IsRegular() {
			http.Error(w, "not a regular file", 400)
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; sandbox")
		if r.Method == "HEAD" {
			// Range is defined for GET only; ServeContent otherwise applies it to HEAD.
			r = r.Clone(r.Context())
			r.Header.Del("Range")
			r.Header.Del("If-Range")
		}
		http.ServeContent(w, r, path.Base(rel), info.ModTime(), f)
		return
	}
	if r.Method == "PUT" {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.restoring {
			http.Error(w, "restore in progress", 409)
			return
		}
		f, err := openWorkspace(s.cfg.Workspace, rel, unix.O_WRONLY|unix.O_CREAT|unix.O_TRUNC|unix.O_NOFOLLOW, 0640)
		if err != nil {
			http.Error(w, "file unavailable", 400)
			return
		}
		defer f.Close()
		if err := f.Chown(int(s.cfg.Agent.UID), int(s.cfg.Agent.GID)); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		n, err := copyLimited(f, http.MaxBytesReader(w, r.Body, maxFileSize+1), maxFileSize)
		if err != nil {
			_ = f.Truncate(0)
			http.Error(w, err.Error(), 413)
			return
		}
		_ = n
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s *Server) archiveHandler(w http.ResponseWriter, r *http.Request) {
	f, err := workspaceDir(s.cfg.Workspace, "")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	f.Close()
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", "attachment; filename=workspace.tar.gz")
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	remaining := int64(maxArchiveSize)
	count := 0
	if err := s.archiveDir(tw, "", 0, &remaining, &count); err != nil {
		panic(http.ErrAbortHandler)
	}
	if err := tw.Close(); err != nil {
		panic(http.ErrAbortHandler)
	}
	if err := gz.Close(); err != nil {
		panic(http.ErrAbortHandler)
	}
}

func (s *Server) archiveDir(tw *tar.Writer, rel string, depth int, remaining *int64, count *int) error {
	if depth > 64 {
		return errors.New("archive nesting too deep")
	}
	d, err := workspaceDir(s.cfg.Workspace, rel)
	if err != nil {
		return err
	}
	defer d.Close()
	entries, err := d.ReadDir(-1)
	if err != nil {
		return err
	}
	for _, e := range entries {
		*count++
		if *count > 100000 {
			return errors.New("too many archive entries")
		}
		name := e.Name()
		if name == "." || name == ".." {
			continue
		}
		child := name
		if rel != "" {
			child = rel + "/" + name
		}
		if e.IsDir() {
			info, err := e.Info()
			if err != nil {
				return err
			}
			if err := tw.WriteHeader(&tar.Header{Name: child + "/", Mode: int64(info.Mode().Perm()), Typeflag: tar.TypeDir}); err != nil {
				return err
			}
			if err := s.archiveDir(tw, child, depth+1, remaining, count); err != nil {
				return err
			}
			continue
		}
		if !e.Type().IsRegular() {
			continue
		}
		f, err := openWorkspace(s.cfg.Workspace, child, unix.O_RDONLY|unix.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil || !info.Mode().IsRegular() {
			f.Close()
			return fmt.Errorf("archive file changed: %s", child)
		}
		if info.Size() > archivelimits.MaxEntryBytes || info.Size() > *remaining {
			f.Close()
			return errors.New("workspace exceeds archive limits")
		}
		*remaining -= info.Size()
		if err := tw.WriteHeader(&tar.Header{Name: child, Mode: int64(info.Mode().Perm()), Typeflag: tar.TypeReg, Size: info.Size()}); err != nil {
			f.Close()
			return err
		}
		_, err = io.CopyN(tw, f, info.Size())
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) restoreHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	active := s.active || s.restoring
	if !active {
		s.restoring = true
	}
	s.mu.Unlock()
	if active {
		http.Error(w, "restore requires staged workspace", 409)
		return
	}
	defer func() { s.mu.Lock(); s.restoring = false; s.mu.Unlock() }()
	d, err := workspaceDir(s.cfg.Workspace, "")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	entries, err := d.ReadDir(1)
	d.Close()
	if err != nil && err != io.EOF {
		http.Error(w, err.Error(), 500)
		return
	}
	if len(entries) > 0 {
		http.Error(w, "workspace must be empty", 409)
		return
	}
	gz, err := gzip.NewReader(http.MaxBytesReader(w, r.Body, archivelimits.MaxWireBytes))
	if err != nil {
		http.Error(w, "invalid gzip", 400)
		return
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var bytes int64
	count := 0
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, "invalid archive", 400)
			return
		}
		count++
		if count > 100000 {
			http.Error(w, "too many archive entries", 413)
			return
		}
		name := strings.TrimSuffix(h.Name, "/")
		if _, err := cleanRelative(name, false); err != nil {
			http.Error(w, "unsafe archive path", 400)
			return
		}
		if h.Typeflag == tar.TypeDir {
			if err := s.makeRestoreDir(name, os.FileMode(h.Mode)&0777); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			continue
		}
		if h.Typeflag != tar.TypeReg || h.Size < 0 || h.Size > archivelimits.MaxEntryBytes || bytes+h.Size > maxArchiveSize {
			http.Error(w, "unsupported archive entry", 400)
			return
		}
		bytes += h.Size
		f, err := openWorkspace(s.cfg.Workspace, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW, 0640)
		if err != nil {
			http.Error(w, "archive path unavailable", 400)
			return
		}
		if err = f.Chown(int(s.cfg.Agent.UID), int(s.cfg.Agent.GID)); err == nil {
			_, err = io.CopyN(f, tr, h.Size)
		}
		if err == nil {
			err = f.Chmod(os.FileMode(h.Mode) & 0777)
		}
		f.Close()
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
	}
	if _, err := copyLimited(io.Discard, gz, 1<<20); err != nil {
		http.Error(w, "invalid archive trailer", 400)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) makeRestoreDir(rel string, mode os.FileMode) error {
	parent, name := path.Split(rel)
	parent = strings.TrimSuffix(parent, "/")
	if parent == "" {
		parent = "."
	}
	d, err := workspaceDir(s.cfg.Workspace, parent)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := unix.Mkdirat(int(d.Fd()), name, 0750); err != nil {
		return err
	}
	f, err := openWorkspace(s.cfg.Workspace, rel, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Chown(int(s.cfg.Agent.UID), int(s.cfg.Agent.GID)); err != nil {
		return err
	}
	return f.Chmod(mode)
}
