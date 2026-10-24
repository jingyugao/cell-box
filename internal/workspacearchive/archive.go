// Package workspacearchive contains the shared, bounded workspace archive
// encoder and validator used by runtime providers.
package workspacearchive

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/archivelimits"
	"cellbox.local/cellbox/internal/objectstorage"
	"golang.org/x/sys/unix"
)

const (
	MaxEntries = 100000
	MaxDepth   = 64
)

var idPattern = regexp.MustCompile(`^arc-[0-9a-f]{32}$`)

type Result struct {
	Size   int64
	SHA256 string
}

// WorkspaceRelative validates a guest workspace path and returns its path
// relative to persistent HOME.
func WorkspaceRelative(workspace string) (string, error) {
	const home = "/home/agent"
	if !filepath.IsAbs(workspace) || filepath.Clean(workspace) != workspace || workspace == home || !strings.HasPrefix(workspace, home+"/") {
		return "", errors.New("configured workspace is outside persistent HOME")
	}
	rel := strings.TrimPrefix(workspace, home+"/")
	if rel == "" || path.Clean(rel) != rel || rel == "." || strings.HasPrefix(rel, "../") {
		return "", errors.New("invalid configured workspace path")
	}
	return filepath.Join(filepath.FromSlash(rel)), nil
}

// OpenWorkspace securely resolves rel beneath root. Every component must be a
// real directory, so symlinked paths cannot escape the volume.
func OpenWorkspace(root, rel string) (*os.File, error) {
	if rel == "" || filepath.IsAbs(rel) || filepath.Clean(rel) != rel || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, errors.New("invalid persistent workspace path")
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for _, component := range strings.Split(rel, string(filepath.Separator)) {
		if component == "" || component == "." || component == ".." {
			unix.Close(fd)
			return nil, errors.New("invalid persistent workspace component")
		}
		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if openErr != nil {
			return nil, openErr
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), filepath.Join(root, rel)), nil
}

type wireLimitWriter struct {
	w     io.Writer
	n     int64
	limit int64
}

func (w *wireLimitWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.limit-w.n {
		return 0, errors.New("workspace archive exceeds wire limit")
	}
	n, err := w.w.Write(p)
	w.n += int64(n)
	return n, err
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// Capture archives root/rel and writes it idempotently to the shared archive
// key. tempDir must be private and owned by the caller.
func Capture(ctx context.Context, root, rel, id, tempDir string, expiresAt time.Time, objects objectstorage.Objects) (Result, error) {
	var result Result
	if objects == nil {
		return result, errors.New("object storage is required for suspended workspace archives")
	}
	if !idPattern.MatchString(id) {
		return result, errors.New("invalid workspace archive identifier")
	}
	key := "archives/" + id + ".tar.gz"
	if existing, _, _, getErr := objects.Get(ctx, key); getErr == nil {
		_ = existing.Close()
		return Existing(ctx, id, objects)
	} else if !errors.Is(getErr, objectstorage.ErrNotFound) {
		return result, getErr
	}
	if !expiresAt.IsZero() && !time.Now().Before(expiresAt) {
		return result, errors.New("workspace archive request expired")
	}
	workspace, err := OpenWorkspace(root, rel)
	if err != nil {
		return result, errors.New("configured persistent workspace directory is unavailable")
	}
	defer workspace.Close()
	if err := os.MkdirAll(tempDir, 0700); err != nil {
		return result, err
	}
	f, err := os.CreateTemp(tempDir, ".workspace-archive-*")
	if err != nil {
		return result, err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return result, err
	}
	h := sha256.New()
	wire := &wireLimitWriter{w: io.MultiWriter(f, h), limit: archivelimits.MaxWireBytes}
	gz := gzip.NewWriter(wire)
	tw := tar.NewWriter(gz)
	count := 0
	var total int64
	err = addDirectory(ctx, tw, workspace, "", 0, &count, &total)
	if closeErr := tw.Close(); err == nil {
		err = closeErr
	}
	if closeErr := gz.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = f.Sync()
	}
	stat, statErr := f.Stat()
	if err == nil {
		err = statErr
	}
	if err == nil && (stat.Size() < 1 || stat.Size() > archivelimits.MaxWireBytes) {
		err = errors.New("workspace archive exceeds wire limit")
	}
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		f.Close()
		return result, err
	}
	size := stat.Size()
	digest := hex.EncodeToString(h.Sum(nil))
	_, err = objects.Put(ctx, key, f, size, "*")
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if errors.Is(err, objectstorage.ErrConflict) {
		return Existing(ctx, id, objects)
	}
	if err != nil {
		return result, err
	}
	return Result{Size: size, SHA256: digest}, nil
}

// Existing verifies the size and completeness of an object already uploaded
// for this archive ID, allowing retries after the caller lost the first reply.
func Existing(ctx context.Context, id string, objects objectstorage.Objects) (Result, error) {
	result := Result{}
	if objects == nil {
		return result, errors.New("object storage is required for suspended workspace archives")
	}
	if !idPattern.MatchString(id) {
		return result, errors.New("invalid workspace archive identifier")
	}
	body, size, _, err := objects.Get(ctx, "archives/"+id+".tar.gz")
	if err != nil {
		return result, err
	}
	defer body.Close()
	if size < 1 || size > archivelimits.MaxWireBytes {
		return result, errors.New("existing workspace archive has invalid size")
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(body, size+1))
	if err != nil || n != size {
		return result, errors.New("existing workspace archive is incomplete")
	}
	return Result{Size: size, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

// Validate reads the full compressed stream and accepts only regular files and
// directories with safe, unique paths and bounded content.
func Validate(r io.Reader) error {
	gz, err := gzip.NewReader(io.LimitReader(r, archivelimits.MaxWireBytes+1))
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
		if count > MaxEntries {
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
		if h.Size < 0 || h.Size > archivelimits.MaxEntryBytes || total+h.Size > archivelimits.MaxContentBytes {
			return errors.New("archive exceeds content limit")
		}
		total += h.Size
		seen[name] = tar.TypeReg
		if _, err := io.CopyN(io.Discard, tr, h.Size); err != nil {
			return fmt.Errorf("truncated archive file: %w", err)
		}
	}
	_, err = copyTail(gz)
	return err
}

func cleanArchiveEntry(name string, isDir bool) error {
	if isDir {
		name = strings.TrimSuffix(name, "/")
	}
	if name == "" || len(name) > 4096 || strings.HasPrefix(name, "/") || strings.ContainsRune(name, 0) || path.Clean(name) != name {
		return errors.New("unsafe archive entry path")
	}
	parts := strings.Split(name, "/")
	if len(parts) > MaxDepth {
		return errors.New("archive entry too deep")
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return errors.New("unsafe archive entry path")
		}
	}
	return nil
}

func copyTail(r io.Reader) (int64, error) {
	n, err := io.Copy(io.Discard, io.LimitReader(r, 1<<20+1))
	if err != nil {
		return n, err
	}
	if n > 1<<20 {
		return n, errors.New("archive has excessive trailing data")
	}
	return n, nil
}

func addDirectory(ctx context.Context, tw *tar.Writer, dir *os.File, rel string, depth int, count *int, total *int64) error {
	if depth > MaxDepth {
		return errors.New("workspace archive nesting too deep")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	remaining := MaxEntries - *count
	entries, err := dir.ReadDir(remaining + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(entries) > remaining {
		return errors.New("too many workspace archive entries")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		*count++
		if *count > MaxEntries {
			return errors.New("too many workspace archive entries")
		}
		name := entry.Name()
		child := name
		if rel != "" {
			child = rel + "/" + name
		}
		if name == "." || name == ".." || path.Clean(child) != child || strings.HasPrefix(child, "../") {
			return errors.New("unsafe workspace archive path")
		}
		var entryStat unix.Stat_t
		if err = unix.Fstatat(int(dir.Fd()), name, &entryStat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}
		if entryStat.Mode&unix.S_IFMT == unix.S_IFLNK || (entryStat.Mode&unix.S_IFMT != unix.S_IFDIR && entryStat.Mode&unix.S_IFMT != unix.S_IFREG) {
			continue
		}
		if entryStat.Mode&unix.S_IFMT == unix.S_IFDIR {
			fd, openErr := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if openErr != nil {
				return openErr
			}
			subdir := os.NewFile(uintptr(fd), name)
			var openedStat unix.Stat_t
			if openErr = unix.Fstat(fd, &openedStat); openErr != nil || openedStat.Dev != entryStat.Dev || openedStat.Ino != entryStat.Ino {
				subdir.Close()
				return errors.New("workspace directory changed during archive capture")
			}
			if err = tw.WriteHeader(&tar.Header{Name: child + "/", Mode: int64(entryStat.Mode & 0777), Typeflag: tar.TypeDir}); err != nil {
				subdir.Close()
				return err
			}
			if err = addDirectory(ctx, tw, subdir, child, depth+1, count, total); err != nil {
				subdir.Close()
				return err
			}
			subdir.Close()
			continue
		}
		if entryStat.Size < 0 || entryStat.Size > archivelimits.MaxEntryBytes || entryStat.Size > archivelimits.MaxContentBytes-*total {
			return errors.New("workspace exceeds archive limits")
		}
		fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		file := os.NewFile(uintptr(fd), name)
		var openedStat unix.Stat_t
		err = unix.Fstat(fd, &openedStat)
		if err != nil || openedStat.Mode&unix.S_IFMT != unix.S_IFREG || openedStat.Dev != entryStat.Dev || openedStat.Ino != entryStat.Ino || openedStat.Size != entryStat.Size {
			file.Close()
			return fmt.Errorf("workspace file changed during archive capture")
		}
		if err = tw.WriteHeader(&tar.Header{Name: child, Mode: int64(openedStat.Mode & 0777), Typeflag: tar.TypeReg, Size: openedStat.Size}); err == nil {
			_, err = io.CopyN(tw, contextReader{ctx: ctx, r: file}, openedStat.Size)
		}
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		*total += openedStat.Size
	}
	return nil
}
