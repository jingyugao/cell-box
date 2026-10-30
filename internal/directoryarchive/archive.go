// Package directoryarchive writes bounded tar.gz archives from a directory
// tree without following symlinks.
package directoryarchive

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
	"sort"
	"strings"

	"github.com/jingyugao/cell-box/internal/archivelimits"
	"golang.org/x/sys/unix"
)

const (
	maxEntries = 100000
	maxDepth   = 64
)

var ErrTooBig = errors.New("directory archive exceeds limits")

// Result describes the compressed tar.gz bytes written to output.
type Result struct {
	SizeBytes int64
	SHA256    string
}

// Write writes a tar.gz containing root/relative. Archive paths are relative
// to the selected directory. relative may be "." to select root itself.
// Symlinks and special files inside the selected tree are skipped. Symlinks
// in root or in the selected relative path are rejected.
func Write(ctx context.Context, root, relative string, output io.Writer) (Result, error) {
	if ctx == nil {
		return Result{}, errors.New("directory archive context is required")
	}
	if output == nil {
		return Result{}, errors.New("directory archive output is required")
	}
	dir, err := openDirectory(root, relative)
	if err != nil {
		return Result{}, err
	}
	defer dir.Close()

	h := sha256.New()
	limited := &wireLimitWriter{w: io.MultiWriter(output, h), limit: archivelimits.MaxWireBytes}
	compressed := gzip.NewWriter(contextWriter{ctx: ctx, w: limited})
	tarWriter := tar.NewWriter(compressed)
	count := 0
	var total int64
	err = addDirectory(ctx, tarWriter, dir, "", 0, &count, &total)
	if closeErr := tarWriter.Close(); err == nil {
		err = closeErr
	}
	if closeErr := compressed.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return Result{}, err
	}
	return Result{SizeBytes: limited.n, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

// WriteTar validates and repacks a tar stream as the same bounded tar.gz
// format produced by Write. The source may be an archive from an untrusted
// snapshot: links and special files are skipped, paths are checked, and size,
// entry-count, and depth limits are applied before writing.
func WriteTar(ctx context.Context, source io.Reader, output io.Writer) (Result, error) {
	if ctx == nil || source == nil || output == nil {
		return Result{}, errors.New("directory archive context, input, and output are required")
	}
	h := sha256.New()
	limited := &wireLimitWriter{w: io.MultiWriter(output, h), limit: archivelimits.MaxWireBytes}
	compressed := gzip.NewWriter(contextWriter{ctx: ctx, w: limited})
	tarWriter := tar.NewWriter(compressed)
	reader := tar.NewReader(source)
	seen := make(map[string]struct{})
	count := 0
	var total int64
	var resultErr error
	for {
		if err := ctx.Err(); err != nil {
			resultErr = err
			break
		}
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			resultErr = fmt.Errorf("invalid directory archive source: %w", err)
			break
		}
		count++
		if count > maxEntries {
			resultErr = ErrTooBig
			break
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA && header.Typeflag != tar.TypeDir {
			continue
		}
		name := header.Name
		if header.Typeflag == tar.TypeDir {
			name = strings.TrimSuffix(name, "/")
		} else if strings.HasSuffix(name, "/") {
			resultErr = errors.New("directory archive contains an invalid file path")
			break
		}
		if header.Typeflag == tar.TypeDir && (name == "." || name == "") {
			continue
		}
		if err := safeTarPath(name); err != nil {
			resultErr = err
			break
		}
		if _, exists := seen[name]; exists {
			resultErr = errors.New("directory archive contains a duplicate path")
			break
		}
		seen[name] = struct{}{}
		if len(strings.Split(name, "/")) > maxDepth {
			resultErr = errors.New("directory archive nesting too deep")
			break
		}
		if header.Typeflag == tar.TypeDir {
			if header.Size != 0 {
				resultErr = errors.New("directory archive contains an invalid directory entry")
				break
			}
			if err := tarWriter.WriteHeader(&tar.Header{Name: name + "/", Mode: header.Mode & 0777, Typeflag: tar.TypeDir}); err != nil {
				resultErr = err
				break
			}
			continue
		}
		if header.Size < 0 || header.Size > archivelimits.MaxEntryBytes || header.Size > archivelimits.MaxContentBytes-total {
			resultErr = ErrTooBig
			break
		}
		if err := tarWriter.WriteHeader(&tar.Header{Name: name, Mode: header.Mode & 0777, Typeflag: tar.TypeReg, Size: header.Size}); err != nil {
			resultErr = err
			break
		}
		if _, err := io.CopyN(tarWriter, contextReader{ctx: ctx, r: reader}, header.Size); err != nil {
			resultErr = err
			break
		}
		total += header.Size
	}
	if err := tarWriter.Close(); resultErr == nil {
		resultErr = err
	}
	if err := compressed.Close(); resultErr == nil {
		resultErr = err
	}
	if resultErr != nil {
		return Result{}, resultErr
	}
	return Result{SizeBytes: limited.n, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

func safeTarPath(name string) error {
	if name == "" || len(name) > 4096 || path.IsAbs(name) || strings.Contains(name, "\\") ||
		strings.IndexFunc(name, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 || path.Clean(name) != name {
		return errors.New("directory archive contains an unsafe path")
	}
	for _, component := range strings.Split(name, "/") {
		if component == "" || component == "." || component == ".." {
			return errors.New("directory archive contains an unsafe path")
		}
	}
	return nil
}

func openDirectory(root, relative string) (*os.File, error) {
	if root == "" {
		return nil, errors.New("directory archive root is required")
	}
	for _, component := range strings.Split(root, string(filepath.Separator)) {
		if component == ".." {
			return nil, errors.New("invalid directory archive root component")
		}
	}
	if relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != relative {
		return nil, errors.New("invalid directory archive relative path")
	}
	if relative != "." {
		for _, component := range strings.Split(relative, string(filepath.Separator)) {
			if component == "" || component == "." || component == ".." {
				return nil, errors.New("invalid directory archive path component")
			}
		}
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	fd, err := unix.Open(string(filepath.Separator), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for _, component := range strings.Split(strings.TrimPrefix(abs, string(filepath.Separator)), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if openErr != nil {
			return nil, openErr
		}
		fd = next
	}
	if relative != "." {
		for _, component := range strings.Split(relative, string(filepath.Separator)) {
			next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			unix.Close(fd)
			if openErr != nil {
				return nil, openErr
			}
			fd = next
		}
	}
	return os.NewFile(uintptr(fd), filepath.Join(abs, relative)), nil
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

type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (w contextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.w.Write(p)
}

type wireLimitWriter struct {
	w     io.Writer
	n     int64
	limit int64
}

func (w *wireLimitWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.limit-w.n {
		return 0, ErrTooBig
	}
	n, err := w.w.Write(p)
	w.n += int64(n)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

func addDirectory(ctx context.Context, tw *tar.Writer, dir *os.File, rel string, depth int, count *int, total *int64) error {
	if depth > maxDepth {
		return errors.New("directory archive nesting too deep")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	remaining := maxEntries - *count
	entries, err := dir.ReadDir(remaining + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(entries) > remaining {
		return errors.New("too many directory archive entries")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		*count++
		if *count > maxEntries {
			return errors.New("too many directory archive entries")
		}
		name := entry.Name()
		child := name
		if rel != "" {
			child = rel + "/" + name
		}
		if name == "." || name == ".." || filepath.ToSlash(filepath.Clean(filepath.FromSlash(child))) != child || strings.HasPrefix(child, "../") {
			return errors.New("unsafe directory archive path")
		}
		var entryStat unix.Stat_t
		if err = unix.Fstatat(int(dir.Fd()), name, &entryStat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}
		kind := entryStat.Mode & unix.S_IFMT
		if kind == unix.S_IFLNK || (kind != unix.S_IFDIR && kind != unix.S_IFREG) {
			continue
		}
		if kind == unix.S_IFDIR {
			fd, openErr := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if openErr != nil {
				return openErr
			}
			subdir := os.NewFile(uintptr(fd), name)
			var openedStat unix.Stat_t
			if openErr = unix.Fstat(fd, &openedStat); openErr != nil || openedStat.Dev != entryStat.Dev || openedStat.Ino != entryStat.Ino {
				subdir.Close()
				return errors.New("directory changed during archive capture")
			}
			if err = tw.WriteHeader(&tar.Header{Name: child + "/", Mode: int64(entryStat.Mode & 0777), Typeflag: tar.TypeDir}); err != nil {
				subdir.Close()
				return err
			}
			if err = addDirectory(ctx, tw, subdir, child, depth+1, count, total); err != nil {
				subdir.Close()
				return err
			}
			if err = subdir.Close(); err != nil {
				return err
			}
			continue
		}
		if entryStat.Size < 0 || entryStat.Size > archivelimits.MaxEntryBytes || entryStat.Size > archivelimits.MaxContentBytes-*total {
			return errors.New("directory archive exceeds content limits")
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
			return fmt.Errorf("directory file changed during archive capture")
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
