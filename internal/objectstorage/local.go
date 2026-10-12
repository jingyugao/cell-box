package objectstorage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	localLockName      = ".lock"
	localReadyName     = ".initialized"
	localWritePrefix   = ".write-"
	localMaxObjectSize = int64(64 << 20)
	localMaxETagSize   = 4096
	localHeaderSize    = 8 + 4 + 8
)

var localEnvelopeMagic = [8]byte{'C', 'B', 'O', 'B', 'J', 0, 0, 1}

// Local stores S3-compatible objects as one atomic envelope per object.
// ETags are retained independently from the payload so imported S3 versions
// continue to satisfy existing compare-and-swap records.
type Local struct {
	mu      sync.RWMutex
	root    *os.Root
	ownerID int
	groupID int
	closed  bool
}

var _ Objects = (*Local)(nil)

// NewLocal opens a durable directory-backed Objects implementation. The
// supplied directory is created if absent; its parent must already exist.
func NewLocal(dir string) (*Local, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("local object directory is required")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve local object directory: %w", err)
	}
	abs = filepath.Clean(abs)
	if abs == string(filepath.Separator) {
		return nil, errors.New("local object directory cannot be filesystem root")
	}

	parentPath := filepath.Dir(abs)
	parentInfo, err := os.Lstat(parentPath)
	if err != nil {
		return nil, fmt.Errorf("inspect local object parent: %w", err)
	}
	if !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("local object parent must be a real directory")
	}
	ownerID, groupID, err := fileOwner(parentInfo)
	if err != nil {
		return nil, err
	}

	rootInfo, err := os.Lstat(abs)
	if errors.Is(err, os.ErrNotExist) {
		if err = os.Mkdir(abs, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create local object directory: %w", err)
		}
		rootInfo, err = os.Lstat(abs)
		if err != nil {
			return nil, fmt.Errorf("inspect created local object directory: %w", err)
		}
		if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
			return nil, errors.New("local object directory must be a real directory")
		}
		if os.Geteuid() == 0 {
			if err = os.Chown(abs, ownerID, groupID); err != nil {
				return nil, fmt.Errorf("set local object directory owner: %w", err)
			}
		}
		if err = os.Chmod(abs, 0700); err != nil {
			return nil, fmt.Errorf("set local object directory mode: %w", err)
		}
		if err = syncOSDir(parentPath); err != nil {
			return nil, fmt.Errorf("sync local object parent: %w", err)
		}
		if err = syncOSDir(abs); err != nil {
			return nil, fmt.Errorf("sync local object directory: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("inspect local object directory: %w", err)
	} else if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, errors.New("local object directory must be a real directory")
	} else {
		ownerID, groupID, err = fileOwner(rootInfo)
		if err != nil {
			return nil, err
		}
	}

	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("open local object directory: %w", err)
	}
	l := &Local{root: root, ownerID: ownerID, groupID: groupID}
	if err = l.ensureLockFile(); err != nil {
		_ = root.Close()
		return nil, err
	}
	return l, nil
}

// Close releases the directory handle. Close is safe to call more than once.
func (l *Local) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	return l.root.Close()
}

func (l *Local) Get(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	if err := validateLocalKey(key); err != nil {
		return nil, 0, "", err
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, "", err
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.checkOpen(); err != nil {
		return nil, 0, "", err
	}
	data, etag, err := l.readObject(key)
	if err != nil {
		return nil, 0, "", err
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), etag, nil
}

func (l *Local) List(ctx context.Context, prefix string) ([]string, error) {
	prefix, err := validateLocalPrefix(prefix)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err = l.checkOpen(); err != nil {
		return nil, err
	}
	unlock, err := l.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	return l.listUnlocked(ctx, prefix)
}

func (l *Local) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, condition string) (string, error) {
	if err := validateLocalKey(key); err != nil {
		return "", err
	}
	if body == nil || size < 0 || size > localMaxObjectSize {
		return "", errors.New("invalid local object size")
	}
	if condition != "" && condition != "*" && len(condition) > localMaxETagSize {
		return "", errors.New("invalid local object condition")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	data, err := readSeekable(body, size)
	if err != nil {
		return "", err
	}
	etag := localETag(data)

	l.mu.RLock()
	defer l.mu.RUnlock()
	if err = l.checkOpen(); err != nil {
		return "", err
	}
	unlock, err := l.lock(ctx)
	if err != nil {
		return "", err
	}
	defer unlock()
	_, currentETag, readErr := l.readObject(key)
	exists := readErr == nil
	if readErr != nil && !errors.Is(readErr, ErrNotFound) {
		return "", readErr
	}
	if condition == "*" && exists || condition != "" && condition != "*" && (!exists || currentETag != condition) {
		return "", ErrConflict
	}
	if err = ctx.Err(); err != nil {
		return "", err
	}
	if err = l.atomicWrite(key, etag, data); err != nil {
		return "", err
	}
	return etag, nil
}

// Import creates an object with its original ETag. Repeating the same import
// is idempotent; any existing object with different bytes or ETag conflicts.
func (l *Local) Import(ctx context.Context, key string, body io.ReadSeeker, size int64, etag string) error {
	if err := validateLocalKey(key); err != nil {
		return err
	}
	if body == nil || size < 0 || size > localMaxObjectSize || etag == "" || len(etag) > localMaxETagSize {
		return errors.New("invalid local object import")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := readSeekable(body, size)
	if err != nil {
		return err
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err = l.checkOpen(); err != nil {
		return err
	}
	unlock, err := l.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	existing, currentETag, readErr := l.readObject(key)
	if readErr == nil {
		if currentETag == etag && bytes.Equal(existing, data) {
			return nil
		}
		return ErrConflict
	}
	if !errors.Is(readErr, ErrNotFound) {
		return readErr
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return l.atomicWrite(key, etag, data)
}

func (l *Local) Delete(ctx context.Context, key string) error {
	if err := validateLocalKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.checkOpen(); err != nil {
		return err
	}
	unlock, err := l.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	return l.deleteUnlocked(key)
}

func (l *Local) DeleteIfMatch(ctx context.Context, key, etag string) error {
	if err := validateLocalKey(key); err != nil {
		return err
	}
	if etag == "" || len(etag) > localMaxETagSize {
		return errors.New("object ETag is required for conditional delete")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.checkOpen(); err != nil {
		return err
	}
	unlock, err := l.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	_, currentETag, err := l.readObject(key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if currentETag != etag {
		return ErrConflict
	}
	return l.deleteUnlocked(key)
}

func (l *Local) DeletePrefix(ctx context.Context, prefix string) error {
	prefix, err := validateLocalPrefix(prefix)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err = l.checkOpen(); err != nil {
		return err
	}
	unlock, err := l.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	keys, err := l.listUnlocked(ctx, prefix)
	if err != nil {
		return err
	}
	parents := make(map[string]struct{})
	for _, key := range keys {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = l.removeObject(key); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		parents[path.Dir(key)] = struct{}{}
	}
	for parent := range parents {
		if err = l.syncDir(parent); err != nil {
			return fmt.Errorf("sync local object deletion: %w", err)
		}
	}
	return nil
}

func (l *Local) listUnlocked(ctx context.Context, prefix string) ([]string, error) {
	wanted := ""
	if prefix != "" {
		wanted = prefix + "/"
	}
	var keys []string
	err := fs.WalkDir(l.root.FS(), ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if walkErr != nil {
			return walkErr
		}
		if name == "." {
			return nil
		}
		base := path.Base(name)
		if name == localLockName || base == localReadyName || strings.HasPrefix(base, localWritePrefix) {
			if entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		info, infoErr := l.root.Lstat(name)
		if infoErr != nil {
			return infoErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("local object tree contains a symlink: %q", name)
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("local object tree contains a non-regular entry: %q", name)
		}
		key := filepath.ToSlash(name)
		if wanted != "" && !strings.HasPrefix(key, wanted) {
			return nil
		}
		if err := validateLocalKey(key); err != nil {
			return err
		}
		keys = append(keys, key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

func (l *Local) deleteUnlocked(key string) error {
	if _, _, err := l.readObject(key); errors.Is(err, ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	if err := l.removeObject(key); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	return l.syncDir(path.Dir(key))
}

func (l *Local) removeObject(key string) error {
	if err := l.checkNoSymlinkPath(key, false); err != nil {
		return err
	}
	if err := l.root.Remove(filepath.FromSlash(key)); err != nil {
		return err
	}
	return nil
}

func (l *Local) readObject(key string) ([]byte, string, error) {
	if err := l.checkNoSymlinkPath(key, false); err != nil {
		return nil, "", err
	}
	name := filepath.FromSlash(key)
	info, err := l.root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, "", fmt.Errorf("local object is not a regular file: %q", key)
	}
	f, err := l.root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, "", ErrNotFound
		}
		return nil, "", err
	}
	defer f.Close()
	openedInfo, err := f.Stat()
	if err != nil {
		return nil, "", err
	}
	if !openedInfo.Mode().IsRegular() {
		return nil, "", errors.New("local object is not a regular file")
	}
	// Atomic replacement may occur between Lstat and OpenFile. Validate the
	// version actually opened; comparing its size with the earlier pathname
	// lookup would make a normal concurrent Put spuriously fail Get.
	maxSize := int64(localHeaderSize+localMaxETagSize) + localMaxObjectSize
	if openedInfo.Size() < localHeaderSize || openedInfo.Size() > maxSize {
		return nil, "", errors.New("invalid local object envelope size")
	}
	all, err := io.ReadAll(io.LimitReader(f, maxSize+1))
	if err != nil {
		return nil, "", err
	}
	if int64(len(all)) != openedInfo.Size() || len(all) < localHeaderSize || !bytes.Equal(all[:8], localEnvelopeMagic[:]) {
		return nil, "", errors.New("invalid local object envelope")
	}
	etagSize := int64(binary.BigEndian.Uint32(all[8:12]))
	bodySize := int64(binary.BigEndian.Uint64(all[12:20]))
	if etagSize < 1 || etagSize > localMaxETagSize || bodySize < 0 || bodySize > localMaxObjectSize || int64(len(all)) != localHeaderSize+etagSize+bodySize {
		return nil, "", errors.New("invalid local object envelope header")
	}
	etag := string(all[localHeaderSize : localHeaderSize+etagSize])
	if etag == "" {
		return nil, "", errors.New("local object has an empty ETag")
	}
	data := bytes.Clone(all[localHeaderSize+etagSize:])
	return data, etag, nil
}

func (l *Local) atomicWrite(key, etag string, data []byte) error {
	if err := l.ensureParentDirs(path.Dir(key)); err != nil {
		return err
	}
	if err := l.checkNoSymlinkPath(key, true); err != nil {
		return err
	}
	parent := path.Dir(key)
	if parent == "." {
		parent = ""
	}
	tempBase, err := randomTempName()
	if err != nil {
		return err
	}
	temp := tempBase
	if parent != "" {
		temp = parent + "/" + tempBase
	}
	name := filepath.FromSlash(temp)
	f, err := l.root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		_ = f.Close()
		if !committed {
			_ = l.root.Remove(name)
		}
	}()
	if err = f.Chmod(0600); err != nil {
		return err
	}
	if err = l.chown(temp); err != nil {
		return err
	}
	header := make([]byte, localHeaderSize)
	copy(header[:8], localEnvelopeMagic[:])
	binary.BigEndian.PutUint32(header[8:12], uint32(len(etag)))
	binary.BigEndian.PutUint64(header[12:20], uint64(len(data)))
	if err = writeFull(f, header); err != nil {
		return err
	}
	if err = writeFull(f, []byte(etag)); err != nil {
		return err
	}
	if err = writeFull(f, data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = l.root.Rename(name, filepath.FromSlash(key)); err != nil {
		return err
	}
	committed = true
	return l.syncDir(parent)
}

func (l *Local) ensureParentDirs(dir string) error {
	if dir == "." || dir == "" {
		return nil
	}
	parts := strings.Split(dir, "/")
	current := ""
	for _, part := range parts {
		if current == "" {
			current = part
		} else {
			current += "/" + part
		}
		name := filepath.FromSlash(current)
		info, err := l.root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			if err = l.root.Mkdir(name, 0700); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			info, err = l.root.Lstat(name)
			if err != nil {
				return err
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return fmt.Errorf("local object path component is not a directory: %q", current)
			}
			if err = l.chown(current); err != nil {
				return err
			}
			if err = l.root.Chmod(name, 0700); err != nil {
				return err
			}
			if err = l.syncDir(path.Dir(current)); err != nil {
				return fmt.Errorf("sync parent after creating local object directory: %w", err)
			}
			if err = l.syncDir(current); err != nil {
				return fmt.Errorf("sync new local object directory: %w", err)
			}
			continue
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("local object path component is not a directory: %q", current)
		}
	}
	return nil
}

func (l *Local) checkNoSymlinkPath(key string, includeFinal bool) error {
	parts := strings.Split(key, "/")
	limit := len(parts)
	if !includeFinal {
		limit--
	}
	current := ""
	for i := 0; i < limit; i++ {
		if current == "" {
			current = parts[i]
		} else {
			current += "/" + parts[i]
		}
		info, err := l.root.Lstat(filepath.FromSlash(current))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("local object path contains a symlink: %q", current)
		}
		if i < len(parts)-1 && !info.IsDir() {
			return fmt.Errorf("local object path component is not a directory: %q", current)
		}
	}
	return nil
}

func (l *Local) syncDir(dir string) error {
	name := "."
	if dir != "" && dir != "." {
		name = filepath.FromSlash(dir)
	}
	f, err := l.root.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (l *Local) chown(name string) error {
	if os.Geteuid() != 0 {
		return nil
	}
	return l.root.Chown(filepath.FromSlash(name), l.ownerID, l.groupID)
}

func (l *Local) ensureLockFile() error {
	info, err := l.root.Lstat(localLockName)
	if errors.Is(err, os.ErrNotExist) {
		f, openErr := l.root.OpenFile(localLockName, os.O_CREATE|os.O_EXCL|os.O_RDWR|unix.O_NOFOLLOW, 0600)
		if openErr != nil && !errors.Is(openErr, os.ErrExist) {
			return fmt.Errorf("create local object lock: %w", openErr)
		}
		if openErr == nil {
			if err = f.Chmod(0600); err == nil {
				err = l.chown(localLockName)
			}
			if err == nil {
				err = f.Sync()
			}
			closeErr := f.Close()
			if err == nil {
				err = closeErr
			}
			if err == nil {
				err = l.syncDir("")
			}
			if err != nil {
				return fmt.Errorf("initialize local object lock: %w", err)
			}
		}
		info, err = l.root.Lstat(localLockName)
	} else if err != nil {
		return fmt.Errorf("inspect local object lock: %w", err)
	}
	if err != nil {
		return fmt.Errorf("inspect local object lock: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("local object lock must be a regular file")
	}
	if os.Geteuid() == 0 {
		if err = l.root.Chown(localLockName, l.ownerID, l.groupID); err != nil {
			return fmt.Errorf("set local object lock owner: %w", err)
		}
		if err = l.root.Chmod(localLockName, 0600); err != nil {
			return fmt.Errorf("set local object lock mode: %w", err)
		}
	}
	return nil
}

func (l *Local) lock(ctx context.Context) (func(), error) {
	f, err := l.root.OpenFile(localLockName, os.O_RDWR|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open local object lock: %w", err)
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("local object lock must be a regular file")
	}
	for {
		if err = ctx.Err(); err != nil {
			f.Close()
			return nil, err
		}
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
				_ = f.Close()
				return nil, ctxErr
			}
			return func() {
				_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
				_ = f.Close()
			}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			f.Close()
			return nil, fmt.Errorf("lock local object store: %w", err)
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			f.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (l *Local) checkOpen() error {
	if l.closed || l.root == nil {
		return errors.New("local object store is closed")
	}
	return nil
}

func validateLocalKey(key string) error {
	if key == "" || strings.HasPrefix(key, "/") || path.Clean(key) != key || key == "." || key == ".." || strings.HasPrefix(key, "../") || strings.ContainsAny(key, "\\\x00\r\n") {
		return errors.New("invalid object key")
	}
	for _, part := range strings.Split(key, "/") {
		if part == "" || part == localLockName || part == localReadyName || strings.HasPrefix(part, localWritePrefix) {
			return errors.New("invalid object key")
		}
	}
	return nil
}

func validateLocalPrefix(prefix string) (string, error) {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix == "" {
		return "", nil
	}
	if strings.HasPrefix(prefix, "/") || path.Clean(prefix) != prefix || prefix == "." || prefix == ".." || strings.HasPrefix(prefix, "../") || strings.ContainsAny(prefix, "\\\x00\r\n") {
		return "", errors.New("invalid object prefix")
	}
	for _, part := range strings.Split(prefix, "/") {
		if part == "" || part == localLockName || part == localReadyName || strings.HasPrefix(part, localWritePrefix) {
			return "", errors.New("invalid object prefix")
		}
	}
	return prefix, nil
}

func readSeekable(body io.ReadSeeker, size int64) ([]byte, error) {
	position, err := body.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	defer body.Seek(position, io.SeekStart)
	if _, err = body.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != size {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}

func localETag(data []byte) string {
	sum := sha256.Sum256(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func randomTempName() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return localWritePrefix + hex.EncodeToString(value[:]), nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func fileOwner(info os.FileInfo) (int, int, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, errors.New("cannot determine local object directory owner")
	}
	return int(stat.Uid), int(stat.Gid), nil
}

func syncOSDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
