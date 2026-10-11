package objectstorage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// Metadata keeps control records on one node and leaves checkpoint/archive
// content in the existing object store. Local misses never fall back to stale
// remote metadata after the one-time import.
type Metadata struct {
	local  *Local
	remote Objects
}

func metadataKey(key string) bool {
	if strings.HasPrefix(key, "metadata/") {
		return true
	}
	parts := strings.Split(key, "/")
	return len(parts) == 3 && (parts[0] == "images" || parts[0] == "checkpoints") && parts[2] == "metadata.json"
}

// OpenMetadata opens an explicitly initialized node directory. initialize is a
// one-time operator action: ALL old API/controller writers must be stopped for
// a consistent import. Directory publication is atomic, so an interrupted copy
// can never be opened as a partial database. Imported ETags remain unchanged.
func OpenMetadata(ctx context.Context, dir string, remote Objects, initialize bool) (*Metadata, error) {
	if remote == nil || !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || dir == "/" {
		return nil, errors.New("local metadata requires an absolute dedicated directory and object storage for assets")
	}
	if initialize {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("local metadata directory is missing or invalid; explicit initialization is required")
	}
	objectsDir := filepath.Join(dir, "objects")
	if err := metadataReady(objectsDir); err == nil {
		return openMetadata(objectsDir, remote)
	} else if !initialize || !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("local metadata is not initialized: %w", err)
	}
	fd, err := unix.Open(filepath.Join(dir, ".initialize.lock"), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), ".initialize.lock")
	defer lock.Close()
	for {
		if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err == nil {
			break
		} else if !errors.Is(err, unix.EWOULDBLOCK) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer unix.Flock(fd, unix.LOCK_UN)
	if err := metadataReady(objectsDir); err == nil {
		return openMetadata(objectsDir, remote)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	// An existing objects directory without its marker is corruption, not a
	// request to overwrite it with an older remote copy.
	if _, err := os.Lstat(objectsDir); !errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("refusing to replace an incomplete local metadata directory")
	}
	if err := importMetadata(ctx, dir, objectsDir, remote); err != nil {
		return nil, err
	}
	return openMetadata(objectsDir, remote)
}

func openMetadata(dir string, remote Objects) (*Metadata, error) {
	local, err := NewLocal(dir)
	if err != nil {
		return nil, err
	}
	return &Metadata{local: local, remote: remote}, nil
}

func metadataReady(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("invalid local metadata objects directory")
	}
	marker := filepath.Join(dir, ".initialized")
	info, err = os.Lstat(marker)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("invalid local metadata initialization marker")
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		return err
	}
	var manifest struct {
		Version int `json:"version"`
	}
	if json.Unmarshal(data, &manifest) != nil || manifest.Version != 1 {
		return errors.New("unsupported local metadata initialization marker")
	}
	return nil
}

func importMetadata(ctx context.Context, dir, destination string, remote Objects) error {
	stage, err := os.MkdirTemp(dir, ".metadata-import-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	local, err := NewLocal(stage)
	if err != nil {
		return err
	}
	defer local.Close()
	keys, err := remoteMetadataKeys(ctx, remote)
	if err != nil {
		return err
	}
	versions := make(map[string]string, len(keys))
	for _, key := range keys {
		body, size, etag, err := remote.Get(ctx, key)
		if err != nil {
			return err
		}
		if size < 0 || size > 64<<20 || etag == "" {
			body.Close()
			return errors.New("invalid metadata object during import")
		}
		data, readErr := io.ReadAll(io.LimitReader(body, size+1))
		closeErr := body.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
		if int64(len(data)) != size {
			return io.ErrUnexpectedEOF
		}
		if err := local.Import(ctx, key, bytes.NewReader(data), size, etag); err != nil {
			return err
		}
		versions[key] = etag
	}
	// Detect writes during copying, including Box-local CAS operations that do
	// not update the global head. This is a guard, not a substitute for stopping
	// writers before migration.
	currentKeys, err := remoteMetadataKeys(ctx, remote)
	if err != nil {
		return err
	}
	if !slices.Equal(keys, currentKeys) {
		return errors.New("remote metadata changed during import; stop all writers before retrying")
	}
	for _, key := range keys {
		body, _, etag, err := remote.Get(ctx, key)
		if err != nil {
			return err
		}
		_, readErr := io.Copy(io.Discard, body)
		closeErr := body.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return err
		}
		if etag != versions[key] {
			return errors.New("remote metadata changed during import; stop all writers before retrying")
		}
	}
	marker, err := os.OpenFile(filepath.Join(stage, ".initialized"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(marker, "{\"version\":1,\"objects\":%d}\n", len(keys))
	if err == nil {
		err = marker.Sync()
	}
	err = errors.Join(err, marker.Close())
	if err != nil {
		return err
	}
	if err = syncMetadataDir(stage); err != nil {
		return err
	}
	if err = os.Rename(stage, destination); err != nil {
		return err
	}
	return syncMetadataDir(dir)
}

func remoteMetadataKeys(ctx context.Context, remote Objects) ([]string, error) {
	var keys []string
	for _, prefix := range []string{"metadata", "images", "checkpoints"} {
		listed, err := remote.List(ctx, prefix)
		if err != nil {
			return nil, err
		}
		for _, key := range listed {
			if !strings.HasPrefix(key, prefix+"/") {
				return nil, errors.New("metadata listing escaped requested prefix")
			}
			if metadataKey(key) {
				keys = append(keys, key)
			}
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys), nil
}

func syncMetadataDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func (m *Metadata) backend(key string) Objects {
	if metadataKey(key) {
		return m.local
	}
	return m.remote
}
func (m *Metadata) Get(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	return m.backend(key).Get(ctx, key)
}
func (m *Metadata) Put(ctx context.Context, key string, data io.ReadSeeker, size int64, condition string) (string, error) {
	return m.backend(key).Put(ctx, key, data, size, condition)
}
func (m *Metadata) Delete(ctx context.Context, key string) error {
	return m.backend(key).Delete(ctx, key)
}
func (m *Metadata) DeleteIfMatch(ctx context.Context, key, etag string) error {
	return m.backend(key).DeleteIfMatch(ctx, key, etag)
}
func (m *Metadata) List(ctx context.Context, prefix string) ([]string, error) {
	local, err := m.local.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if prefix == "metadata" || strings.HasPrefix(prefix, "metadata/") {
		return local, nil
	}
	remote, err := m.remote.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	for _, key := range remote {
		if !metadataKey(key) {
			local = append(local, key)
		}
	}
	slices.Sort(local)
	return slices.Compact(local), nil
}
func (m *Metadata) DeletePrefix(ctx context.Context, prefix string) error {
	if err := m.local.DeletePrefix(ctx, prefix); err != nil {
		return err
	}
	if prefix == "metadata" || strings.HasPrefix(prefix, "metadata/") {
		return nil
	}
	return m.remote.DeletePrefix(ctx, prefix)
}
func (m *Metadata) Close() error { return m.local.Close() }

// ListMetadata lets inventory readers list only local descriptors, without
// requesting remote blob listings or reviving descriptors left by migration.
func ListMetadata(ctx context.Context, objects Objects, prefix string) ([]string, error) {
	if local, ok := objects.(*Metadata); ok {
		return local.local.List(ctx, prefix)
	}
	return objects.List(ctx, prefix)
}

func MetadataBackend(objects Objects) string {
	if _, ok := objects.(*Metadata); ok {
		return "local"
	}
	return "object_storage"
}
