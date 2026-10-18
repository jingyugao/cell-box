package service

import (
	"bytes"
	"cellbox.local/cellbox/internal/objectstorage"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// ledgerObjects is an in-memory payload store used by archive tests. Runtime
// metadata and execution output are stored in SQLite and must not use this fake.
type ledgerObjects struct {
	mu          sync.Mutex
	data        map[string][]byte
	unavailable bool
}

func (o *ledgerObjects) Get(_ context.Context, key string) (io.ReadCloser, int64, string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.unavailable {
		return nil, 0, "", objectstorage.ErrUnavailable
	}
	data, ok := o.data[key]
	if !ok {
		return nil, 0, "", objectstorage.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), data...))), int64(len(data)), objectETag(data), nil
}

func (o *ledgerObjects) Put(_ context.Context, key string, body io.ReadSeeker, size int64, condition string) (string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.unavailable {
		return "", objectstorage.ErrUnavailable
	}
	current, exists := o.data[key]
	if condition == "*" && exists || condition != "" && condition != "*" && (!exists || condition != objectETag(current)) {
		return "", objectstorage.ErrConflict
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	if int64(len(data)) != size {
		return "", io.ErrUnexpectedEOF
	}
	if o.data == nil {
		o.data = map[string][]byte{}
	}
	o.data[key] = data
	return objectETag(data), nil
}

func (o *ledgerObjects) Delete(_ context.Context, key string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.unavailable {
		return objectstorage.ErrUnavailable
	}
	delete(o.data, key)
	return nil
}

func (o *ledgerObjects) DeletePrefix(context.Context, string) error { return errors.New("unused") }

func (o *ledgerObjects) List(_ context.Context, prefix string) ([]string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.unavailable {
		return nil, objectstorage.ErrUnavailable
	}
	var keys []string
	for key := range o.data {
		if strings.HasPrefix(key, prefix+"/") {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (o *ledgerObjects) DeleteIfMatch(_ context.Context, key, etag string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.unavailable {
		return objectstorage.ErrUnavailable
	}
	data, exists := o.data[key]
	if !exists {
		return nil
	}
	if etag != objectETag(data) {
		return objectstorage.ErrConflict
	}
	delete(o.data, key)
	return nil
}

func objectETag(data []byte) string { return fmt.Sprintf("\"%x\"", sha256.Sum256(data)) }
