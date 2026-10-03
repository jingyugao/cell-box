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
	"testing"
)

type ledgerObjects struct {
	mu           sync.Mutex
	data         map[string][]byte
	revision     int
	stateReads   int
	lostResponse bool
	unavailable  bool
	failKey      string
	failWrites   int
}

func (o *ledgerObjects) Get(_ context.Context, key string) (io.ReadCloser, int64, string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if key == stateObjectKey {
		o.stateReads++
	}
	if o.unavailable {
		return nil, 0, "", objectstorage.ErrUnavailable
	}
	data, ok := o.data[key]
	if !ok {
		return nil, 0, "", objectstorage.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), data...))), int64(len(data)), fmt.Sprintf("\"%x\"", sha256.Sum256(o.data[key])), nil
}
func (o *ledgerObjects) Put(_ context.Context, key string, body io.ReadSeeker, size int64, condition string) (string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.unavailable {
		return "", objectstorage.ErrUnavailable
	}
	if key == o.failKey && o.failWrites > 0 {
		o.failWrites--
		return "", objectstorage.ErrUnavailable
	}
	_, exists := o.data[key]
	if condition == "*" && exists || condition != "" && condition != "*" && condition != fmt.Sprintf("\"%x\"", sha256.Sum256(o.data[key])) {
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
	o.revision++
	if o.lostResponse && key == stateObjectKey {
		o.lostResponse = false
		return "", objectstorage.ErrUnavailable
	}
	return fmt.Sprintf("\"%x\"", sha256.Sum256(o.data[key])), nil
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

func TestObjectStoreFailureCancelsServiceAndRejectsStaleReads(t *testing.T) {
	objects := &ledgerObjects{}
	store, err := OpenObjectStore(context.Background(), objects)
	if err != nil {
		t.Fatal(err)
	}
	canceled := false
	store.onFailure = func() { canceled = true }
	objects.unavailable = true
	if err = store.Update(func(st *State) error { st.Keys["write"] = keyRecord{Expired: true}; return nil }); err == nil || !canceled {
		t.Fatal("failed durable write did not stop service")
	}
	if _, err = store.BoxRecords("owner", nil); err == nil {
		t.Fatal("stale reads accepted after write failure")
	}
}

func TestArchiveRemoteDeleteFailureKeepsRetryableDeletionFence(t *testing.T) {
	f := newCoreFixture(t)
	objects := &ledgerObjects{unavailable: true}
	f.service.objects = objects
	id := "arc-11111111111111111111111111111111"
	if err := f.service.store.Update(func(st *State) error {
		st.Archives[id] = archiveRecord{ClientID: "client-a", Archive: Archive{ID: id}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	status, body := f.call(t, "DELETE", "/v1/archives/"+id, testClientToken, "", nil)
	wantStatus(t, status, 502, body)
	var retained bool
	if err := f.service.store.View(func(st State) error { retained = st.Archives[id].Deleting; return nil }); err != nil {
		t.Fatal(err)
	}
	if !retained {
		t.Fatal("remote delete failure lost retry state")
	}
	if _, err := f.service.archiveForClient("client-a", id); err == nil {
		t.Fatal("deleting archive accepted for restore/download")
	}
	objects.unavailable = false
	status, body = f.call(t, "DELETE", "/v1/archives/"+id, testClientToken, "", nil)
	wantStatus(t, status, 204, body)
	if err := f.service.store.View(func(st State) error {
		if _, exists := st.Archives[id]; exists {
			t.Fatal("archive metadata remains after successful retry")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

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
	if etag != fmt.Sprintf("\"%x\"", sha256.Sum256(data)) {
		return objectstorage.ErrConflict
	}
	delete(o.data, key)
	return nil
}
