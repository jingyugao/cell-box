package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"

	"cellbox.local/cellbox/internal/objectstorage"
)

type ledgerObjects struct {
	mu           sync.Mutex
	data         map[string][]byte
	revision     int
	lostResponse bool
	unavailable  bool
	failKey      string
	failWrites   int
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

func TestObjectStoreRestartFencesStaleWriterAndResolvesLostResponse(t *testing.T) {
	objects := &ledgerObjects{}
	store, err := OpenObjectStore(context.Background(), objects)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Update(func(st *State) error { st.Routes["route"] = Route{ID: "route", BoxID: "box", Port: 8080}; return nil }); err != nil {
		t.Fatal(err)
	}
	stale, err := OpenObjectStore(context.Background(), objects)
	if err != nil {
		t.Fatal(err)
	}
	objects.lostResponse = true
	if err = store.Update(func(st *State) error { st.Leases["lease"] = Lease{ID: "lease", BoxID: "box"}; return nil }); err != nil {
		t.Fatalf("committed update with lost response failed: %v", err)
	}
	if err = stale.Update(func(st *State) error { delete(st.Routes, "route"); return nil }); err == nil {
		t.Fatal("stale writer overwrote ledger")
	}
	if err = stale.View(func(State) error { return nil }); err == nil {
		t.Fatal("fenced writer still serves old state")
	}
	reopened, err := OpenObjectStore(context.Background(), objects)
	if err != nil {
		t.Fatal(err)
	}
	if err = reopened.View(func(st State) error {
		if len(st.Routes) != 1 || len(st.Leases) != 1 {
			t.Fatalf("restart lost state: %+v", st)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
func TestObjectStoreFailureCancelsServiceAndRejectsStaleReads(t *testing.T) {
	objects := &ledgerObjects{}
	store, err := OpenObjectStore(context.Background(), objects)
	if err != nil {
		t.Fatal(err)
	}
	canceled := false
	store.onFailure = func() { canceled = true }
	objects.unavailable = true
	if err = store.Update(func(st *State) error { return nil }); err == nil || !canceled {
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

func TestImageMetadataObjectsIsolationAndInterruptedTransactionRecovery(t *testing.T) {
	ctx := context.Background()
	objects := &ledgerObjects{}
	store, err := OpenObjectStore(ctx, objects)
	if err != nil {
		t.Fatal(err)
	}
	first := "img-11111111111111111111111111111111"
	second := "img-22222222222222222222222222222222"
	if err = store.Update(func(st *State) error {
		for _, id := range []string{first, second} {
			st.ImportedImages[id] = importedImageRecord{ClientID: "owner", ImportedImage: ImportedImage{ID: id, Source: "original"}}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	firstKey, _ := imageMetadataKey(first)
	secondKey, _ := imageMetadataKey(second)
	originalSecond := append([]byte(nil), objects.data[secondKey]...)
	if bytes.Contains(objects.data[stateObjectKey], []byte("importedImages")) {
		t.Fatal("aggregate ledger still contains image metadata")
	}
	// Stop after the independent image object commits but before the ledger.
	objects.failKey = stateObjectKey
	objects.failWrites = 1
	if err = store.Update(func(st *State) error {
		image := st.ImportedImages[first]
		image.ImportedImage.Source = "updated"
		st.ImportedImages[first] = image
		st.Operations["op"] = operationRecord{ClientID: "owner", Operation: Operation{ID: "op", Status: "succeeded"}}
		return nil
	}); err == nil {
		t.Fatal("expected interrupted ledger write")
	}
	if !bytes.Equal(originalSecond, objects.data[secondKey]) {
		t.Fatal("updating one image rewrote another image")
	}
	if _, exists := objects.data[pendingObjectKey]; !exists {
		t.Fatal("partial transaction lost recovery intent")
	}
	reopened, err := OpenObjectStore(ctx, objects)
	if err != nil {
		t.Fatal(err)
	}
	if err = reopened.View(func(st State) error {
		if len(st.ImportedImages) != 2 || st.ImportedImages[first].ImportedImage.Source != "updated" || st.Operations["op"].Operation.Status != "succeeded" {
			t.Fatal("image/operation transaction did not recover")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, exists := objects.data[pendingObjectKey]; exists {
		t.Fatal("completed recovery intent remains")
	}
	if err = reopened.Update(func(st *State) error { delete(st.ImportedImages, first); return nil }); err != nil {
		t.Fatal(err)
	}
	if _, exists := objects.data[firstKey]; exists {
		t.Fatal("deleted image metadata remains")
	}
	again, err := OpenObjectStore(ctx, objects)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.state.ImportedImages) != 1 || again.state.ImportedImages[second].ImportedImage.ID != second {
		t.Fatal("restart image listing lost neighbor or resurrected deleted image")
	}
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

func TestSupersededIntentDoesNotOverwriteImageOrBlockRestart(t *testing.T) {
	ctx := context.Background()
	objects := &ledgerObjects{}
	store, err := OpenObjectStore(ctx, objects)
	if err != nil {
		t.Fatal(err)
	}
	id := "img-33333333333333333333333333333333"
	if err = store.Update(func(st *State) error {
		st.ImportedImages[id] = importedImageRecord{ClientID: "owner", ImportedImage: ImportedImage{ID: id, Source: "correct"}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := store.etag
	stale, err := cloneState(store.state)
	if err != nil {
		t.Fatal(err)
	}
	stale.Revision = "superseded"
	stale.ImportedImages = nil
	ledger, err := json.Marshal(stale)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := imageMetadataKey(id)
	record := importedImageRecord{ClientID: "owner", ImportedImage: ImportedImage{ID: id, Source: "wrong"}}
	imageData, err := json.Marshal(storedImage{Revision: stale.Revision, Record: &record})
	if err != nil {
		t.Fatal(err)
	}
	tx := objectTransaction{Revision: stale.Revision, BeforeETag: before, State: ledger, Images: []imageChange{{Key: key, BeforeETag: store.imageEtags[id], Data: imageData}}}
	if err = store.Update(func(st *State) error { st.Routes["current"] = Route{ID: "current", BoxID: "box"}; return nil }); err != nil {
		t.Fatal(err)
	}
	// A delayed stale process may acquire the intent slot after the next owner
	// commits. Recovery must discard it without applying its image mutation.
	intent, err := json.Marshal(tx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = objects.Put(ctx, pendingObjectKey, bytes.NewReader(intent), int64(len(intent)), "*"); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenObjectStore(ctx, objects)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.state.ImportedImages[id].ImportedImage.Source != "correct" || restarted.state.Routes["current"].ID != "current" {
		t.Fatal("superseded transaction overwrote committed data")
	}
	if _, exists := objects.data[pendingObjectKey]; exists {
		t.Fatal("superseded transaction blocked future writes")
	}
}

type replacingImageObjects struct {
	*ledgerObjects
	key         string
	replacement []byte
}

func (o *replacingImageObjects) DeleteIfMatch(ctx context.Context, key, etag string) error {
	if key == o.key && o.replacement != nil {
		replacement := o.replacement
		o.replacement = nil
		if _, err := o.ledgerObjects.Put(ctx, key, bytes.NewReader(replacement), int64(len(replacement)), ""); err != nil {
			return err
		}
	}
	return o.ledgerObjects.DeleteIfMatch(ctx, key, etag)
}
func TestImageDeleteCannotRemoveNewerMetadataAfterLeaderTakeover(t *testing.T) {
	ctx := context.Background()
	objects := &replacingImageObjects{ledgerObjects: &ledgerObjects{}}
	store, err := OpenObjectStore(ctx, objects)
	if err != nil {
		t.Fatal(err)
	}
	id := "img-44444444444444444444444444444444"
	if err = store.Update(func(st *State) error {
		st.ImportedImages[id] = importedImageRecord{ClientID: "owner", ImportedImage: ImportedImage{ID: id, Source: "old"}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	objects.key, _ = imageMetadataKey(id)
	newer := storedImage{Revision: "new-owner", Record: &importedImageRecord{ClientID: "owner", ImportedImage: ImportedImage{ID: id, Source: "new"}}}
	objects.replacement, err = json.Marshal(newer)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Update(func(st *State) error { delete(st.ImportedImages, id); return nil }); err == nil {
		t.Fatal("stale delete was not fenced")
	}
	actual, _, err := readObject(ctx, objects, objects.key)
	if err != nil {
		t.Fatal("new metadata deleted by stale process:", err)
	}
	var image storedImage
	if err = json.Unmarshal(actual, &image); err != nil || image.Record.ImportedImage.Source != "new" {
		t.Fatal("new metadata changed")
	}
}
