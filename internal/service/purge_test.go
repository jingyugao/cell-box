package service

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/objectstorage"
)

type purgeObjects struct {
	*ledgerObjects
	failDelete string
}

func (o *purgeObjects) DeleteIfMatch(ctx context.Context, key, etag string) error {
	if key == o.failDelete {
		return objectstorage.ErrUnavailable
	}
	return o.ledgerObjects.DeleteIfMatch(ctx, key, etag)
}
func purgeService(store *Store, objects objectstorage.Objects) *Service {
	return &Service{ctx: context.Background(), store: store, objects: objects, config: Config{ClientID: "owner"}, streams: map[string]activeStream{}}
}
func seedPurge(t *testing.T, store *Store) {
	t.Helper()
	updateRecords(t, store, func(st *State) {
		box := coreBox("box")
		box.Box.Phase = "deleted"
		st.Boxes["box"] = box
		st.Operations["create"] = operationRecord{ClientID: "owner", Operation: Operation{ID: "create", TargetID: "box", Kind: "create", Status: "succeeded"}}
		st.Operations["capture"] = operationRecord{ClientID: "owner", Operation: Operation{ID: "capture", TargetID: "box", Kind: "archive", Status: "succeeded", Result: map[string]string{"archiveId": "archive"}}}
		st.Executions["exec"] = executionRecord{Execution: Execution{ID: "exec", BoxID: "box", OperationID: "capture", State: "exited", Result: &guestapi.ExecResult{Stdout: "private-output"}}}
		st.Keys["retry-key"] = keyRecord{Hash: "private-input", OperationID: "create"}
		st.Leases["lease"] = Lease{ID: "lease", BoxID: "box"}
		st.Routes["route"] = Route{ID: "route", BoxID: "box"}
		st.Archives["archive"] = archiveRecord{ClientID: "owner", Archive: Archive{ID: "archive", SourceBoxID: "box", ImportedImageID: "image", PreparedImage: "prepared"}}
	})
}
func assertPurged(t *testing.T, store *Store) {
	t.Helper()
	if err := store.View(func(st State) error {
		if len(st.Boxes)+len(st.Operations)+len(st.Executions)+len(st.Leases)+len(st.Routes)+len(st.Purges) != 0 {
			t.Fatal("resource metadata remains")
		}
		if len(st.Archives) != 1 || st.Archives["archive"].Archive.ImportedImageID != "image" {
			t.Fatal("lost archive or image reference")
		}
		if !st.Keys["retry-key"].Expired || st.Keys["retry-key"].Hash != "" || st.Keys["retry-key"].OperationID != "" {
			t.Fatal("request history remained or replay fence disappeared")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
func TestPurgeRejectsOtherNamespacesAndLiveOrUnfinishedBoxes(t *testing.T) {
	objects := &ledgerObjects{}
	store := openRecords(t, objects)
	seedPurge(t, store)
	s := purgeService(store, objects)
	for _, tc := range []struct {
		client, phase, status string
		want                  int
	}{
		{"other", "deleted", "succeeded", 404}, {"owner", "running", "succeeded", 409}, {"owner", "deleting", "succeeded", 409}, {"owner", "deleted", "running", 409},
	} {
		updateRecords(t, store, func(st *State) {
			b := st.Boxes["box"]
			b.Box.Phase = tc.phase
			st.Boxes["box"] = b
			op := st.Operations["create"]
			op.Operation.Status = tc.status
			st.Operations["create"] = op
		})
		req := httptest.NewRequest("POST", "/v1/boxes/box:purge", nil)
		req.Header.Set("X-Cellbox-Client-ID", tc.client)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		if w.Code != tc.want {
			t.Fatalf("%+v: got %d: %s", tc, w.Code, w.Body.String())
		}
		if len(store.state.Boxes) != 1 || len(store.state.Executions) != 1 {
			t.Fatal("refused purge mutated data")
		}
	}
	req := httptest.NewRequest("POST", "/v1/boxes/missing:purge", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != 204 {
		t.Fatalf("missing purge: %d", w.Code)
	}
}
func TestPurgeJournalsFailedDeletionAndRecoversAfterRestart(t *testing.T) {
	objects := &purgeObjects{ledgerObjects: &ledgerObjects{}}
	store := openRecords(t, objects)
	seedPurge(t, store)
	result := store.state.Executions["exec"].ResultObject
	archiveKey := "archives/archive/content.tar.gz"
	imageKey := "images/image/rootfs.tar"
	objects.data[archiveKey] = []byte("archive-bytes")
	objects.data[imageKey] = []byte("image-bytes")
	archiveMetadata := bytes.Clone(objects.data[recordKey("archives", "archive")])
	objects.failDelete = result
	s := purgeService(store, objects)
	if err := s.purgeBox(context.Background(), "owner", "box"); err == nil {
		t.Fatal("failed object deletion returned success")
	}
	head, err := decodeRecordHead(objects.data[stateObjectKey])
	if err != nil || len(head.Retired) != 1 || head.Retired[0] != recordKey("boxes", "box") {
		t.Fatal("grouped operations added unnecessary permanent fences")
	}
	if len(store.state.Purges) != 1 || objects.data[result] == nil {
		t.Fatal("failed deletion lost its durable journal")
	}
	for _, key := range []string{recordKey("boxes", "box"), recordKey("operations", "create"), recordKey("operations", "capture"), recordKey("keys", "retry-key")} {
		if _, exists := objects.data[key]; exists {
			t.Fatalf("historical metadata remains: %s", key)
		}
	}
	objects.failDelete = ""
	again := openRecords(t, objects)
	s = purgeService(again, objects)
	s.gcMu.Lock()
	err = s.recoverPurges(context.Background())
	s.gcMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	assertPurged(t, again)
	if objects.data[result] != nil || !bytes.Equal(objects.data[archiveKey], []byte("archive-bytes")) || !bytes.Equal(archiveMetadata, objects.data[recordKey("archives", "archive")]) || objects.data[imageKey] == nil {
		t.Fatal("result cleanup or archive preservation failed")
	}
	for key, data := range objects.data {
		if strings.HasPrefix(key, "metadata/") && (bytes.Contains(data, []byte("private-output")) || bytes.Contains(data, []byte("private-input"))) {
			t.Fatalf("historical payload remains in %s", key)
		}
	}
	// A delayed former leader may create a retired object, but the shared fence
	// prevents loading it and startup removes it again.
	stale := resourceRecords{Revision: "stale", State: func() *State { st := newState(); st.Boxes["box"] = coreBox("box"); return &st }()}
	data, _ := json.Marshal(stale)
	objects.data[recordKey("boxes", "box")] = data
	restarted := openRecords(t, objects)
	assertPurged(t, restarted)
	if objects.data[recordKey("boxes", "box")] != nil {
		t.Fatal("startup did not remove retired residue")
	}
	if err := purgeService(restarted, objects).purgeBox(context.Background(), "owner", "box"); err != nil {
		t.Fatal(err)
	}
}
func TestLocalPurgePersistsWithoutDeletingArchives(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	seedPurge(t, store)
	archive := filepath.Join(dir, "archives", "archive.tar.gz")
	if err = os.MkdirAll(filepath.Dir(archive), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(archive, []byte("archive"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = purgeService(store, nil).purgeBox(context.Background(), "owner", "box"); err != nil {
		t.Fatal(err)
	}
	store.Close()
	store, err = OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	assertPurged(t, store)
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("private-output")) || bytes.Contains(data, []byte("private-input")) {
		t.Fatal("local history survived purge")
	}
	if data, err = os.ReadFile(archive); err != nil || string(data) != "archive" {
		t.Fatal("archive was removed")
	}
}

func TestPurgeRemovesExpiredOrphansAndPreservesOtherExecutionOutput(t *testing.T) {
	objects := &ledgerObjects{}
	store := openRecords(t, objects)
	seedPurge(t, store)
	updateRecords(t, store, func(st *State) {
		st.Boxes["neighbor"] = coreBox("neighbor")
		st.Executions["neighbor-exec"] = executionRecord{Execution: Execution{ID: "neighbor-exec", BoxID: "neighbor", State: "exited", Result: &guestapi.ExecResult{Stdout: "neighbor-output"}}}
	})
	neighbor := store.state.Executions["neighbor-exec"].ResultObject
	orphan := executionResultKey("old-expired-execution", []byte(`{"stdout":"expired-output"}`))
	objects.data[orphan] = []byte(`{"stdout":"expired-output"}`)
	if err := purgeService(store, objects).purgeBox(context.Background(), "owner", "box"); err != nil {
		t.Fatal(err)
	}
	if objects.data[orphan] != nil || objects.data[neighbor] == nil || len(store.state.Executions) != 1 {
		t.Fatal("expired output cleanup removed referenced neighboring output")
	}
}

func TestPurgeRecoversCommittedMetadataDeletionFailure(t *testing.T) {
	objects := &purgeObjects{ledgerObjects: &ledgerObjects{}}
	store := openRecords(t, objects)
	seedPurge(t, store)
	objects.failDelete = recordKey("boxes", "box")
	if err := purgeService(store, objects).purgeBox(context.Background(), "owner", "box"); err == nil {
		t.Fatal("metadata deletion failure returned success")
	}
	objects.failDelete = ""
	again := openRecords(t, objects)
	service := purgeService(again, objects)
	if err := service.purgeBox(context.Background(), "owner", "box"); err != nil {
		t.Fatal(err)
	}
	assertPurged(t, again)
	if objects.data[recordKey("boxes", "box")] != nil {
		t.Fatal("committed retirement did not replay")
	}
}

func TestPurgeRemovesDeletedBoxTombstoneLeftByRetention(t *testing.T) {
	objects := &ledgerObjects{}
	store := openRecords(t, objects)
	updateRecords(t, store, func(st *State) { b := coreBox("box"); b.Box.Phase = "deleted"; st.Boxes["box"] = b })
	updateRecords(t, store, func(st *State) { delete(st.Boxes, "box") })
	if objects.data[recordKey("boxes", "box")] == nil {
		t.Fatal("test did not create retained tombstone")
	}
	if err := purgeService(store, objects).purgeBox(context.Background(), "owner", "box"); err != nil {
		t.Fatal(err)
	}
	if objects.data[recordKey("boxes", "box")] != nil {
		t.Fatal("missing-box purge left retained tombstone")
	}
}

func TestRecordHeadMigratesOldSchemaBeforeUsingRetirement(t *testing.T) {
	objects := &ledgerObjects{}
	store := openRecords(t, objects)
	head, err := decodeRecordHead(objects.data[stateObjectKey])
	if err != nil {
		t.Fatal(err)
	}
	head.Schema = stateSchema
	data, err := json.Marshal(head)
	if err != nil {
		t.Fatal(err)
	}
	objects.data[stateObjectKey] = data
	store = openRecords(t, objects)
	head, err = decodeRecordHead(objects.data[stateObjectKey])
	if err != nil || head.Schema != recordHeadSchema {
		t.Fatal("old record head was not migrated")
	}
	seedPurge(t, store)
	if err = purgeService(store, objects).purgeBox(context.Background(), "owner", "box"); err != nil {
		t.Fatal(err)
	}
	head, err = decodeRecordHead(objects.data[stateObjectKey])
	if err != nil || len(head.Retired) == 0 || head.Schema == stateSchema {
		t.Fatal("retirement can be silently accepted by old binaries")
	}
}

func TestPurgeMissingBoxStillRemovesMatchingHistory(t *testing.T) {
	objects := &ledgerObjects{}
	store := openRecords(t, objects)
	seedPurge(t, store)
	updateRecords(t, store, func(st *State) { delete(st.Boxes, "box") })
	if err := purgeService(store, objects).purgeBox(context.Background(), "owner", "box"); err != nil {
		t.Fatal(err)
	}
	assertPurged(t, openRecords(t, objects))
	if objects.data[recordKey("boxes", "box")] != nil || objects.data[recordKey("operations", "create")] != nil || objects.data[recordKey("operations", "capture")] != nil {
		t.Fatal("missing box history remains")
	}
}

type pausedPurgeObjects struct {
	*ledgerObjects
	key     string
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (o *pausedPurgeObjects) DeleteIfMatch(ctx context.Context, key, etag string) error {
	if key == o.key {
		o.once.Do(func() { close(o.reached); <-o.release })
	}
	return o.ledgerObjects.DeleteIfMatch(ctx, key, etag)
}

func TestPurgeAndRetiredCleanupPermitConcurrentUnrelatedWriters(t *testing.T) {
	objects := &pausedPurgeObjects{ledgerObjects: &ledgerObjects{}, reached: make(chan struct{}), release: make(chan struct{})}
	store := openRecords(t, objects)
	seedPurge(t, store)
	updateRecords(t, store, func(st *State) { st.Boxes["neighbor"] = coreBox("neighbor") })
	objects.key = store.state.Executions["exec"].ResultObject
	service := purgeService(store, objects)
	purgeDone := make(chan error, 1)
	go func() { purgeDone <- service.purgeBox(context.Background(), "owner", "box") }()
	release := sync.OnceFunc(func() { close(objects.release) })
	defer release()
	select {
	case <-objects.reached:
	case <-time.After(5 * time.Second):
		t.Fatal("purge did not reach result cleanup")
	}
	writersDone := make(chan error, 1)
	cleanupDone := make(chan error, 1)
	go func() {
		for i := 0; i < 25; i++ {
			if err := store.Update(func(st *State) error {
				box := st.Boxes["neighbor"]
				box.Box.OwnerKey = strings.Repeat("x", i+1)
				st.Boxes["neighbor"] = box
				return nil
			}); err != nil {
				writersDone <- err
				return
			}
		}
		writersDone <- nil
	}()
	go func() {
		for i := 0; i < 25; i++ {
			if err := store.cleanupRetired(context.Background()); err != nil {
				cleanupDone <- err
				return
			}
		}
		cleanupDone <- nil
	}()
	for _, done := range []chan error{writersDone, cleanupDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("unrelated metadata writes or cleanup blocked behind result deletion")
		}
	}
	release()
	select {
	case err := <-purgeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("purge did not finish")
	}
	if err := store.View(func(st State) error {
		if len(st.Boxes) != 1 || st.Boxes["neighbor"].Box.OwnerKey != strings.Repeat("x", 25) || len(st.Purges) != 0 {
			t.Fatal("concurrent purge lost unrelated state")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPurgeRemovesOnlyOwnCaptureScratchAfterOperationsFinish(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	seedPurge(t, store)
	service := purgeService(store, nil)
	service.config.DataDir = dir
	archives := filepath.Join(dir, "archives")
	if err = os.MkdirAll(archives, 0700); err != nil {
		t.Fatal(err)
	}
	own := filepath.Join(archives, captureTempPrefix("box")+"partial")
	neighbor := filepath.Join(archives, captureTempPrefix("neighbor")+"active")
	valid := filepath.Join(archives, "arc-11111111111111111111111111111111.tar.gz")
	download := filepath.Join(archives, ".download-active")
	generic := filepath.Join(archives, ".capture-old-ownerless")
	for _, name := range []string{own, neighbor, valid, download, generic} {
		if err = os.WriteFile(name, []byte("content"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	updateRecords(t, store, func(st *State) {
		op := st.Operations["capture"]
		op.Operation.Status = "running"
		st.Operations["capture"] = op
	})
	if err = service.purgeBox(context.Background(), "owner", "box"); err == nil {
		t.Fatal("unfinished capture was not refused")
	}
	if _, err = os.Stat(own); err != nil {
		t.Fatal("unfinished operation lost scratch content")
	}
	updateRecords(t, store, func(st *State) {
		op := st.Operations["capture"]
		op.Operation.Status = "succeeded"
		st.Operations["capture"] = op
	})
	if err = service.purgeBox(context.Background(), "owner", "box"); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(own); !os.IsNotExist(err) {
		t.Fatal("owned capture scratch survived purge")
	}
	for _, name := range []string{neighbor, valid, download, generic} {
		if _, err = os.Stat(name); err != nil {
			t.Fatalf("purge removed shared or other-box file: %s", name)
		}
	}
	missing := filepath.Join(archives, captureTempPrefix("missing")+"partial")
	if err = os.WriteFile(missing, []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = service.purgeBox(context.Background(), "owner", "missing"); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("missing box's capture scratch survived purge")
	}
}

func TestExclusiveStartupCleansAbandonedTempsAndPreservesArchives(t *testing.T) {
	fixture := newCoreFixture(t)
	dir := fixture.config.DataDir
	archives := filepath.Join(dir, "archives")
	if err := os.MkdirAll(archives, 0700); err != nil {
		t.Fatal(err)
	}
	valid := filepath.Join(archives, "arc-11111111111111111111111111111111.tar.gz")
	payload := makeArchiveTestData(t, archiveTestEntry{name: "file", kind: 0, body: "content"})
	if err := os.WriteFile(valid, payload, 0600); err != nil {
		t.Fatal(err)
	}
	files := []string{filepath.Join(dir, ".state-abandoned"), filepath.Join(archives, ".capture-old-ownerless"), filepath.Join(archives, captureTempPrefix("box")+"abandoned"), filepath.Join(archives, ".download-abandoned")}
	for _, name := range files {
		if err := os.WriteFile(name, []byte("partial"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	symlink := filepath.Join(archives, ".capture-preserved-link")
	if err := os.Symlink(valid, symlink); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(archives, ".download-preserved-directory")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	fixture.reopen(t)
	for _, name := range files {
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatalf("abandoned temp remains: %s", name)
		}
	}
	if data, err := os.ReadFile(valid); err != nil || !bytes.Equal(data, payload) {
		t.Fatal("valid archive changed during startup cleanup")
	}
	for _, name := range []string{filepath.Join(dir, "state.json"), filepath.Join(dir, "service.lock"), symlink, directory} {
		if _, err := os.Lstat(name); err != nil {
			t.Fatalf("startup removed retained data: %s", name)
		}
	}
	if _, err := lockDataDirectory(dir); err == nil {
		t.Fatal("another process can own active archive scratch directory")
	}
}
