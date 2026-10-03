package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/objectstorage"
)

func openRecords(t *testing.T, objects objectstorage.Objects) *Store {
	t.Helper()
	s, err := OpenObjectStore(context.Background(), objects)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func updateRecords(t *testing.T, s *Store, fn func(*State)) {
	t.Helper()
	if err := s.Update(func(st *State) error { fn(st); return nil }); err != nil {
		t.Fatal(err)
	}
}

func coreBox(id string) boxRecord {
	return boxRecord{ClientID: "owner", Box: Box{ID: id, Phase: "running", ProfileID: "default", Generation: 7},
		Profile: Profile{ID: "default", Provider: "resumable-k8s-pod", Image: "prepared@sha256:abc", Guest: guestapi.DefaultConfig()},
		Handle:  boxprovider.Handle{ID: "cr-" + id, Provider: "resumable-k8s-pod"}, ExecutionID: "pod-" + id}
}

func TestStoresRejectOldMetadataWithoutRewritingIt(t *testing.T) {
	old := []byte(`{"schema":2,"boxes":{}}`)
	for _, key := range []string{stateObjectKey, "metadata/pending.json", "images/img-11111111111111111111111111111111/metadata.json"} {
		objects := &ledgerObjects{data: map[string][]byte{key: bytes.Clone(old)}}
		if _, err := OpenObjectStore(context.Background(), objects); err == nil {
			t.Fatalf("accepted old or orphaned metadata: %s", key)
		}
		if objects.revision != 0 || !bytes.Equal(objects.data[key], old) || len(objects.data) != 1 {
			t.Fatal("opening unsupported metadata changed stored data")
		}
	}
	for _, data := range [][]byte{old, []byte(`{}`)} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "state.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		if store, err := OpenStore(dir); err == nil {
			store.Close()
			t.Fatal("accepted an old or unversioned local state")
		}
	}
}

func TestCoreStoreRestartsWithRecoveryDataWithoutRoutes(t *testing.T) {
	ctx := context.Background()
	objects := &ledgerObjects{}
	store := openRecords(t, objects)
	updateRecords(t, store, func(st *State) {
		b := coreBox("source")
		b.Box.ImportedImageID = "img-11111111111111111111111111111111"
		b.Staged, b.RestoreComplete, b.RestoreArchiveID = true, false, "archive-original"
		st.Boxes[b.Box.ID] = b
		st.Operations["op"] = operationRecord{ClientID: "owner", Operation: Operation{ID: "op", TargetID: b.Box.ID, Status: "running"}}
		st.Keys["retry"] = keyRecord{OperationID: "op", Hash: "input"}
		st.Leases["lease"] = Lease{ID: "lease", BoxID: b.Box.ID, ExpiresAt: time.Now().Add(time.Hour)}
		st.Executions["exec"] = executionRecord{Execution: Execution{ID: "exec", BoxID: b.Box.ID, OperationID: "op", State: "exited", Result: &guestapi.ExecResult{Stdout: "secret-output"}}}
		st.Archives["archive"] = archiveRecord{ClientID: "owner", Archive: Archive{ID: "archive", SourceBoxID: b.Box.ID, ManifestVersion: 1, ImportedImageID: b.Box.ImportedImageID, PreparedImage: b.Profile.Image}}
		st.Routes["route"] = Route{ID: "route", BoxID: b.Box.ID}
	})
	openRecords(t, objects)
	again := openRecords(t, objects)
	st := again.state
	b := st.Boxes["source"]
	if b.ClientID != "owner" || b.Handle.ID != "cr-source" || !b.Staged || b.RestoreComplete || b.RestoreArchiveID != "archive-original" || b.Profile.Image == "" {
		t.Fatalf("lost recovery identity/barrier: %+v", b)
	}
	if b.Box.Phase != "unknown" || b.Box.Generation != 0 || b.ExecutionID != "" || len(st.Routes) != 0 || len(st.Leases) != 1 || st.Keys["retry"].OperationID != "op" {
		t.Fatal("durable and derived state were not separated")
	}
	a := st.Archives["archive"].Archive
	if a.ManifestVersion != 1 || a.ImportedImageID != b.Box.ImportedImageID || a.PreparedImage != b.Profile.Image {
		t.Fatal("archive still depends on its source box")
	}
	e := st.Executions["exec"]
	if e.Result != nil || e.ResultObject == "" {
		t.Fatal("output was not moved out of core state")
	}
	result, err := again.executionResult(ctx, e)
	if err != nil || result.Result == nil || result.Result.Stdout != "secret-output" {
		t.Fatalf("output did not survive restart: %v", err)
	}
	if bytes.Contains(objects.data[stateObjectKey], []byte("secret-output")) {
		t.Fatal("commit head contains non-core payload")
	}
}

func TestCoreStoreWritesOnlyChangedResourceAndKeepsObservationsInMemory(t *testing.T) {
	objects := &ledgerObjects{}
	s := openRecords(t, objects)
	updateRecords(t, s, func(st *State) {
		st.Boxes["a"], st.Boxes["b"] = coreBox("a"), coreBox("b")
		st.Executions["exec"] = executionRecord{Execution: Execution{ID: "exec", BoxID: "b", Result: &guestapi.ExecResult{Stdout: strings.Repeat("large-history", 100000)}}}
	})
	neighbor := bytes.Clone(objects.data[recordKey("boxes", "b")])
	before := objects.revision
	updateRecords(t, s, func(st *State) {
		b := st.Boxes["a"]
		b.Box.OperationID = "new-op"
		st.Boxes["a"] = b
	})
	head, err := decodeRecordHead(objects.data[stateObjectKey])
	if err != nil || len(head.Changes) != 1 || head.Changes[0].Key != recordKey("boxes", "a") || objects.revision-before != 2 || len(objects.data[stateObjectKey]) > 10000 {
		t.Fatalf("update is not resource-sized: writes=%d, head=%d, err=%v", objects.revision-before, len(objects.data[stateObjectKey]), err)
	}
	if !bytes.Equal(neighbor, objects.data[recordKey("boxes", "b")]) {
		t.Fatal("unrelated box was rewritten")
	}
	before = objects.revision
	updateRecords(t, s, func(st *State) { st.Routes["route"] = Route{ID: "route", BoxID: "a"} })
	base, _ := s.BoxRecord("a")
	if _, err = s.UpdateObservedBox(base, func(b *boxRecord) (bool, error) {
		b.Box.Generation, b.ExecutionID, b.Box.Phase = 8, "next-pod", "running"
		b.Box.Version++
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if objects.revision != before {
		t.Fatal("route or Kubernetes observation wrote to OSS")
	}
}

func TestCoreStoreRecoversPartialCommitAndLostResponseAndFencesOldWriter(t *testing.T) {
	objects := &ledgerObjects{}
	s := openRecords(t, objects)
	updateRecords(t, s, func(st *State) { st.Boxes["a"] = coreBox("a") })
	stale := s
	s = openRecords(t, objects)
	objects.lostResponse = true
	updateRecords(t, s, func(st *State) { st.Leases["lease"] = Lease{ID: "lease", BoxID: "a"} })
	if err := stale.Update(func(st *State) error { st.Boxes["stale"] = coreBox("stale"); return nil }); err == nil {
		t.Fatal("stale writer committed a disjoint resource")
	}
	if err := stale.View(func(State) error { return nil }); err == nil {
		t.Fatal("failed writer continued serving stale data")
	}
	objects.failKey, objects.failWrites = recordKey("archives", "arc"), 1
	if err := s.Update(func(st *State) error {
		st.Archives["arc"] = archiveRecord{Archive: Archive{ID: "arc"}}
		delete(st.Leases, "lease")
		return nil
	}); err == nil {
		t.Fatal("partial materialization was reported as complete")
	}
	again := openRecords(t, objects)
	if len(again.state.Archives) != 1 || len(again.state.Leases) != 0 || len(again.state.Boxes) != 1 {
		t.Fatal("restart did not recover the complete transaction")
	}
	updateRecords(t, again, func(st *State) { delete(st.Archives, "arc") })
	if len(openRecords(t, objects).state.Archives) != 0 {
		t.Fatal("restart resurrected a deleted resource")
	}
}

func TestImageRecordsRecoverTogetherWithOperationsAndKeepDeletionFences(t *testing.T) {
	objects := &ledgerObjects{}
	store := openRecords(t, objects)
	first, second := "img-11111111111111111111111111111111", "img-22222222222222222222222222222222"
	updateRecords(t, store, func(st *State) {
		for _, id := range []string{first, second} {
			st.ImportedImages[id] = importedImageRecord{ClientID: "owner", ImportedImage: ImportedImage{ID: id, Source: "original"}}
		}
	})
	firstKey, _ := imageMetadataKey(first)
	secondKey, _ := imageMetadataKey(second)
	neighbor := bytes.Clone(objects.data[secondKey])
	// Commit the new head, then fail its image materialization.
	objects.failKey, objects.failWrites = firstKey, 1
	if err := store.Update(func(st *State) error {
		image := st.ImportedImages[first]
		image.ImportedImage.Source = "updated"
		st.ImportedImages[first] = image
		st.Operations["op"] = operationRecord{ClientID: "owner", Operation: Operation{ID: "op", Status: "succeeded"}}
		return nil
	}); err == nil {
		t.Fatal("partial image transaction reported success")
	}
	store = openRecords(t, objects)
	if len(store.state.ImportedImages) != 2 || store.state.ImportedImages[first].ImportedImage.Source != "updated" || store.state.Operations["op"].Operation.Status != "succeeded" {
		t.Fatal("image and operation did not recover together")
	}
	if !bytes.Equal(neighbor, objects.data[secondKey]) {
		t.Fatal("updating one image rewrote its neighbor")
	}
	updateRecords(t, store, func(st *State) { delete(st.ImportedImages, first) })
	store = openRecords(t, objects)
	if len(store.state.ImportedImages) != 1 || store.state.ImportedImages[second].ImportedImage.ID != second || store.records.etags[firstKey] == "" {
		t.Fatal("restart lost the image deletion fence or its neighbor")
	}
}

func TestCoreTombstonePreventsDelayedCreateFromResurrectingDeletedResource(t *testing.T) {
	objects := &ledgerObjects{}
	s := openRecords(t, objects)
	updateRecords(t, s, func(st *State) { st.Archives["arc"] = archiveRecord{Archive: Archive{ID: "arc"}} })
	old, err := decodeRecordHead(objects.data[stateObjectKey])
	if err != nil {
		t.Fatal(err)
	}
	updateRecords(t, s, func(st *State) { delete(st.Archives, "arc") })
	for _, replay := range []bool{false, true} {
		if _, err := materializeRecords(context.Background(), objects, old.Changes, replay); err == nil {
			t.Fatal("delayed create passed the deletion fence")
		}
	}
	again := openRecords(t, objects)
	if len(again.state.Archives) != 0 {
		t.Fatal("deleted archive was resurrected")
	}
	// Reuse of an internal record key is still possible through a new committed
	// transaction, conditional on the tombstone's version.
	updateRecords(t, again, func(st *State) { st.Archives["arc"] = archiveRecord{Archive: Archive{ID: "arc", ManifestVersion: 1}} })
}

type blockedHeadObjects struct {
	*ledgerObjects
	entered chan struct{}
	release chan struct{}
}

func (o *blockedHeadObjects) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, condition string) (string, error) {
	if key == stateObjectKey && o.entered != nil {
		close(o.entered)
		<-o.release
	}
	return o.ledgerObjects.Put(ctx, key, body, size, condition)
}

func TestCoreStoreReadersDoNotWaitForOSSCommit(t *testing.T) {
	objects := &blockedHeadObjects{ledgerObjects: &ledgerObjects{}}
	s := openRecords(t, objects)
	updateRecords(t, s, func(st *State) { st.Boxes["a"] = coreBox("a") })
	objects.entered, objects.release = make(chan struct{}), make(chan struct{})
	defer close(objects.release)
	done := make(chan error, 1)
	go func() {
		done <- s.Update(func(st *State) error {
			b := st.Boxes["a"]
			b.Box.OperationID = "new"
			st.Boxes["a"] = b
			return nil
		})
	}()
	<-objects.entered
	read := make(chan boxRecord, 1)
	go func() { b, _ := s.BoxRecord("a"); read <- b }()
	select {
	case b := <-read:
		if b.Box.OperationID != "" {
			t.Fatal("uncommitted state was published")
		}
	case <-time.After(time.Second):
		t.Fatal("OSS blocked a memory read")
	}
	// A send releases this writer; deferred close also releases it on failure.
	objects.release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCoreRetentionPreservesReplayFencesAndUnknownExecutions(t *testing.T) {
	objects := &ledgerObjects{}
	s := openRecords(t, objects)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)
	updateRecords(t, s, func(st *State) {
		b := coreBox("deleted")
		b.Box.Phase = "deleted"
		st.Boxes["deleted"] = b
		for _, id := range []string{"complete", "unknown", "running"} {
			st.Operations[id] = operationRecord{ClientID: "owner", Operation: Operation{ID: id, TargetID: "deleted", Status: "succeeded", FinishedAt: &old}}
			st.Keys["owner:exec:deleted:"+id] = keyRecord{OperationID: id, Hash: inputHash(nil)}
			st.Executions[id] = executionRecord{Execution: Execution{ID: id, BoxID: "deleted", OperationID: id, State: id}}
		}
		e := st.Executions["complete"]
		e.State, e.Result = "exited", &guestapi.ExecResult{Stdout: "old output"}
		st.Executions["complete"] = e
		st.Leases["active"] = Lease{ID: "active", BoxID: "deleted", ExpiresAt: now.Add(time.Hour)}
		st.Leases["expired"] = Lease{ID: "expired", BoxID: "deleted", ExpiresAt: old}
		st.Archives["arc"] = archiveRecord{Archive: Archive{ID: "arc", SourceBoxID: "deleted", ManifestVersion: 1, PreparedImage: b.Profile.Image}}
	})
	service := &Service{ctx: context.Background(), store: s, objects: objects, config: Config{OperationRetentionSeconds: 86400, ExecutionRetentionSeconds: 3600}}
	if err := service.pruneMetadata(now); err != nil {
		t.Fatal(err)
	}
	if len(s.state.Operations) != 2 || len(s.state.Executions) != 2 || len(s.state.Leases) != 1 || s.state.Keys["owner:exec:deleted:complete"].Expired != true || s.state.Archives["arc"].Archive.ManifestVersion != 1 {
		t.Fatal("pruning discarded a core fence or retained old details")
	}
	if keys, _ := objects.List(context.Background(), resultPrefix); len(keys) != 0 {
		t.Fatal("expired output not removed")
	}
	service.store = openRecords(t, objects)
	called := false
	_, fresh, err := service.prepareOperation("owner", "complete", "exec", "deleted", nil, func(*State, *Operation) error { called = true; return nil })
	var apiErr *APIError
	if fresh || called || !errors.As(err, &apiErr) || apiErr.Code != "OPERATION_EXPIRED" {
		t.Fatalf("expired retry could be executed: %v", err)
	}
	if _, err := s.executionResult(context.Background(), executionRecord{Execution: Execution{State: "expired"}}); !errors.As(err, &apiErr) || apiErr.Code != "RESULT_EXPIRED" {
		t.Fatal("expired output did not return an explicit error")
	}
}

func TestCoreOutputGCFindsOrphansAfterRestartAndProtectsLiveResults(t *testing.T) {
	objects := &ledgerObjects{}
	s := openRecords(t, objects)
	updateRecords(t, s, func(st *State) {
		st.Boxes["a"] = coreBox("a")
		st.Executions["live"] = executionRecord{Execution: Execution{ID: "live", BoxID: "a", Result: &guestapi.ExecResult{Stdout: "live"}}}
	})
	orphan, _ := json.Marshal(guestapi.ExecResult{Stdout: "never committed"})
	key := executionResultKey("orphan", orphan)
	if _, err := writeObject(context.Background(), objects, key, orphan, ""); err != nil {
		t.Fatal(err)
	}
	service := &Service{ctx: context.Background(), store: openRecords(t, objects), objects: objects, config: Config{OperationRetentionSeconds: 86400, ExecutionRetentionSeconds: 3600}}
	now := time.Now()
	if err := service.pruneMetadata(now); err != nil {
		t.Fatal(err)
	}
	if _, exists := objects.data[key]; !exists {
		t.Fatal("GC did not allow for an in-flight transaction")
	}
	if err := service.pruneMetadata(now.Add(3 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, exists := objects.data[key]; exists {
		t.Fatal("orphan survived the next sweep")
	}
	e, err := service.store.executionResult(context.Background(), service.store.state.Executions["live"])
	if err != nil || e.Result == nil || e.Result.Stdout != "live" {
		t.Fatal("GC removed a live result")
	}
}

func TestCoreRESTCreateExecRestartAndExpireOutput(t *testing.T) {
	f := newCoreFixture(t)
	objects := &ledgerObjects{}
	reopen := func() {
		// Reuse the fixture's fake runtime and HTTP clients, with a fresh OSS
		// Store and process context. Close waits for all previous workers.
		if err := f.service.Close(); err != nil {
			t.Fatal(err)
		}
		f.service.ctx, f.service.cancel = context.WithCancel(context.Background())
		f.service.store = openRecords(t, objects)
		f.service.store.onFailure = f.service.cancel
		f.service.objects = objects
	}
	reopen()
	_, box := f.createBox(t, "create")
	input := map[string]any{"argv": []string{"echo", "ok"}, "expectedGeneration": box.Generation}
	status, body := f.call(t, "POST", "/v1/boxes/"+box.ID+"/execs", testClientToken, "once", input)
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	f.waitOperation(t, op.ID, "succeeded")
	finished, err := f.service.operation("client-a", op.ID)
	if err != nil {
		t.Fatal(err)
	}
	execPath := "/v1/execs/" + finished.Result["execId"]
	reopen()
	status, body = f.call(t, "GET", execPath, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	result := decodeResponse[Execution](t, body)
	if result.Result == nil || result.Result.Stdout != "ok" || bytes.Contains(body, []byte("resultObject")) {
		t.Fatalf("incorrect public result after restart: %s", body)
	}
	status, body = f.call(t, "GET", execPath, otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "POST", "/v1/boxes/"+box.ID+"/execs", testClientToken, "once", input)
	wantStatus(t, status, 202, body)
	if decodeResponse[Operation](t, body).ID != op.ID || f.execCalls.Load() != 1 {
		t.Fatal("restart replayed the command")
	}
	if err := f.service.pruneMetadata(finished.FinishedAt.Add(2 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	status, body = f.call(t, "GET", execPath, testClientToken, "", nil)
	wantStatus(t, status, 410, body)
	if !bytes.Contains(body, []byte("RESULT_EXPIRED")) {
		t.Fatal("expired output did not explain expiry")
	}
	if err := f.service.pruneMetadata(finished.FinishedAt.Add(48 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	reopen()
	status, body = f.call(t, "POST", "/v1/boxes/"+box.ID+"/execs", testClientToken, "once", input)
	wantStatus(t, status, 410, body)
	if !bytes.Contains(body, []byte("OPERATION_EXPIRED")) || f.execCalls.Load() != 1 {
		t.Fatal("expired request could be replayed")
	}
}
