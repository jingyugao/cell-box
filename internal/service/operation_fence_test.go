package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestGetBoxDoesNotInspectRuntime(t *testing.T) {
	store := newTestStore(t)
	box := coreBox("get-projection")
	box.Box.Phase, box.Box.Generation = "running", 2
	box.ClientID = "client-a"
	if err := store.Update(func(st *State) error {
		st.Boxes[box.Box.ID] = box
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before, err := store.BoxRecord(box.Box.ID)
	if err != nil {
		t.Fatal(err)
	}
	projectionOnly := &Service{store: store, config: Config{ClientID: "client-a"}, ctx: context.Background(), streams: map[string]activeStream{}}
	request := httptest.NewRequest(http.MethodGet, "/v1/boxes/"+box.Box.ID, nil)
	request.Header.Set("Authorization", "Bearer "+testClientToken)
	response := httptest.NewRecorder()
	projectionOnly.Handler().ServeHTTP(response, request)
	wantStatus(t, response.Code, http.StatusOK, response.Body.Bytes())
	after, err := store.BoxRecord(box.Box.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Box, after.Box) || before.TransitionID != after.TransitionID || before.ObservedTransitionID != after.ObservedTransitionID {
		t.Fatalf("GET mutated the local projection: before=%+v after=%+v", before, after)
	}
}

func TestObsoleteLifecycleWorkerCannotOverwriteTerminalOperation(t *testing.T) {
	store := newTestStore(t)
	box := coreBox("stale-worker")
	box.ClientID = "owner"
	box.Box.OperationID = "op-worker"
	op := Operation{ID: "op-worker", Kind: "resume", TargetID: box.Box.ID, Status: "running", Version: 1}
	updateRecords(t, store, func(st *State) {
		st.Boxes[box.Box.ID] = box
		st.Operations[op.ID] = operationRecord{Operation: op, ClientID: "owner"}
	})
	service := &Service{ctx: context.Background(), store: store}
	entered, release := make(chan struct{}), make(chan struct{})
	service.launchWithCommitContext(context.Background(), op, func(context.Context) (map[string]string, error) {
		close(entered)
		<-release
		return map[string]string{"boxId": box.Box.ID}, nil
	}, nil)
	<-entered
	updateRecords(t, store, func(st *State) {
		record := st.Operations[op.ID]
		record.Operation.Status = "failed"
		record.Operation.Error = &APIError{Code: "UNKNOWN_OUTCOME", Message: "new lifecycle owns recovery"}
		st.Operations[op.ID] = record
		current := st.Boxes[box.Box.ID]
		current.Box.OperationID = "newer-operation"
		st.Boxes[box.Box.ID] = current
	})
	close(release)
	service.wg.Wait()
	record, err := store.Operation("owner", op.ID)
	if err != nil || record.Status != "failed" || record.Error == nil || record.Error.Code != "UNKNOWN_OUTCOME" {
		t.Fatalf("stale worker overwrote terminal operation: %+v, %v", record, err)
	}
	current, err := store.BoxRecord(box.Box.ID)
	if err != nil || current.Box.OperationID != "newer-operation" {
		t.Fatalf("stale worker cleared newer lifecycle ownership: %+v, %v", current.Box, err)
	}
}
