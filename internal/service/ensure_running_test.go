package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countingInspectProvider struct {
	*fakeCoreProvider
	inspects atomic.Int32
}

func (p *countingInspectProvider) Inspect(ctx context.Context, h boxprovider.Handle) (boxprovider.Observation, error) {
	p.inspects.Add(1)
	return p.fakeCoreProvider.Inspect(ctx, h)
}

func TestEnsureRunningRunningSnapshotIgnoresActiveWork(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	p := &countingInspectProvider{fakeCoreProvider: f.provider}
	p.created = true
	f.service.providers["docker"] = p
	box := coreBox("ensure-running-active")
	box.Box.Phase, box.Box.Generation = "running", 9
	box.ClientID, box.Profile = "client-a", f.config.Profiles[0]
	if err := f.service.store.Update(func(st *State) error {
		st.Boxes[box.Box.ID] = box
		st.Executions["exec-active"] = executionRecord{Execution: Execution{ID: "exec-active", BoxID: box.Box.ID, State: "running"}}
		st.Leases["lease-active"] = Lease{ID: "lease-active", BoxID: box.Box.ID}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/boxes/ensure-running-active:ensure-running", nil)
	request.Header.Set("Idempotency-Key", "ensure-running")
	request.Header.Set("Authorization", "Bearer "+testClientToken)
	response := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(response, request)
	wantStatus(t, response.Code, http.StatusOK, response.Body.Bytes())
	op := decodeResponse[Operation](t, response.Body.Bytes())
	if op.Status != "succeeded" || op.Box == nil || op.Box.ID != box.Box.ID || op.Box.OperationID != "" || op.Box.Phase != "running" {
		t.Fatalf("ensure-running did not return a successful final snapshot: %+v", op)
	}
	if p.inspects.Load() == 0 {
		t.Fatal("ensure-running skipped its required runtime observation")
	}
}

func TestEnsureRunningJoinsExistingResumeWithDifferentKey(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	box := coreBox("ensure-join")
	box.Box.Phase, box.Box.Generation, box.Box.OperationID = "resuming", 4, "op-existing"
	box.ClientID, box.Profile = "client-a", f.config.Profiles[0]
	op := Operation{ID: "op-existing", Kind: "resume", TargetID: box.Box.ID, Status: "running", Version: 1}
	if err := f.service.store.Update(func(st *State) error {
		st.Boxes[box.Box.ID] = box
		st.Operations[op.ID] = operationRecord{Operation: op, ClientID: "client-a"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	joined, err := f.service.ensureRunning(context.Background(), "client-a", "ensure-alias", box.Box.ID, nil)
	if err != nil || joined.ID != op.ID || joined.Kind != "resume" {
		t.Fatalf("different-key ensure did not join the existing resume: %+v, %v", joined, err)
	}
	snapshot, err := f.service.store.BoxAdmission("client-a", box.Box.ID, "client-a:ensure-running:"+box.Box.ID+":ensure-alias")
	if err != nil || !snapshot.KeyExists || snapshot.KeyOperation.ID != op.ID {
		t.Fatalf("joined operation key was not durably bound: %+v, %v", snapshot, err)
	}
}

func TestEnsureRunningRejectsStaleGenerationBeforeMutation(t *testing.T) {
	store := openRecords(t, &ledgerObjects{})
	box := coreBox("ensure-stale")
	box.Box.Phase, box.Box.Generation = "running", 12
	box.ClientID = "client-a"
	service := &Service{store: store}
	if err := store.Update(func(st *State) error {
		st.Boxes[box.Box.ID] = box
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stale := uint64(11)
	if _, err := service.ensureRunning(context.Background(), "client-a", "stale", box.Box.ID, &stale); err == nil || err.(*APIError).Code != "STALE_GENERATION" {
		t.Fatalf("stale generation was admitted: %v", err)
	}
	after, err := store.BoxRecord(box.Box.ID)
	if err != nil || after.Box.OperationID != "" || after.Box.Phase != "running" {
		t.Fatalf("stale generation mutated Box: %+v, %v", after.Box, err)
	}
}

func TestGetBoxDoesNotInspectRuntime(t *testing.T) {
	store := openRecords(t, &ledgerObjects{})
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
	if before.Box != after.Box || before.TransitionID != after.TransitionID || before.ObservedTransitionID != after.ObservedTransitionID {
		t.Fatalf("GET mutated the local projection: before=%+v after=%+v", before, after)
	}
}

func TestObsoleteLifecycleWorkerCannotOverwriteTerminalOperation(t *testing.T) {
	store := openRecords(t, &ledgerObjects{})
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
	service.launchWithCompletion(context.Background(), op, func(context.Context) (map[string]string, error) {
		close(entered)
		<-release
		return map[string]string{"boxId": box.Box.ID}, nil
	}, nil, nil)
	<-entered
	updateRecords(t, store, func(st *State) {
		record := st.Operations[op.ID]
		record.Operation.Status = "failed"
		record.Operation.Error = &APIError{Code: "OPERATION_INTERRUPTED", Message: "new lifecycle owns recovery"}
		st.Operations[op.ID] = record
		current := st.Boxes[box.Box.ID]
		current.Box.OperationID = "newer-operation"
		st.Boxes[box.Box.ID] = current
	})
	close(release)
	service.wg.Wait()
	record, err := store.Operation("owner", op.ID)
	if err != nil || record.Status != "failed" || record.Error == nil || record.Error.Code != "OPERATION_INTERRUPTED" {
		t.Fatalf("stale worker overwrote terminal operation: %+v, %v", record, err)
	}
	current, err := store.BoxRecord(box.Box.ID)
	if err != nil || current.Box.OperationID != "newer-operation" {
		t.Fatalf("stale worker cleared newer lifecycle ownership: %+v, %v", current.Box, err)
	}
}

type failingUpgradeProvider struct {
	*fakeCoreProvider
	calls atomic.Int32
}

func (p *failingUpgradeProvider) Upgrade(context.Context, boxprovider.Handle, boxprovider.Spec, string) (boxprovider.Handle, error) {
	p.calls.Add(1)
	return boxprovider.Handle{}, context.DeadlineExceeded
}

func TestRuntimeObserverBacksOffForPendingUpgradeIntent(t *testing.T) {
	store := openRecords(t, &ledgerObjects{})
	box := coreBox("observer-upgrade-backoff")
	box.ClientID = "owner"
	box.Box.OperationID = "upgrade-op"
	box.Box.Phase = "upgrading"
	box.Upgrade = &upgradeIntent{Nonce: "upgrade-op"}
	updateRecords(t, store, func(st *State) { st.Boxes[box.Box.ID] = box })
	provider := &failingUpgradeProvider{fakeCoreProvider: &fakeCoreProvider{created: true}}
	ctx, cancel := context.WithCancel(context.Background())
	service := &Service{ctx: ctx, cancel: cancel, store: store, providers: map[string]boxprovider.Provider{"resumable-k8s-pod": provider}}
	service.wg.Add(1)
	go service.observeBoxLoop(box.Box.ID)
	deadline := time.After(time.Second)
	for provider.calls.Load() == 0 {
		select {
		case <-deadline:
			cancel()
			service.wg.Wait()
			t.Fatal("observer did not attempt the pending upgrade")
		case <-time.After(time.Millisecond):
		}
	}
	// The failed first attempt must not turn into the previous 250ms/API spin.
	select {
	case <-time.After(100 * time.Millisecond):
	case <-deadline:
		cancel()
		service.wg.Wait()
		t.Fatal("observer retry deadline elapsed")
	}
	if got := provider.calls.Load(); got != 1 {
		cancel()
		service.wg.Wait()
		t.Fatalf("observer retried unresolved upgrade too quickly: %d attempts", got)
	}
	cancel()
	service.wg.Wait()
}

type transitionPhaseProvider struct {
	*fakeCoreProvider
	mu                 sync.Mutex
	phase              string
	transitionID       string
	observedTransition string
	execution          string
	actionCalls        int
}

func (p *transitionPhaseProvider) Inspect(context.Context, boxprovider.Handle) (boxprovider.Observation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return boxprovider.Observation{Phase: p.phase, ExecutionID: p.execution, TransitionID: p.transitionID, ObservedTransitionID: p.observedTransition}, nil
}

func (p *transitionPhaseProvider) Action(ctx context.Context, h boxprovider.Handle, action string) error {
	p.mu.Lock()
	previous := p.transitionID
	p.mu.Unlock()
	return p.ActionTransition(ctx, h, action, previous, "")
}

func (p *transitionPhaseProvider) ActionTransition(_ context.Context, _ boxprovider.Handle, action, expected, next string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.transitionID != expected {
		return boxprovider.ErrStaleTransition
	}
	if action != "resume" || next == "" {
		return boxprovider.ErrUnsupported
	}
	p.actionCalls++
	p.phase = "running"
	p.transitionID, p.observedTransition = next, next
	p.execution = "execution-after-resume"
	return nil
}

func TestEnsureRunningRepairsFailedUnquiesceWithoutRepeatingRuntimeResume(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	var guestCalls atomic.Int32
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/unquiesce" && guestCalls.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer guest.Close()
	baseProvider := &fakeCoreProvider{guestURL: guest.URL, created: true}
	provider := &transitionPhaseProvider{fakeCoreProvider: baseProvider, phase: "suspended", transitionID: "prior-transition", observedTransition: "prior-transition", execution: "before-resume"}
	f.service.providers["resumable-k8s-pod"] = provider
	profile := f.config.Profiles[0]
	profile.Provider = "resumable-k8s-pod"
	box := coreBox("ensure-unquiesce-repair")
	box.Box.Phase = "suspended"
	box.ClientID, box.Profile = "client-a", profile
	box.Handle = boxprovider.Handle{Provider: profile.Provider, ID: "runtime-repair"}
	box.TransitionID, box.ObservedTransitionID = "prior-transition", "prior-transition"
	box.ExecutionID = "before-resume"
	if err := f.service.store.Update(func(st *State) error { st.Boxes[box.Box.ID] = box; return nil }); err != nil {
		t.Fatal(err)
	}
	call := func(key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/boxes/"+box.Box.ID+":ensure-running", nil)
		r.Header.Set("Idempotency-Key", key)
		r.Header.Set("Authorization", "Bearer "+testClientToken)
		w := httptest.NewRecorder()
		f.service.Handler().ServeHTTP(w, r)
		return w
	}
	first := call("ensure-first")
	wantStatus(t, first.Code, http.StatusBadGateway, first.Body.Bytes())
	pending, err := f.service.rawBox(box.Box.ID)
	if err != nil || !pending.ResumePending || !pending.ResumeNeedsGuest || pending.Box.OperationID != "" || pending.Box.Phase != "resuming" {
		t.Fatalf("failed unquiesce lost its durable repair intent: %+v, %v", pending, err)
	}
	get := httptest.NewRequest(http.MethodGet, "/v1/boxes/"+box.Box.ID, nil)
	get.Header.Set("Authorization", "Bearer "+testClientToken)
	getResult := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(getResult, get)
	if got := decodeResponse[Box](t, getResult.Body.Bytes()); got.Phase == "running" {
		t.Fatal("GET reported a Box running before Guest unquiesce completed")
	}
	second := call("ensure-repair")
	wantStatus(t, second.Code, http.StatusOK, second.Body.Bytes())
	completed := decodeResponse[Operation](t, second.Body.Bytes())
	if completed.Status != "succeeded" || completed.Box == nil || completed.Box.Phase != "running" {
		t.Fatalf("repair did not return the completed Box snapshot: %+v", completed)
	}
	provider.mu.Lock()
	actions := provider.actionCalls
	provider.mu.Unlock()
	if actions != 1 || guestCalls.Load() != 2 {
		t.Fatalf("repair repeated runtime resume or skipped guest retry: runtimeActions=%d guestCalls=%d", actions, guestCalls.Load())
	}
	final, err := f.service.rawBox(box.Box.ID)
	if err != nil || final.ResumePending || final.ResumeNeedsGuest || final.Box.Phase != "running" {
		t.Fatalf("resume repair did not clear its intent: %+v, %v", final, err)
	}
}
