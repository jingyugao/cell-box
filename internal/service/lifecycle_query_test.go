package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jingyugao/cell-box/internal/boxprovider"
)

type queryProviderHook struct {
	boxprovider.Provider
	inspect func(context.Context, boxprovider.Handle) (boxprovider.Observation, error)
}

func (p *queryProviderHook) Inspect(ctx context.Context, handle boxprovider.Handle) (boxprovider.Observation, error) {
	return p.inspect(ctx, handle)
}

func replaceQueryProvider(f *coreFixture, inspect func(context.Context, boxprovider.Handle) (boxprovider.Observation, error)) {
	f.service.providers["docker"] = &queryProviderHook{Provider: f.provider, inspect: inspect}
}

func TestObservePreservesSuspendedGenerationAfterServiceRestart(t *testing.T) {
	dir := t.TempDir()
	initial, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := initial.Update(func(st *State) error {
		b := coreBox("paused-after-restart")
		b.Profile.Provider = "resumable-k8s-pod"
		b.Box.Phase = "suspended"
		b.Box.Generation = 17
		b.ExecutionID = ""
		st.Boxes[b.Box.ID] = b
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.Close() })
	record, err := restarted.BoxRecord("paused-after-restart")
	if err != nil {
		t.Fatal(err)
	}
	if record.Box.Generation != 17 || record.ExecutionID != "" {
		t.Fatalf("restart lost durable runtime observation: generation=%d execution=%q", record.Box.Generation, record.ExecutionID)
	}

	generation := uint64(17)
	provider := &queryProviderHook{
		Provider: &fakeCoreProvider{},
		inspect: func(context.Context, boxprovider.Handle) (boxprovider.Observation, error) {
			return boxprovider.Observation{Phase: "suspended", Generation: generation}, nil
		},
	}
	s := &Service{ctx: context.Background(), store: restarted,
		providers: map[string]boxprovider.Provider{"resumable-k8s-pod": provider}}
	observed, err := s.observe(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Phase != "suspended" || observed.Generation != 17 {
		t.Fatalf("paused CR observation did not restore generation: %+v", observed)
	}
	current, err := restarted.BoxRecord(record.Box.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.ExecutionID != "" || current.Box.Generation != 17 {
		t.Fatalf("suspended observation produced wrong runtime identity: generation=%d execution=%q", current.Box.Generation, current.ExecutionID)
	}

	// A delayed stale observation must not move the durable fence backwards.
	generation = 16
	observed, err = s.observe(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Generation != 17 {
		t.Fatalf("stale provider generation regressed the Box fence: %+v", observed)
	}

	generation = 18
	provider.inspect = func(context.Context, boxprovider.Handle) (boxprovider.Observation, error) {
		return boxprovider.Observation{Phase: "running", Generation: generation, ExecutionID: "pod-18"}, nil
	}
	observed, err = s.observe(context.Background(), current)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Generation != 18 || observed.Phase != "running" {
		t.Fatalf("new execution did not advance generation fence: %+v", observed)
	}
	current, err = restarted.BoxRecord(record.Box.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Box.Generation != 18 || current.ExecutionID != "pod-18" {
		t.Fatalf("new execution identity was not adopted: generation=%d execution=%q", current.Box.Generation, current.ExecutionID)
	}
}

func TestObserveUnchangedBoxDoesNotWriteDatabase(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "query-no-write")
	// A trigger observes any accidental write to the sandbox record.
	_, err := f.service.store.db.Exec(`CREATE TRIGGER reject_unchanged_observation BEFORE UPDATE ON runtime_records
 WHEN OLD.kind='boxes' AND OLD.id='` + box.ID + `' BEGIN SELECT RAISE(ABORT,'unexpected observation write'); END`)
	if err != nil {
		t.Fatal(err)
	}
	record, err := f.service.rawBox(box.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.service.observe(context.Background(), record); err != nil {
		t.Fatal(err)
	}
}

func TestOperationQueryPreservesIsolationAndOwnership(t *testing.T) {
	f := newCoreFixture(t)
	op, _ := f.createBox(t, "query-operation-copy")
	if err := f.service.store.Update(func(st *State) error {
		record := st.Operations[op.ID]
		record.Operation.Error = &APIError{Code: "original", Message: "original"}
		st.Operations[op.ID] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	copy, err := f.service.operation("client-a", op.ID)
	if err != nil {
		t.Fatal(err)
	}
	copy.Result["boxId"] = "changed"
	copy.Error.Code = "changed"
	*copy.FinishedAt = time.Time{}
	original, err := f.service.operation("client-a", op.ID)
	if err != nil || original.Result["boxId"] == "changed" || original.Error.Code != "original" || original.FinishedAt.IsZero() {
		t.Fatalf("query exposed mutable operation state: %+v %v", original, err)
	}
	if _, err := f.service.operation("client-b", op.ID); err == nil {
		t.Fatal("operation query exposed another client's record")
	}
	if _, err := f.service.operation("client-a", "missing-operation"); err == nil {
		t.Fatal("missing operation was accepted")
	}
}

func TestObserveCASRetriesAfterIdentityAndVersionChange(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "query-cas-retry")
	var calls atomic.Int32
	replaceQueryProvider(f, func(ctx context.Context, handle boxprovider.Handle) (boxprovider.Observation, error) {
		observation, err := f.provider.Inspect(ctx, handle)
		if calls.Add(1) == 1 {
			if err != nil {
				return observation, err
			}
			if err = f.service.store.Update(func(st *State) error {
				record := st.Boxes[box.ID]
				record.Box.Phase = "suspending"
				record.Box.OperationID = "new-action"
				record.Box.Generation = 9
				record.Box.Version++
				record.ExecutionID = "execution-new"
				st.Boxes[box.ID] = record
				return nil
			}); err != nil {
				return boxprovider.Observation{}, err
			}
			f.provider.setExecutionID("execution-new")
		}
		return observation, err
	})

	base, err := f.service.rawBox(box.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.service.observe(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	current, err := f.service.rawBox(box.ID)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() < 2 {
		t.Fatalf("observation did not retry after CAS conflict: Inspect calls=%d", calls.Load())
	}
	if got.Phase != "suspending" || current.Box.Phase != "suspending" || current.Box.Generation != 9 || current.ExecutionID != "execution-new" {
		t.Fatalf("stale observation replaced newer box identity/state: got=%+v current=%+v exec=%q", got, current.Box, current.ExecutionID)
	}
}

func TestObserveCanceledDuringInspectDoesNotCommit(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "query-canceled")
	started := make(chan struct{})
	release := make(chan struct{})
	replaceQueryProvider(f, func(context.Context, boxprovider.Handle) (boxprovider.Observation, error) {
		close(started)
		<-release // Simulate a provider that returns after its caller was canceled.
		return boxprovider.Observation{Phase: "frozen", ExecutionID: "late-execution"}, nil
	})
	base, err := f.service.rawBox(box.ID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := f.service.observe(ctx, base); result <- err }()
	<-started
	cancel()
	close(release)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("observe error = %v, want context canceled", err)
	}
	current, err := f.service.rawBox(box.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Box.Phase != base.Box.Phase || current.Box.Version != base.Box.Version || current.ExecutionID != base.ExecutionID {
		t.Fatalf("canceled observation committed: before=%+v after=%+v", base, current)
	}
}

func TestListQueryUsesSavedProjectionWithoutInspectingRuntime(t *testing.T) {
	f := newCoreFixture(t)
	for i := 0; i < 10; i++ {
		f.createBox(t, "query-workers-"+string(rune('a'+i)))
	}
	var calls atomic.Int32
	replaceQueryProvider(f, func(context.Context, boxprovider.Handle) (boxprovider.Observation, error) {
		calls.Add(1)
		return boxprovider.Observation{}, errors.New("GET must not inspect runtime")
	})
	status, body := f.call(t, "GET", "/v1/boxes", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if boxes := decodeResponse[[]Box](t, body); len(boxes) != 10 {
		t.Fatalf("list returned %d boxes, want 10", len(boxes))
	}
	if calls.Load() != 0 {
		t.Fatalf("list query inspected runtime %d times", calls.Load())
	}
}

func TestFailedOrMissingRuntimeOverridesActiveTransition(t *testing.T) {
	for _, phase := range []string{"failed", "deleted"} {
		t.Run(phase, func(t *testing.T) {
			f := newCoreFixture(t)
			_, box := f.createBox(t, "query-transition-"+phase)
			if err := f.service.store.Update(func(st *State) error {
				record := st.Boxes[box.ID]
				record.Box.Phase = "suspending"
				record.Box.OperationID = "active-suspend"
				record.Box.Version++
				st.Boxes[box.ID] = record
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			replaceQueryProvider(f, func(context.Context, boxprovider.Handle) (boxprovider.Observation, error) {
				return boxprovider.Observation{Phase: phase}, nil
			})
			base, err := f.service.rawBox(box.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.service.observe(context.Background(), base); err != nil {
				t.Fatal(err)
			}
			updated, err := f.service.rawBox(box.ID)
			if err != nil {
				t.Fatal(err)
			}
			if updated.Box.Phase != "failed" {
				t.Fatalf("phase %q was hidden by active transition: %+v", phase, updated.Box)
			}
		})
	}
}
