package service

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
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

func TestObserveUnchangedBoxDoesNotRewriteStateFile(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "query-no-write")
	path := f.config.DataDir + "/state.json"
	before, err := os.Stat(path)
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
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
		t.Fatalf("unchanged observation rewrote state file: before=%v after=%v", before.ModTime(), after.ModTime())
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

func TestListObservationIsBoundedAndReturnsNoPartialSuccess(t *testing.T) {
	f := newCoreFixture(t)
	for i := 0; i < 10; i++ {
		f.createBox(t, "query-workers-"+string(rune('a'+i)))
	}
	var active, maxActive atomic.Int32
	var calls atomic.Int32
	var mu sync.Mutex
	failed := false
	replaceQueryProvider(f, func(ctx context.Context, handle boxprovider.Handle) (boxprovider.Observation, error) {
		calls.Add(1)
		current := active.Add(1)
		defer active.Add(-1)
		for old := maxActive.Load(); current > old && !maxActive.CompareAndSwap(old, current); old = maxActive.Load() {
		}
		time.Sleep(20 * time.Millisecond)
		observation, err := f.provider.Inspect(ctx, handle)
		mu.Lock()
		shouldFail := failed
		mu.Unlock()
		if shouldFail {
			return boxprovider.Observation{}, errors.New("provider inspect failed")
		}
		return observation, err
	})
	status, body := f.call(t, "GET", "/v1/boxes", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if boxes := decodeResponse[[]Box](t, body); len(boxes) != 10 {
		t.Fatalf("list returned %d boxes, want 10", len(boxes))
	}
	if got := maxActive.Load(); got > 8 || got < 2 || calls.Load() != 10 {
		t.Fatalf("Inspect concurrency/calls = %d/%d, want concurrency 2..8 and 10 calls", got, calls.Load())
	}
	mu.Lock()
	failed = true
	mu.Unlock()
	status, body = f.call(t, "GET", "/v1/boxes", testClientToken, "", nil)
	if status == 200 {
		t.Fatalf("provider error returned a successful list: %s", body)
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
			status, body := f.call(t, "GET", "/v1/boxes/"+box.ID, testClientToken, "", nil)
			wantStatus(t, status, 200, body)
			got := decodeResponse[Box](t, body)
			if got.Phase != "failed" {
				t.Fatalf("phase %q was hidden by active transition: %+v", phase, got)
			}
		})
	}
}
