package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
)

type nodeResumeFixtureProvider struct {
	*fakeCoreProvider
	initial   boxprovider.Observation
	stream    chan boxprovider.Observation
	requests  atomic.Int32
	watches   atomic.Int32
	inspects  atomic.Int32
	holdGuest bool
}

func (p *nodeResumeFixtureProvider) WatchObservations(ctx context.Context, h boxprovider.Handle) (<-chan boxprovider.Observation, error) {
	p.watches.Add(1)
	stream := make(chan boxprovider.Observation, 8)
	stream <- p.initial
	go func() {
		defer close(stream)
		for {
			select {
			case <-ctx.Done():
				return
			case observation := <-p.stream:
				select {
				case stream <- observation:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return stream, nil
}
func (p *nodeResumeFixtureProvider) Inspect(ctx context.Context, h boxprovider.Handle) (boxprovider.Observation, error) {
	p.inspects.Add(1)
	return p.fakeCoreProvider.Inspect(ctx, h)
}
func (p *nodeResumeFixtureProvider) RequestResume(ctx context.Context, h boxprovider.Handle, previous, id string) error {
	p.requests.Add(1)
	p.stream <- boxprovider.Observation{Phase: "running", Generation: 2, ExecutionID: "restored-execution", TransitionID: id, ObservedTransitionID: id, GuestReady: !p.holdGuest}
	return nil
}

func TestNodeResumeUsesOneSharedStreamAndNoInspectOrAPIGuestRequest(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	p := &nodeResumeFixtureProvider{fakeCoreProvider: f.provider, stream: make(chan boxprovider.Observation, 8), initial: boxprovider.Observation{Phase: "suspended", Generation: 1, TransitionID: "suspend-before", ObservedTransitionID: "suspend-before"}}
	f.service.providers["docker"] = p
	b := coreBox("minimal-resume")
	b.ClientID, b.Profile = "client-a", f.config.Profiles[0]
	b.Box.Phase, b.Box.Generation = "suspended", 1
	b.TransitionID, b.ObservedTransitionID = "suspend-before", "suspend-before"
	if err := f.service.store.Update(func(st *State) error { st.Boxes[b.Box.ID] = b; return nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	op, err := f.service.ensureRunning(ctx, "client-a", "one-resume", b.Box.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	finished, err := f.service.waitResume(ctx, "client-a", op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Box == nil || finished.Box.Phase != "running" || finished.Box.ExecutionID != "restored-execution" || finished.Box.Generation != 2 {
		t.Fatalf("wrong result: %+v", finished)
	}
	replayed, err := f.service.ensureRunning(ctx, "client-a", "one-resume", b.Box.ID, nil)
	if err != nil || replayed.ID != op.ID {
		t.Fatalf("lost idempotent result: %+v, %v", replayed, err)
	}
	if p.requests.Load() != 1 || p.watches.Load() != 1 || p.inspects.Load() != 0 {
		t.Fatalf("requests=%d watches=%d inspect=%d", p.requests.Load(), p.watches.Load(), p.inspects.Load())
	}
	// fakeCoreProvider's Guest URL is deliberately empty. Completion therefore
	// also demonstrates that API-side connection resolution/unquiesce was absent.
}

func TestNodeResumeRecoveryUsesCompletedNodeResultWithoutAnotherCommand(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	p := &nodeResumeFixtureProvider{fakeCoreProvider: f.provider, stream: make(chan boxprovider.Observation, 8), initial: boxprovider.Observation{Phase: "running", Generation: 2, ExecutionID: "restored-execution", TransitionID: "interrupted-resume", ObservedTransitionID: "interrupted-resume", GuestReady: true}}
	f.service.providers["docker"] = p
	b := coreBox("minimal-resume-recovery")
	b.Box.Generation = 1
	b.ClientID, b.Profile = "client-a", f.config.Profiles[0]
	b.Box.Phase, b.Box.OperationID, b.ResumePending, b.ResumeIntentID = "resuming", "repair-after-interruption", true, "interrupted-resume"
	op := Operation{ID: "repair-after-interruption", Kind: "ensure-running", TargetID: b.Box.ID, Status: "running", Version: 1}
	if err := f.service.store.Update(func(st *State) error {
		st.Boxes[b.Box.ID] = b
		st.Operations[op.ID] = operationRecord{Operation: op, ClientID: "client-a"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f.service.recoverResumeOperations()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	finished, err := f.service.waitResume(ctx, "client-a", op.ID)
	if err != nil || finished.Status != "succeeded" {
		t.Fatalf("recovery: %+v, %v", finished, err)
	}
	if p.requests.Load() != 0 || p.watches.Load() != 1 || p.inspects.Load() != 0 {
		t.Fatalf("recovery repeated work: commands=%d watches=%d inspect=%d", p.requests.Load(), p.watches.Load(), p.inspects.Load())
	}
}

func TestNodeResumeDoesNotCompleteBeforeGuestReopens(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	p := &nodeResumeFixtureProvider{fakeCoreProvider: f.provider, holdGuest: true, stream: make(chan boxprovider.Observation, 8), initial: boxprovider.Observation{Phase: "suspended", Generation: 1, TransitionID: "previous", ObservedTransitionID: "previous"}}
	f.service.providers["docker"] = p
	b := coreBox("guest-incomplete")
	b.ClientID, b.Profile = "client-a", f.config.Profiles[0]
	b.Box.Phase, b.Box.Generation = "suspended", 1
	b.TransitionID, b.ObservedTransitionID = "previous", "previous"
	if err := f.service.store.Update(func(st *State) error { st.Boxes[b.Box.ID] = b; return nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	op, err := f.service.ensureRunning(ctx, "client-a", "wait-guest", b.Box.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	pending, cancelPending := context.WithTimeout(ctx, 50*time.Millisecond)
	_, err = f.service.waitResume(pending, "client-a", op.ID)
	cancelPending()
	if err == nil {
		t.Fatal("completed before Guest reopened")
	}
	p.stream <- boxprovider.Observation{Phase: "running", Generation: 2, ExecutionID: "restored-execution", TransitionID: op.ID, ObservedTransitionID: op.ID, GuestReady: true}
	if _, err = f.service.waitResume(ctx, "client-a", op.ID); err != nil {
		t.Fatal(err)
	}
	if p.requests.Load() != 1 {
		t.Fatal("repeated restore command")
	}
}
