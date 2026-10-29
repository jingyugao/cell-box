package service

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
)

type resumeTestProvider struct {
	*fakeCoreProvider
	initial     boxprovider.Observation
	stream      chan boxprovider.Observation
	requests    atomic.Int32
	watches     atomic.Int32
	inspects    atomic.Int32
	requestIn   chan struct{}
	portsIn     chan []int
	release     chan struct{}
	holdRequest bool
	releaseOnce sync.Once
	result      *boxprovider.Observation
}

func (p *resumeTestProvider) unblock() {
	if p.release != nil {
		p.releaseOnce.Do(func() { close(p.release) })
	}
}

func (p *resumeTestProvider) Inspect(ctx context.Context, h boxprovider.Handle) (boxprovider.Observation, error) {
	p.inspects.Add(1)
	return p.fakeCoreProvider.Inspect(ctx, h)
}

func (p *resumeTestProvider) WatchObservations(ctx context.Context, _ boxprovider.Handle) (<-chan boxprovider.Observation, error) {
	p.watches.Add(1)
	out := make(chan boxprovider.Observation, 8)
	out <- p.initial
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case observation := <-p.stream:
				select {
				case out <- observation:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

func (p *resumeTestProvider) RequestResume(_ context.Context, _ boxprovider.Handle, _, transitionID string, waitPorts ...int) error {
	p.requests.Add(1)
	if p.portsIn != nil {
		ports := append([]int(nil), waitPorts...)
		select {
		case p.portsIn <- ports:
		default:
		}
	}
	if p.requestIn != nil {
		select {
		case p.requestIn <- struct{}{}:
		default:
		}
	}
	if p.holdRequest {
		<-p.release
	}
	observation := boxprovider.Observation{
		Phase: "running", Generation: 2, ExecutionID: "restored-execution",
		TransitionID: transitionID, ObservedTransitionID: transitionID,
	}
	if p.result != nil {
		observation = *p.result
		observation.TransitionID = transitionID
		observation.ObservedTransitionID = transitionID
	}
	p.stream <- observation
	return nil
}

func newResumeTestBox(t *testing.T, f *coreFixture, id string, p *resumeTestProvider) boxRecord {
	t.Helper()
	f.service.providers["resumable-k8s-pod"] = p
	profile := f.config.Profiles[0]
	profile.Provider = "resumable-k8s-pod"
	b := boxRecord{
		Box:      Box{ID: id, Phase: "suspended", Generation: 1},
		ClientID: "client-a", Profile: profile,
		Handle:      boxprovider.Handle{Provider: "docker", ID: "runtime-" + id},
		ExecutionID: "old-execution", TransitionID: "before-resume", ObservedTransitionID: "before-resume",
	}
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error { return tx.PutBox(b) }); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestNodeResumeWaitsForCommittedRunningTransitionOnOneStream(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	p := &resumeTestProvider{fakeCoreProvider: f.provider, stream: make(chan boxprovider.Observation, 8), initial: boxprovider.Observation{Phase: "suspended", Generation: 1}, result: &boxprovider.Observation{Phase: "resuming", Generation: 2}}
	b := newResumeTestBox(t, f, "minimal-resume", p)
	op, err := f.service.actionContextWithGeneration(context.Background(), b.ClientID, b.Box.ID, "resume", ptrUint64(1))
	if err != nil {
		t.Fatal(err)
	}
	if op.Status != "running" {
		t.Fatalf("admitted operation status = %q", op.Status)
	}
	pending, err := f.service.waitOperation(context.Background(), b.ClientID, op.ID, 40*time.Millisecond)
	if err != nil || pending.Status != "running" {
		t.Fatalf("resume completed before a committed running observation: %+v, %v", pending, err)
	}
	if pending.Result["resumePath"] != "" {
		t.Fatalf("resume path was reported before completion: %+v", pending.Result)
	}
	// Running is insufficient until the controller commits this transition.
	p.stream <- boxprovider.Observation{Phase: "running", Generation: 2, ExecutionID: "restored-execution", TransitionID: op.ID, ObservedTransitionID: "before-resume"}
	pending, err = f.service.waitOperation(context.Background(), b.ClientID, op.ID, 40*time.Millisecond)
	if err != nil || pending.Status != "running" {
		t.Fatalf("resume completed before transition commit: %+v, %v", pending, err)
	}
	p.stream <- boxprovider.Observation{Phase: "running", Generation: 2, ExecutionID: "restored-execution", TransitionID: op.ID, ObservedTransitionID: op.ID, ResumePath: "warm_pool"}
	finished, err := f.service.waitOperation(context.Background(), b.ClientID, op.ID, 2*time.Second)
	if err != nil || finished.Status != "succeeded" || finished.Box == nil || finished.Box.Generation != 2 || finished.Box.ExecutionID != "restored-execution" {
		t.Fatalf("resume result: %+v, %v", finished, err)
	}
	if finished.Result["resumePath"] != "warm_pool" {
		t.Fatalf("resume operation omitted completed path: %+v", finished.Result)
	}
	if p.requests.Load() != 1 || p.watches.Load() != 1 || p.guestCount() != 0 || p.inspects.Load() != 0 {
		t.Fatalf("resume calls=%d watches=%d Guest=%d Inspect=%d", p.requests.Load(), p.watches.Load(), p.guestCount(), p.inspects.Load())
	}
}

func TestNodeResumeOperationResultIncludesStandardPathAndOmitsLegacyPath(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "standard", path: "standard"},
		{name: "legacy missing", path: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCoreFixture(t)
			defer f.service.Close()
			p := &resumeTestProvider{
				fakeCoreProvider: f.provider,
				stream:           make(chan boxprovider.Observation, 4),
				initial:          boxprovider.Observation{Phase: "suspended", Generation: 1},
				result:           &boxprovider.Observation{Phase: "running", Generation: 2, ExecutionID: "restored", ResumePath: tc.path},
			}
			b := newResumeTestBox(t, f, "resume-path-"+strings.ReplaceAll(tc.name, " ", "-"), p)
			op, err := f.service.actionContextWithGeneration(context.Background(), b.ClientID, b.Box.ID, "resume", ptrUint64(1))
			if err != nil {
				t.Fatal(err)
			}
			finished, err := f.service.waitOperation(context.Background(), b.ClientID, op.ID, 2*time.Second)
			if err != nil || finished.Status != "succeeded" {
				t.Fatalf("resume operation = %+v, err=%v", finished, err)
			}
			got, exists := finished.Result["resumePath"]
			if tc.path == "" {
				if exists {
					t.Fatalf("legacy observation unexpectedly reported resumePath=%q", got)
				}
			} else if !exists || got != tc.path {
				t.Fatalf("resumePath=%q exists=%v, want %q", got, exists, tc.path)
			}
		})
	}
}

func TestFailedResumeConsumesRuntimeObservationAndSettlesBox(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	p := &resumeTestProvider{
		fakeCoreProvider: f.provider,
		stream:           make(chan boxprovider.Observation, 4),
		initial:          boxprovider.Observation{Phase: "suspended", Generation: 1},
		result:           &boxprovider.Observation{Phase: "failed", Generation: 2, ExecutionID: "failed-execution"},
	}
	b := newResumeTestBox(t, f, "failed-resume", p)
	op, err := f.service.actionContextWithGeneration(context.Background(), b.ClientID, b.Box.ID, "resume", ptrUint64(1))
	if err != nil {
		t.Fatal(err)
	}
	finished, err := f.service.waitOperation(context.Background(), b.ClientID, op.ID, 2*time.Second)
	if err != nil || finished.Status != "failed" || finished.Error == nil || finished.Error.Code != "READINESS_FAILED" {
		t.Fatalf("failed runtime resume = %+v, %v", finished, err)
	}
	current, err := f.service.rawBox(b.Box.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Box.Phase != "failed" || current.Box.OperationID != "" {
		t.Fatalf("consumed failed observation left Box unsettled: %+v", current.Box)
	}
}

func ptrUint64(v uint64) *uint64 { return &v }
