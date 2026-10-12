package resumable

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/telemetry"
	"go.opentelemetry.io/otel/trace"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type observedClient struct {
	client.Client
	gets    atomic.Int32
	patches atomic.Int32
	watcher *watch.FakeWatcher
}

func (c *observedClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*api.ResumablePod); ok {
		c.gets.Add(1)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *observedClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if _, ok := obj.(*api.ResumablePod); ok {
		c.patches.Add(1)
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *observedClient) Watch(_ context.Context, _ client.ObjectList, _ ...client.ListOption) (watch.Interface, error) {
	return c.watcher, nil
}

func TestRequestResumeUsesOneConditionalPatch(t *testing.T) {
	p, s, ctx := fixture(t)
	h := createSuspendedForResume(t, p, s, ctx, "previous")
	c := &observedClient{Client: p.Client}
	p.Client = c

	if err := p.RequestResume(ctx, h, "previous", "next"); err != nil {
		t.Fatal(err)
	}
	if got := c.patches.Load(); got != 1 {
		t.Fatalf("patch count = %d, want 1", got)
	}
	if got := c.gets.Load(); got != 0 {
		t.Fatalf("GET count = %d, want 0 on normal path", got)
	}
}

func TestRequestResumeRejectsOldTransition(t *testing.T) {
	p, s, ctx := fixture(t)
	h := createSuspendedForResume(t, p, s, ctx, "current")
	c := &observedClient{Client: p.Client}
	p.Client = c

	err := p.RequestResume(ctx, h, "old", "next")
	if !errors.Is(err, boxprovider.ErrStaleTransition) {
		t.Fatalf("RequestResume error = %v, want stale transition", err)
	}
	if got := c.patches.Load(); got != 1 {
		t.Fatalf("patch count = %d, want one fenced patch attempt", got)
	}
	w := &api.ResumablePod{}
	if err := c.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	if w.Spec.TransitionID != "current" || w.Spec.DesiredState != "Suspended" {
		t.Fatalf("stale request changed lifecycle state: transition=%q desired=%q", w.Spec.TransitionID, w.Spec.DesiredState)
	}
}

func TestRequestResumeLegacyEmptyTokenCannotCrossTransition(t *testing.T) {
	p, s, ctx := fixture(t)
	h := createSuspendedForResume(t, p, s, ctx, "newer-transition")
	c := &observedClient{Client: p.Client}
	p.Client = c

	err := p.RequestResume(ctx, h, "", "legacy-resume")
	if !errors.Is(err, boxprovider.ErrStaleTransition) {
		t.Fatalf("RequestResume error = %v, want stale transition", err)
	}
	if got := c.patches.Load(); got != 0 {
		t.Fatalf("legacy stale request sent %d patches, want 0", got)
	}
	if got := c.gets.Load(); got != 1 {
		t.Fatalf("legacy stale request GET count = %d, want 1", got)
	}
}

func TestWatchObservationsMapsEventsWithoutGETAndIgnoresBookmarks(t *testing.T) {
	p, s, ctx := fixture(t)
	h := createSuspendedForResume(t, p, s, ctx, "transition-1")
	w := &api.ResumablePod{}
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	w.Spec.DesiredState = "Running"
	w.Status.Phase = "Running"
	w.Status.PodUID = "exec-1"
	w.Status.Cycle = 7
	w.Status.ObservedTransitionID = "transition-1"
	w.Status.ResumeReadyTransitionID = "transition-1"
	w.Status.ObservedGeneration = w.Generation
	if err := p.Client.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase = "Running"
	w.Status.PodUID = "exec-1"
	w.Status.Cycle = 7
	w.Status.ObservedTransitionID = "transition-1"
	w.Status.ResumeReadyTransitionID = "transition-1"
	w.Status.ObservedGeneration = w.Generation
	if err := p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}

	fakeWatch := watch.NewFake()
	c := &observedClient{Client: p.Client, watcher: fakeWatch}
	p.Client = c
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	observations, err := p.WatchObservations(watchCtx, h)
	if err != nil {
		t.Fatal(err)
	}
	initial := receiveObservation(t, observations)
	if initial.Phase != "running" || initial.Generation != 7 || initial.ExecutionID != "exec-1" || !initial.GuestReady {
		t.Fatalf("unexpected initial observation: %#v", initial)
	}
	if got := c.gets.Load(); got != 1 {
		t.Fatalf("initial GET count = %d, want exactly one", got)
	}

	fakeWatch.Action(watch.Bookmark, &api.ResumablePod{ObjectMeta: meta.ObjectMeta{ResourceVersion: "2"}})
	select {
	case got := <-observations:
		t.Fatalf("bookmark unexpectedly produced observation: %#v", got)
	case <-time.After(30 * time.Millisecond):
	}

	updated := w.DeepCopy()
	updated.ResourceVersion = "3"
	updated.Status.Cycle = 8
	updated.Status.PodUID = "exec-2"
	updated.Spec.TransitionID = "transition-2"
	updated.Status.ObservedTransitionID = "transition-2"
	updated.Status.ResumeReadyTransitionID = "transition-2"
	fakeWatch.Modify(updated)
	event := receiveObservation(t, observations)
	if event.Generation != 8 || event.ExecutionID != "exec-2" || event.TransitionID != "transition-2" || !event.GuestReady {
		t.Fatalf("unexpected watch observation: %#v", event)
	}
	if got := c.gets.Load(); got != 1 {
		t.Fatalf("watch event caused extra GET: count=%d", got)
	}

	cancel()
}

func createSuspendedForResume(t *testing.T, p *Provider, s boxprovider.Spec, ctx context.Context, transition string) boxprovider.Handle {
	t.Helper()
	// Keep the fixture's CR identity while putting it at a durably observed
	// suspended transition, the admission state required by RequestResume.
	h, err := p.Create(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{}
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	w.Spec.DesiredState = "Suspended"
	w.Spec.TransitionID = transition
	if err := p.Client.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase = "Suspended"
	w.Status.ObservedTransitionID = transition
	if err := p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	return h
}

func receiveObservation(t *testing.T, observations <-chan boxprovider.Observation) boxprovider.Observation {
	t.Helper()
	select {
	case observation, ok := <-observations:
		if !ok {
			t.Fatal("observation stream closed")
		}
		return observation
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for observation")
		return boxprovider.Observation{}
	}
}

func TestRequestResumeSameTokenRecoveryDoesNotRestoreAgain(t *testing.T) {
	p, spec, ctx := fixture(t)
	h := createSuspendedForResume(t, p, spec, ctx, "previous")
	if err := p.RequestResume(ctx, h, "previous", "resume"); err != nil {
		t.Fatal(err)
	}
	c := &observedClient{Client: p.Client}
	p.Client = c
	if err := p.RequestResume(ctx, h, "resume", "resume"); err != nil {
		t.Fatal(err)
	}
	if c.patches.Load() != 0 || c.gets.Load() != 1 {
		t.Fatal("recovery changed the existing command")
	}
}

type reconnectClient struct {
	*observedClient
	calls atomic.Int32
	final *watch.FakeWatcher
}

func (c *reconnectClient) Watch(context.Context, client.ObjectList, ...client.ListOption) (watch.Interface, error) {
	switch c.calls.Add(1) {
	case 1:
		return c.watcher, nil
	case 2:
		return nil, errors.New("temporary connection failure")
	default:
		return c.final, nil
	}
}
func TestWatchReconnectRetriesAfterOpenFailure(t *testing.T) {
	p, spec, ctx := fixture(t)
	h := createSuspendedForResume(t, p, spec, ctx, "previous")
	c := &reconnectClient{observedClient: &observedClient{Client: p.Client, watcher: watch.NewFake()}, final: watch.NewFake()}
	p.Client = c
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	observations, err := p.WatchObservations(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	receiveObservation(t, observations)
	c.watcher.Stop()
	receiveObservation(t, observations)
	if c.calls.Load() != 3 {
		t.Fatalf("watch attempts=%d", c.calls.Load())
	}
}

func TestTracedResumePreservesMetadataAndRepairsMissingAnnotations(t *testing.T) {
	p, spec, ctx := fixture(t)
	h := createSuspendedForResume(t, p, spec, ctx, "previous")
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled})
	ctx = trace.ContextWithSpanContext(ctx, sc)
	if err := p.RequestResume(ctx, h, "previous", "resume"); err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{}
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	if w.Spec.TransitionID != "resume" {
		t.Fatal("resume command missing")
	}
	w.Status.Phase = "Restoring"
	if trace.SpanContextFromContext(telemetry.ExtractOperation(context.Background(), w)).TraceID() != sc.TraceID() {
		t.Fatal("resume command lost trace")
	}
}

func TestWatchReconnectPublishesDeletionMissedWhileDisconnected(t *testing.T) {
	p, spec, ctx := fixture(t)
	h := createSuspendedForResume(t, p, spec, ctx, "previous")
	c := &observedClient{Client: p.Client, watcher: watch.NewFake()}
	p.Client = c
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	observations, err := p.WatchObservations(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	receiveObservation(t, observations)
	w := &api.ResumablePod{}
	if err := c.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	w.Finalizers = nil
	if err := c.Client.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := c.Client.Delete(ctx, w); err != nil {
		t.Fatal(err)
	}
	c.watcher.Stop()
	if got := receiveObservation(t, observations); got.Phase != "deleted" {
		t.Fatalf("missed deletion: %+v", got)
	}
}
