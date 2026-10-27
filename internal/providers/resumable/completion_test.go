package resumable

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/runtimecontrol"
	"cellbox.local/cellbox/internal/telemetry"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func createSuspendedForResume(t *testing.T, p *Provider, s boxprovider.Spec, ctx context.Context, transition string) boxprovider.Handle {
	t.Helper()
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

func TestEmbeddedArchiveCompletionUsesDirectSnapshot(t *testing.T) {
	p, s, ctx := fixture(t)
	s.PersistentHome = true
	s.HomeVolumeUID = "volume-one"
	h := createSuspendedForResume(t, p, s, ctx, "suspended")
	w := &api.ResumablePod{}
	key := client.ObjectKey{Namespace: h.Namespace, Name: h.Name}
	if err := p.Client.Get(ctx, key, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Snapshot = "checkpoint-1"
	if err := p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	counted := &observedClient{Client: p.Client}
	local := runtimecontrol.New(counted)
	p.Client, p.Local = local, local
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer q.ShutDown()
	if err := local.Start(ctx, q); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	go func() { <-ctx.Done(); q.ShutDown() }()
	type result struct {
		size   int64
		digest string
		err    error
	}
	done := make(chan result, 1)
	id := "arc-0123456789abcdef0123456789abcdef"
	go func() { size, digest, err := p.CaptureWorkspaceArchive(ctx, h, id); done <- result{size, digest, err} }()
	request, stopped := q.Get()
	if stopped {
		t.Fatal("archive was not dispatched directly")
	}
	if err := counted.Client.Get(ctx, request.NamespacedName, w); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(api.ArchiveCaptureResult{SourceUID: string(w.UID), SourceGeneration: w.Generation, ID: id, Snapshot: w.Status.Snapshot, Size: 123, SHA256: strings.Repeat("a", 64)})
	w.Annotations[api.ArchiveResultAnnotation] = string(encoded)
	if err := local.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	q.Done(request)
	select {
	case got := <-done:
		if got.err != nil || got.size != 123 || got.digest != strings.Repeat("a", 64) {
			t.Fatalf("archive completion: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal("archive missed local completion")
	}
	// Initial admission, subscription and request cleanup are the only reads.
	if counted.gets.Load() != 3 || counted.watchedVersion.Load() != nil || counted.lists.Load() != 0 {
		t.Fatalf("archive polled Kubernetes: GETs=%d", counted.gets.Load())
	}
}

func TestEmbeddedResumeDispatchAndCompletionNeedNoKubernetesWatch(t *testing.T) {
	p, s, ctx := fixture(t)
	h := createSuspendedForResume(t, p, s, ctx, "previous")
	counted := &observedClient{Client: p.Client}
	local := runtimecontrol.New(counted)
	p.Client, p.Local = local, local
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer q.ShutDown()
	if err := local.Start(ctx, q); err != nil {
		t.Fatal(err)
	}
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	observations, err := p.WatchObservations(watchCtx, h)
	if err != nil {
		t.Fatal(err)
	}
	next := func() boxprovider.Observation {
		select {
		case event := <-observations:
			return event
		case <-time.After(3 * time.Second):
			t.Fatal("missing direct completion")
			return boxprovider.Observation{}
		}
	}
	if event := next(); event.Phase != "suspended" {
		t.Fatalf("initial state: %+v", event)
	}
	initialReads := counted.gets.Load()
	if err = p.RequestResume(ctx, h, "previous", "resume"); err != nil {
		t.Fatal(err)
	}
	if q.Len() != 1 {
		t.Fatal("resume did not directly dispatch runtime work")
	}
	request, _ := q.Get()
	if request.Name != h.Name || request.Namespace != h.Namespace {
		t.Fatal("wrong runtime key")
	}
	// This is the runtime's durable completion, on the same client used by the
	// embedded reconciler. Reads below bypass instrumentation to model its work.
	w := &api.ResumablePod{}
	if err = counted.Client.Get(ctx, request.NamespacedName, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase = "Running"
	w.Status.PodUID = "new-execution"
	w.Status.Cycle = 2
	w.Status.ObservedGeneration = w.Generation
	w.Status.ObservedTransitionID = "resume"
	w.Status.ResumeReadyTransitionID = "resume"
	if err = local.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	q.Done(request)
	for {
		event := next()
		if event.Phase == "running" && event.GuestReady {
			if event.ExecutionID != "new-execution" || event.Generation != 2 || event.TransitionID != "resume" {
				t.Fatalf("wrong completion: %+v", event)
			}
			break
		}
	}
	if counted.watchedVersion.Load() != nil || counted.lists.Load() != 0 || counted.gets.Load() != initialReads {
		t.Fatalf("resume used a watch/LIST/follow-up GET: reads=%d initial=%d lists=%d", counted.gets.Load(), initialReads, counted.lists.Load())
	}
}

type observedClient struct {
	client.Client
	gets           atomic.Int32
	lists          atomic.Int32
	watchedVersion atomic.Value
	patches        atomic.Int32
	watcher        *watch.FakeWatcher
}

func (c *observedClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*api.ResumablePod); ok {
		c.gets.Add(1)
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *observedClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*api.ResumablePodList); ok {
		c.lists.Add(1)
	}
	if err := c.Client.List(ctx, list, opts...); err != nil {
		return err
	}
	list.SetResourceVersion("current-collection-version")
	return nil
}

func (c *observedClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if _, ok := obj.(*api.ResumablePod); ok {
		c.patches.Add(1)
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *observedClient) Watch(_ context.Context, _ client.ObjectList, opts ...client.ListOption) (watch.Interface, error) {
	options := &client.ListOptions{}
	options.ApplyOptions(opts)
	c.watchedVersion.Store(options.Raw.ResourceVersion)
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
