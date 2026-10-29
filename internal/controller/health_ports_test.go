package controller

import (
	"context"
	"testing"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/node"
	"github.com/jingyugao/cell-box/internal/runtimecontrol"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type portHealthRuntime struct {
	*fakeRuntime
	ready       bool
	waitStarted chan struct{}
	eventReady  chan struct{}
	waitCalls   int
	healthCalls int
}

func (f *portHealthRuntime) CheckPortsListening(_ context.Context, w *api.ResumablePod, p *core.Pod) (*api.Execution, error) {
	if !f.ready {
		return nil, nil
	}
	return &api.Execution{PodUID: string(p.UID), ContainerID: "containerd://one", PodIP: p.Status.PodIP}, nil
}

func (f *portHealthRuntime) WaitPortsListening(ctx context.Context, w *api.ResumablePod, p *core.Pod) (*api.Execution, error) {
	f.waitCalls++
	select {
	case f.waitStarted <- struct{}{}:
	default:
	}
	select {
	case <-f.eventReady:
		return &api.Execution{PodUID: string(p.UID), ContainerID: "containerd://one", PodIP: p.Status.PodIP}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *portHealthRuntime) CheckHealth(context.Context, *api.ResumablePod, *core.Pod) error {
	f.healthCalls++
	if f.ready {
		return nil
	}
	return node.ErrPortsNotListening
}

func (*portHealthRuntime) InvalidateSnapshot(context.Context, *api.ResumablePod) error { return nil }
func (*portHealthRuntime) NeedsGuestUnquiesce(context.Context, *api.ResumablePod) (bool, error) {
	return false, nil
}

func prepareResumeFixture(t *testing.T, r *Reconciler, w *api.ResumablePod, phase string) *core.Pod {
	t.Helper()
	ctx := context.Background()
	key := client.ObjectKeyFromObject(w)
	if err := r.Get(ctx, key, w); err != nil {
		t.Fatal(err)
	}
	w.Spec.DesiredState = "Running"
	w.Spec.TransitionID = "resume-current"
	w.Spec.WaitPorts = []int{8080}
	w.Spec.ServicePorts = []core.ServicePort{{Name: "http", Port: 8080, TargetPort: intstr.FromInt32(8080)}}
	if err := r.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase = phase
	w.Status.Snapshot = "checkpoint-1"
	w.Status.Since = meta.Now()
	w.Status.SpecHash = fingerprint(w)
	if err := r.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	p := &core.Pod{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: w.Namespace, Name: w.Status.PodName}, p); err != nil {
		t.Fatal(err)
	}
	if p.Labels == nil {
		p.Labels = map[string]string{}
	}
	p.Labels[api.OwnerLabel] = string(w.UID)
	p.Labels[api.ServingLabel] = "false"
	if err := r.Update(ctx, p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestListenerSnapshotMakesResumeUsableBeforeHealthCheck(t *testing.T) {
	r, _, w := fixture(t)
	prepareResumeFixture(t, r, w, "Restoring")
	backend := &portHealthRuntime{fakeRuntime: &fakeRuntime{}, ready: true, waitStarted: make(chan struct{}, 1)}
	r.Runtime = backend
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(w)}); err != nil {
		t.Fatal(err)
	}
	current := &api.ResumablePod{}
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(w), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != "Running" || current.Status.Snapshot != "" || current.Status.ObservedTransitionID != "resume-current" {
		t.Fatalf("listener-ready resume was not committed: %+v", current.Status)
	}
	if backend.healthCalls != 0 || backend.waitCalls != 0 {
		t.Fatalf("resume depended on health or an event waiter after the snapshot found LISTEN: health=%d wait=%d", backend.healthCalls, backend.waitCalls)
	}
	p := &core.Pod{}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: w.Namespace, Name: current.Status.PodName}, p); err != nil {
		t.Fatal(err)
	}
	if p.Labels[api.ServingLabel] != "true" {
		t.Fatalf("listener-ready execution is still withdrawn: labels=%v", p.Labels)
	}
	svc := &core.Service{}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: w.Namespace, Name: w.Name}, svc); err != nil {
		t.Fatal(err)
	}
	for key, value := range svc.Spec.Selector {
		if p.Labels[key] != value {
			t.Fatalf("service selector does not include the listener-ready Pod: selector=%v labels=%v", svc.Spec.Selector, p.Labels)
		}
	}
}

func TestListenEventNotifiesResumeWithoutHealthCheck(t *testing.T) {
	r, _, w := fixture(t)
	prepareResumeFixture(t, r, w, "Restoring")
	backend := &portHealthRuntime{
		fakeRuntime: &fakeRuntime{},
		waitStarted: make(chan struct{}, 1),
		eventReady:  make(chan struct{}),
	}
	r.Runtime = backend
	local := runtimecontrol.New(r.Client)
	r.Client = local
	ctx := context.Background()
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(q.ShutDown)
	if err := local.Start(ctx, q); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(w)
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-backend.waitStarted:
	case <-time.After(time.Second):
		t.Fatal("resume did not start the event waiter")
	}
	backend.ready = true
	close(backend.eventReady)

	type queueResult struct {
		request  reconcile.Request
		shutdown bool
	}
	queued := make(chan queueResult, 1)
	go func() {
		request, shutdown := q.Get()
		queued <- queueResult{request: request, shutdown: shutdown}
	}()
	var result queueResult
	select {
	case result = <-queued:
	case <-time.After(time.Second):
		t.Fatal("listen event did not notify the reconciliation queue")
	}
	if result.shutdown || result.request.NamespacedName != key {
		t.Fatalf("listen event queued the wrong request: request=%v shutdown=%t", result.request, result.shutdown)
	}
	defer q.Done(result.request)
	if _, err := r.Reconcile(ctx, result.request); err != nil {
		t.Fatal(err)
	}
	current := &api.ResumablePod{}
	if err := r.Get(ctx, key, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != "Running" || current.Status.Snapshot != "" || current.Status.ObservedTransitionID != "resume-current" {
		t.Fatalf("event-ready resume was not committed: %+v", current.Status)
	}
	if backend.healthCalls != 0 || backend.waitCalls != 1 {
		t.Fatalf("resume did not complete from exactly one listener event: health=%d wait=%d", backend.healthCalls, backend.waitCalls)
	}
}

func TestHealthSnapshotRecoversMissedListenEvent(t *testing.T) {
	r, _, w := fixture(t)
	prepareResumeFixture(t, r, w, "Restoring")
	backend := &portHealthRuntime{fakeRuntime: &fakeRuntime{}, waitStarted: make(chan struct{}, 1)}
	r.Runtime = backend
	local := runtimecontrol.New(r.Client)
	r.Client = local
	ctx := context.Background()
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(q.ShutDown)
	if err := local.Start(ctx, q); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(w)
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-backend.waitStarted:
	case <-time.After(time.Second):
		t.Fatal("resume did not start the event waiter")
	}
	backend.ready = true // The kernel event was missed, but the live socket exists.
	(&HealthChecker{Reconciler: r, Namespace: w.Namespace}).check(ctx, map[types.NamespacedName]healthFailure{})
	if backend.healthCalls != 1 {
		t.Fatalf("health check calls = %d, want 1", backend.healthCalls)
	}
	request, shutdown := q.Get()
	if shutdown || request.NamespacedName != key {
		t.Fatalf("health check did not enqueue the missed-event Box: request=%v shutdown=%t", request, shutdown)
	}
	defer q.Done(request)
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	current := &api.ResumablePod{}
	if err := r.Get(ctx, key, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != "Running" {
		t.Fatalf("health snapshot did not recover the missed event: phase=%s", current.Status.Phase)
	}
}

func TestHealthMarksMissingPortsFailedAndWithdrawsServingPod(t *testing.T) {
	r, _, w := fixture(t)
	p := prepareResumeFixture(t, r, w, "Running")
	p.Labels[api.ServingLabel] = "true"
	if err := r.Update(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if err := r.service(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	backend := &portHealthRuntime{fakeRuntime: &fakeRuntime{}, waitStarted: make(chan struct{}, 1)}
	r.Runtime = backend
	r.Client = runtimecontrol.New(r.Client)
	h := &HealthChecker{Reconciler: r, Namespace: w.Namespace}
	failures := make(map[types.NamespacedName]healthFailure)
	for range 3 {
		h.check(context.Background(), failures)
	}
	current := &api.ResumablePod{}
	key := client.ObjectKeyFromObject(w)
	if err := r.Get(context.Background(), key, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != "Failed" {
		t.Fatalf("missing listening port did not fail the execution: phase=%s message=%q", current.Status.Phase, current.Status.Message)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	retained := &core.Pod{}
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: w.Namespace, Name: p.Name}, retained); err != nil {
		t.Fatalf("health failure removed the Pod: %v", err)
	}
	if retained.Labels[api.ServingLabel] != "false" {
		t.Fatalf("failed execution remains in Service endpoints: labels=%v", retained.Labels)
	}
	if backend.healthCalls != 3 {
		t.Fatalf("health checks = %d, want 3 confirmations", backend.healthCalls)
	}
}

func TestRestoringHealthFailuresDoNotCarryIntoRunning(t *testing.T) {
	r, _, w := fixture(t)
	prepareResumeFixture(t, r, w, "Restoring")
	ctx := context.Background()
	key := client.ObjectKeyFromObject(w)
	current := &api.ResumablePod{}
	if err := r.Get(ctx, key, current); err != nil {
		t.Fatal(err)
	}
	current.Spec.StartupTimeoutSeconds = 30
	if err := r.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	current.Status.Since = meta.Now()
	if err := r.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	h := &HealthChecker{Reconciler: r}
	local := runtimecontrol.New(r.Client)
	failures := make(map[types.NamespacedName]healthFailure)
	for range 2 {
		current = &api.ResumablePod{}
		if err := r.Get(ctx, key, current); err != nil {
			t.Fatal(err)
		}
		h.recordHealthResult(ctx, local, failures, healthResult{resource: current, err: node.ErrPortsNotListening})
		if _, exists := failures[key]; exists {
			t.Fatal("startup-grace failures were retained for Running")
		}
	}
	current = &api.ResumablePod{}
	if err := r.Get(ctx, key, current); err != nil {
		t.Fatal(err)
	}
	current.Status.Since = meta.NewTime(time.Now().Add(-31 * time.Second))
	if err := r.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	for count := 1; count <= 2; count++ {
		current = &api.ResumablePod{}
		if err := r.Get(ctx, key, current); err != nil {
			t.Fatal(err)
		}
		h.recordHealthResult(ctx, local, failures, healthResult{resource: current, err: node.ErrPortsNotListening})
		current = &api.ResumablePod{}
		if err := r.Get(ctx, key, current); err != nil {
			t.Fatal(err)
		}
		if current.Status.Phase != "Restoring" || failures[key].count != count {
			t.Fatalf("startup-timeout failure %d was not retained independently: phase=%s failures=%+v", count, current.Status.Phase, failures[key])
		}
	}

	current = &api.ResumablePod{}
	if err := r.Get(ctx, key, current); err != nil {
		t.Fatal(err)
	}
	current.Status.Phase = "Running"
	current.Status.Since = meta.Now()
	if err := r.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	for count := 1; count <= 3; count++ {
		current = &api.ResumablePod{}
		if err := r.Get(ctx, key, current); err != nil {
			t.Fatal(err)
		}
		h.recordHealthResult(ctx, local, failures, healthResult{resource: current, err: node.ErrPortsNotListening})
		current = &api.ResumablePod{}
		if err := r.Get(ctx, key, current); err != nil {
			t.Fatal(err)
		}
		if count < 3 && current.Status.Phase != "Running" {
			t.Fatalf("Running failed after only %d Running health checks: phase=%s", count, current.Status.Phase)
		}
		if count == 3 && current.Status.Phase != "Failed" {
			t.Fatalf("three consecutive Running health failures did not fail execution: phase=%s", current.Status.Phase)
		}
	}
}

func TestRestoringHealthFailuresCountAfterStartupTimeout(t *testing.T) {
	r, _, w := fixture(t)
	prepareResumeFixture(t, r, w, "Restoring")
	ctx := context.Background()
	key := client.ObjectKeyFromObject(w)
	current := &api.ResumablePod{}
	if err := r.Get(ctx, key, current); err != nil {
		t.Fatal(err)
	}
	current.Spec.StartupTimeoutSeconds = 1
	if err := r.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	current.Status.Since = meta.Now()
	if err := r.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	h := &HealthChecker{Reconciler: r}
	local := runtimecontrol.New(r.Client)
	failures := make(map[types.NamespacedName]healthFailure)
	for range 2 {
		current = &api.ResumablePod{}
		if err := r.Get(ctx, key, current); err != nil {
			t.Fatal(err)
		}
		h.recordHealthResult(ctx, local, failures, healthResult{resource: current, err: node.ErrPortsNotListening})
		if _, exists := failures[key]; exists {
			t.Fatal("startup-grace failures were retained past the startup timeout")
		}
	}
	current = &api.ResumablePod{}
	if err := r.Get(ctx, key, current); err != nil {
		t.Fatal(err)
	}
	current.Status.Since = meta.NewTime(time.Now().Add(-2 * time.Second))
	if err := r.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	for count := 1; count <= 3; count++ {
		current = &api.ResumablePod{}
		if err := r.Get(ctx, key, current); err != nil {
			t.Fatal(err)
		}
		h.recordHealthResult(ctx, local, failures, healthResult{resource: current, err: node.ErrPortsNotListening})
		current = &api.ResumablePod{}
		if err := r.Get(ctx, key, current); err != nil {
			t.Fatal(err)
		}
		if count < 3 && current.Status.Phase != "Restoring" {
			t.Fatalf("startup timeout failed after only %d consecutive checks: phase=%s", count, current.Status.Phase)
		}
		if count == 3 && current.Status.Phase != "Failed" {
			t.Fatalf("startup timeout did not fail after three consecutive checks: phase=%s", current.Status.Phase)
		}
	}
}

func TestStaleHealthResultCannotOverwriteNewerTransition(t *testing.T) {
	r, _, w := fixture(t)
	ctx := context.Background()
	stale := &api.ResumablePod{}
	key := client.ObjectKeyFromObject(w)
	if err := r.Get(ctx, key, stale); err != nil {
		t.Fatal(err)
	}
	current := &api.ResumablePod{}
	if err := r.Get(ctx, key, current); err != nil {
		t.Fatal(err)
	}
	current.Spec.TransitionID = "resume-newer"
	if err := r.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	current.Status.Phase = "Restoring"
	current.Status.ObservedTransitionID = "resume-newer"
	if err := r.Status().Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	h := &HealthChecker{Reconciler: r, Namespace: w.Namespace}
	h.recordHealthResult(ctx, runtimecontrol.New(r.Client), map[types.NamespacedName]healthFailure{}, healthResult{resource: stale, err: node.ErrExecutionLost})
	actual := &api.ResumablePod{}
	if err := r.Get(ctx, key, actual); err != nil {
		t.Fatal(err)
	}
	if actual.Spec.TransitionID != "resume-newer" || actual.Status.Phase != "Restoring" || actual.Status.ObservedTransitionID != "resume-newer" {
		t.Fatalf("stale health result overwrote a newer transition: spec=%s status=%+v", actual.Spec.TransitionID, actual.Status)
	}
}
