package runtimecontrol

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func fixture(t *testing.T) (*Client, *api.ResumablePod) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{Name: "cellbox-test", Namespace: "test", UID: types.UID("uid")}, Spec: api.Spec{DesiredState: "Suspended", NodeName: "node"}, Status: api.Status{Phase: "Suspended"}}
	base := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.ResumablePod{}).WithObjects(w).Build()
	c := New(base)
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
	return c, w
}

func next(t *testing.T, ch <-chan Snapshot) Snapshot {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("missing direct snapshot")
		return Snapshot{}
	}
}

func queue(t *testing.T, c *Client) workqueue.TypedRateLimitingInterface[reconcile.Request] {
	t.Helper()
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(q.ShutDown)
	if err := c.Start(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	return q
}

func TestCommittedChangesWakeWorkersAndSlowSubscriberGetsLatest(t *testing.T) {
	c, w := fixture(t)
	key := client.ObjectKeyFromObject(w)
	ch, stop, err := c.Subscribe(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	_ = next(t, ch)
	// Commit before workers start: startup must not lose the request.
	w.Spec.DesiredState = "Running"
	if err = c.Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	q := queue(t, c)
	if q.Len() != 1 {
		t.Fatalf("pending request was lost: queue=%d", q.Len())
	}
	request, shutdown := q.Get()
	if shutdown || request.NamespacedName != key {
		t.Fatal("wrong request")
	}
	// Several transitions while this key is executing must neither block the
	// writer on a slow subscriber nor dispatch the same key concurrently.
	for i := 0; i < 20; i++ {
		w.Status.Cycle = int64(i + 1)
		w.Status.Phase = "Restoring"
		if err = c.Status().Update(t.Context(), w); err != nil {
			t.Fatal(err)
		}
	}
	if q.Len() != 0 {
		t.Fatal("same sandbox was queued concurrently")
	}
	q.Done(request)
	if q.Len() != 1 {
		t.Fatal("change during execution was lost")
	}
	event := next(t, ch)
	if event.Resource.Status.Cycle != 20 {
		t.Fatalf("slow consumer did not receive latest state: %+v", event)
	}
	// Mutating a delivered snapshot must not corrupt any durable state.
	event.Resource.Spec.DesiredState = "invalid"
	actual := &api.ResumablePod{}
	if err = c.Client.Get(t.Context(), key, actual); err != nil || actual.Spec.DesiredState != "Running" {
		t.Fatalf("snapshot alias: %v", err)
	}
}

type blockingRead struct {
	client.Client
	once          sync.Once
	read, release chan struct{}
}

func (b *blockingRead) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := b.Client.Get(ctx, key, obj, opts...)
	b.once.Do(func() {
		close(b.read)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	})
	return err
}

func TestSubscriptionReadCannotOverwriteConcurrentCompletion(t *testing.T) {
	c, w := fixture(t)
	blocked := &blockingRead{Client: c.Client, read: make(chan struct{}), release: make(chan struct{})}
	c.Client = blocked
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	type subscription struct {
		ch   <-chan Snapshot
		stop func()
		err  error
	}
	ready := make(chan subscription, 1)
	go func() {
		ch, stop, err := c.Subscribe(ctx, client.ObjectKeyFromObject(w))
		ready <- subscription{ch, stop, err}
	}()
	<-blocked.read
	w.Status.Phase = "Running"
	done := make(chan error, 1)
	go func() { done <- c.Status().Update(ctx, w) }()
	close(blocked.release)
	s := <-ready
	if s.err != nil {
		t.Fatal(s.err)
	}
	defer s.stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if event := next(t, s.ch); event.Resource.Status.Phase != "Running" {
		t.Fatalf("late initial state replaced completion: %+v", event)
	}
}

func TestConflictingStatusIsNotPublishedAndFinalizerRemovalPublishesDeletion(t *testing.T) {
	c, w := fixture(t)
	w.Finalizers = []string{api.Finalizer}
	if err := c.Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	ch, stop, err := c.Subscribe(t.Context(), client.ObjectKeyFromObject(w))
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	_ = next(t, ch)
	stale := w.DeepCopy()
	w.Status.Phase = "Restoring"
	if err = c.Status().Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	_ = next(t, ch)
	stale.Status.Phase = "Running"
	if err = c.Status().Update(t.Context(), stale); err == nil {
		t.Fatal("stale write succeeded")
	}
	select {
	case event := <-ch:
		t.Fatalf("uncommitted completion leaked: %+v", event)
	default:
	}
	if err = c.Delete(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	if err = c.Get(t.Context(), client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
	if event := next(t, ch); event.Deleted {
		t.Fatal("finalizer-held resource prematurely deleted")
	}
	w.Finalizers = nil
	if err = c.Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	if event := next(t, ch); !event.Deleted {
		t.Fatal("final deletion was not published")
	}
	if err = c.Get(t.Context(), client.ObjectKeyFromObject(w), &api.ResumablePod{}); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("finalizer removal did not delete: %v", err)
	}
}
