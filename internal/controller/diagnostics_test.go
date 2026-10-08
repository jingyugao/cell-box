package controller

import (
	"context"
	"errors"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

type recordingRuntime struct {
	*fakeRuntime
	captured bool
	err      error
}

func (f *recordingRuntime) CaptureFailure(_ context.Context, w *api.ResumablePod, p *core.Pod) error {
	if f.cleanups != 0 {
		return errors.New("cleanup occurred before evidence")
	}
	if w.Status.Message == "" || p == nil {
		return errors.New("missing failure details")
	}
	f.captured = true
	return f.err
}

func TestFailureEvidenceMustPersistBeforePodCleanup(t *testing.T) {
	r, runtime, w := fixture(t)
	f := &recordingRuntime{fakeRuntime: runtime, err: errors.New("disk unavailable")}
	r.Runtime = f
	key := types.NamespacedName{Namespace: w.Namespace, Name: w.Name}
	p := &core.Pod{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: w.Namespace, Name: w.Status.PodName}, p); err != nil {
		t.Fatal(err)
	}
	p.Status.Phase = core.PodFailed
	if err := r.Status().Update(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err == nil {
		t.Fatal("evidence write failure ignored")
	}
	if !f.captured || runtime.cleanups != 0 {
		t.Fatal("failure cleaned before evidence persisted")
	}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: w.Namespace, Name: w.Status.PodName}, p); err != nil {
		t.Fatal("Pod deleted before evidence persisted", err)
	}
	f.err = nil
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(context.Background(), key, w); err != nil {
		t.Fatal(err)
	}
	if w.Status.Phase != "Failing" {
		t.Fatalf("phase=%s", w.Status.Phase)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if runtime.cleanups == 0 {
		t.Fatal("persisted evidence prevented cleanup")
	}
}
