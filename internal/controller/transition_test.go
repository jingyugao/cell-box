package controller

import (
	"context"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestRunningControllerConfirmsObservedTransitionID(t *testing.T) {
	r, _, w := fixture(t)
	ctx := context.Background()
	key := client.ObjectKeyFromObject(w)
	if err := r.Get(ctx, key, w); err != nil {
		t.Fatal(err)
	}
	w.Spec.TransitionID = "resume-transition"
	if err := r.Update(ctx, w); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	current := &api.ResumablePod{}
	if err := r.Get(ctx, key, current); err != nil {
		t.Fatal(err)
	}
	if current.Spec.TransitionID != "resume-transition" {
		t.Fatalf("desired transition changed unexpectedly: %q", current.Spec.TransitionID)
	}
	if current.Status.ObservedTransitionID != current.Spec.TransitionID {
		t.Fatalf("controller did not confirm transition: observed=%q desired=%q", current.Status.ObservedTransitionID, current.Spec.TransitionID)
	}
}
