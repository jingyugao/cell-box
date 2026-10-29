package controller

import (
	"context"
	"errors"
	"testing"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	core "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type guestResumeFakeRuntime struct {
	*fakeRuntime
	failure error
	calls   int
	needed  bool
}

func (f *guestResumeFakeRuntime) NeedsGuestUnquiesce(context.Context, *api.ResumablePod) (bool, error) {
	return f.needed, nil
}

func (f *guestResumeFakeRuntime) ResumeGuest(context.Context, *api.ResumablePod, *core.Pod) error {
	f.calls++
	if f.failure == nil {
		f.needed = false
	}
	return f.failure
}

func TestStartedPublishesRunningOnlyAfterGuestResumeAndCanRetry(t *testing.T) {
	r, runtime, w := fixture(t)
	ctx := context.Background()
	key := client.ObjectKeyFromObject(w)
	if err := r.Get(ctx, key, w); err != nil {
		t.Fatal(err)
	}
	w.Spec.TransitionID, w.Status.Phase = "resume-one", "Restoring"
	if err := r.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase = "Restoring"
	if err := r.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	p := &core.Pod{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: w.Namespace, Name: w.Status.PodName}, p); err != nil {
		t.Fatal(err)
	}
	node := &guestResumeFakeRuntime{fakeRuntime: runtime, failure: errors.New("Guest not reachable"), needed: true}
	r.Runtime = node
	if _, err := r.started(ctx, w, p); err == nil {
		t.Fatal("published success after Guest failure")
	}
	current := &api.ResumablePod{}
	if err := r.Get(ctx, key, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != "Restoring" || current.Status.ObservedTransitionID == "resume-one" {
		t.Fatal("Guest failure became terminal Running")
	}
	node.failure = nil
	if _, err := r.started(ctx, current, p); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, key, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != "Running" || current.Status.ObservedTransitionID != "resume-one" {
		t.Fatalf("incomplete result: %+v", current.Status)
	}
	if _, err := r.started(ctx, current, p); err != nil {
		t.Fatal(err)
	}
	if node.calls != 2 {
		t.Fatalf("completed node stage repeated: calls=%d", node.calls)
	}
}
