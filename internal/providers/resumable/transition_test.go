package resumable

import (
	"context"
	"errors"
	"testing"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/boxprovider"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type transitionPatchCounter struct {
	client.Client
	patches int
}

func (c *transitionPatchCounter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if _, ok := obj.(*api.ResumablePod); ok {
		c.patches++
	}
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func TestActionTransitionSameTokenReplayDoesNotPatchAgain(t *testing.T) {
	p, spec, ctx := fixture(t)
	h, err := p.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{}
	if err := p.Client.Get(ctx, client.ObjectKeyFromObject(&api.ResumablePod{ObjectMeta: meta.ObjectMeta{Name: h.Name, Namespace: h.Namespace}}), w); err != nil {
		t.Fatal(err)
	}
	w.Spec.DesiredState = "Suspended"
	w.Spec.TransitionID = "previous-transition"
	w.Status.Phase = "Suspended"
	w.Status.ObservedTransitionID = "previous-transition"
	if err := p.Client.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase = "Suspended"
	w.Status.ObservedTransitionID = "previous-transition"
	if err := p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	counter := &transitionPatchCounter{Client: p.Client}
	p.Client = counter

	if err := p.ActionTransition(ctx, h, "resume", "previous-transition", "resume-transition"); err != nil {
		t.Fatalf("first transition failed: %v", err)
	}
	if counter.patches != 1 {
		t.Fatalf("first transition patches = %d, want 1", counter.patches)
	}
	if err := p.ActionTransition(ctx, h, "resume", "previous-transition", "resume-transition"); err != nil {
		t.Fatalf("same-token replay failed: %v", err)
	}
	if counter.patches != 1 {
		t.Fatalf("same-token replay added a patch: patches = %d, want 1", counter.patches)
	}
	current := &api.ResumablePod{}
	if err := p.Client.Get(ctx, client.ObjectKeyFromObject(w), current); err != nil {
		t.Fatal(err)
	}
	if current.Spec.DesiredState != "Running" || current.Spec.TransitionID != "resume-transition" {
		t.Fatalf("transition was not durably recorded: desired=%q token=%q", current.Spec.DesiredState, current.Spec.TransitionID)
	}
}

func TestActionTransitionRejectsStaleOrUnobservedPreviousToken(t *testing.T) {
	tests := []struct {
		name               string
		expected, observed string
	}{
		{name: "stale previous token", expected: "older-transition", observed: "current-transition"},
		{name: "unfinished previous token", expected: "current-transition", observed: "older-transition"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, spec, ctx := fixture(t)
			h, err := p.Create(ctx, spec)
			if err != nil {
				t.Fatal(err)
			}
			w := &api.ResumablePod{}
			if err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
				t.Fatal(err)
			}
			w.Spec.DesiredState = "Suspended"
			w.Spec.TransitionID = "current-transition"
			w.Status.Phase = "Suspended"
			w.Status.ObservedTransitionID = tt.observed
			if err := p.Client.Update(ctx, w); err != nil {
				t.Fatal(err)
			}
			if err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
				t.Fatal(err)
			}
			w.Status.Phase = "Suspended"
			w.Status.ObservedTransitionID = tt.observed
			if err := p.Client.Status().Update(ctx, w); err != nil {
				t.Fatal(err)
			}
			counter := &transitionPatchCounter{Client: p.Client}
			p.Client = counter

			err = p.ActionTransition(ctx, h, "resume", tt.expected, "next-transition")
			if !errors.Is(err, boxprovider.ErrStaleTransition) {
				t.Fatalf("ActionTransition error = %v, want ErrStaleTransition", err)
			}
			if counter.patches != 0 {
				t.Fatalf("rejected stale transition patched %d times", counter.patches)
			}
		})
	}
}
