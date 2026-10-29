package resumable

import (
	"errors"
	"testing"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/runtimecontrol"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func commitRunning(t *testing.T, p *Provider, h boxprovider.Handle, pod, ip string) {
	t.Helper()
	ctx := t.Context()
	key := client.ObjectKey{Namespace: h.Namespace, Name: h.Name}
	w := &api.ResumablePod{}
	if err := p.Client.Get(ctx, key, w); err != nil {
		t.Fatal(err)
	}
	w.Spec.DesiredState, w.Spec.TransitionID = "Running", "t1"
	if err := p.Client.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := p.Client.Get(ctx, key, w); err != nil {
		t.Fatal(err)
	}
	w.Status = api.Status{Phase: "Running", ObservedGeneration: w.Generation, ObservedTransitionID: "t1", PodUID: pod,
		Execution: &api.Execution{PodUID: pod, ContainerID: "c1", PodIP: ip}}
	if err := p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
}

func TestCachedGuestRoutesOnlyToTheCommittedExecution(t *testing.T) {
	p, s, ctx := fixture(t)
	local := runtimecontrol.New(p.Client)
	p.Client, p.Local = local, local
	h, err := p.Create(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.CachedGuestForExecution(ctx, h, ""); !errors.Is(err, boxprovider.ErrNotReady) {
		t.Fatalf("nothing committed yet: %v", err)
	}
	commitRunning(t, p, h, "pod-1", "10.0.0.8")
	for _, expected := range []string{"", "pod-1"} {
		conn, err := p.CachedGuestForExecution(ctx, h, expected)
		if err != nil || conn.URL != "http://10.0.0.8:40000" {
			t.Fatalf("expected %q: %#v %v", expected, conn, err)
		}
	}
	if _, err = p.CachedGuestForExecution(ctx, h, "pod-0"); !errors.Is(err, boxprovider.ErrStaleExecution) {
		t.Fatalf("a request for a previous execution must be stale: %v", err)
	}
	other := h
	other.ID = "another-owner"
	if _, err = p.CachedGuestForExecution(ctx, other, ""); !errors.Is(err, boxprovider.ErrStaleExecution) {
		t.Fatalf("a handle for another incarnation must be stale: %v", err)
	}
	if err = p.Action(ctx, h, "suspend"); err != nil {
		t.Fatal(err)
	}
	if _, err = p.CachedGuestForExecution(ctx, h, "pod-1"); !errors.Is(err, boxprovider.ErrNotReady) {
		t.Fatalf("a suspending Box must not be routed: %v", err)
	}
}
