package runtimecontrol

import (
	"testing"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func committedRunning(uid, pod, ip string) *api.ResumablePod {
	return &api.ResumablePod{
		ObjectMeta: meta.ObjectMeta{Name: "box", Namespace: "ns", UID: types.UID(uid), Generation: 3,
			Labels: map[string]string{"cellbox.jingyugao.github.io/box-id": "box-1"}},
		Spec: api.Spec{DesiredState: "Running", TransitionID: "t1", NodeName: "node"},
		Status: api.Status{Phase: "Running", ObservedGeneration: 3, ObservedTransitionID: "t1", PodUID: pod, Cycle: 2,
			Execution: &api.Execution{PodUID: pod, ContainerID: "c1", PodIP: ip}},
	}
}

func project(c *Client, key types.NamespacedName, w *api.ResumablePod, deleted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.projectEndpoint(key, w, deleted)
}

// The cache may only name an address for an execution the controller has fully
// committed. Anything less must drop the entry so no request is routed by it.
func TestEndpointIsCachedOnlyForACommittedRunningExecution(t *testing.T) {
	c := &Client{}
	key := types.NamespacedName{Namespace: "ns", Name: "box"}
	good := committedRunning("owner", "pod-1", "10.0.0.8")
	project(c, key, good, false)
	got, ok := c.GuestEndpoint(key)
	if !ok || got.IP != "10.0.0.8" || got.PodUID != "pod-1" || got.OwnerUID != "owner" || got.BoxID != "box-1" || got.NodeName != "node" {
		t.Fatalf("committed execution not cached: %#v %v", got, ok)
	}
	for name, mutate := range map[string]func(*api.ResumablePod){
		"suspending":             func(w *api.ResumablePod) { w.Spec.DesiredState = "Suspended" },
		"not running":            func(w *api.ResumablePod) { w.Status.Phase = "Restoring" },
		"generation not seen":    func(w *api.ResumablePod) { w.Status.ObservedGeneration = 2 },
		"transition not seen":    func(w *api.ResumablePod) { w.Status.ObservedTransitionID = "t0" },
		"no execution":           func(w *api.ResumablePod) { w.Status.Execution = nil },
		"execution of other pod": func(w *api.ResumablePod) { w.Status.Execution.PodUID = "pod-2" },
		"invalid address":        func(w *api.ResumablePod) { w.Status.Execution.PodIP = "not-an-ip" },
		"being deleted":          func(w *api.ResumablePod) { now := meta.Now(); w.DeletionTimestamp = &now },
	} {
		project(c, key, good, false)
		w := good.DeepCopy()
		mutate(w)
		project(c, key, w, false)
		if _, ok := c.GuestEndpoint(key); ok {
			t.Fatalf("%s: stale endpoint kept", name)
		}
	}
}

// A late deletion event for a previous incarnation of the name must not remove
// the endpoint of the Box that now owns it.
func TestDeletionOfAnOlderResourceKeepsTheNewOwnersEndpoint(t *testing.T) {
	c := &Client{}
	key := types.NamespacedName{Namespace: "ns", Name: "box"}
	project(c, key, committedRunning("new-owner", "pod-2", "10.0.0.9"), false)
	project(c, key, committedRunning("old-owner", "pod-1", "10.0.0.8"), true)
	if got, ok := c.GuestEndpoint(key); !ok || got.OwnerUID != "new-owner" {
		t.Fatalf("new owner's endpoint was removed: %#v %v", got, ok)
	}
	project(c, key, committedRunning("new-owner", "pod-2", "10.0.0.9"), true)
	if _, ok := c.GuestEndpoint(key); ok {
		t.Fatal("deleting the current owner must remove its endpoint")
	}
}
