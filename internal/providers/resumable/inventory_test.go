package resumable

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/inventory"
	"context"
	"encoding/json"
	"fmt"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

type inventoryReadClient struct {
	client.Client
	lists    int
	gets     int
	podLists int
}

func (c *inventoryReadClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.lists++
	if _, ok := list.(*core.PodList); ok {
		c.podLists++
	}
	return c.Client.List(ctx, list, opts...)
}

func (c *inventoryReadClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.gets++
	return c.Client.Get(ctx, key, obj, opts...)
}

func TestResourceListReadsFiveRunningBoxesWithOneListAndNoPodReads(t *testing.T) {
	p, spec, ctx := fixture(t)
	spec.Staged = false
	for i := 0; i < 6; i++ {
		spec.BoxID = fmt.Sprintf("box-%d", i)
		clientID := "client-a"
		if i == 5 {
			clientID = "foreign"
		}
		spec.Inventory, _ = json.Marshal(inventory.Record{ClientID: clientID, Box: json.RawMessage(`{"id":"` + spec.BoxID + `"}`)})
		handle, err := p.Create(ctx, spec)
		if err != nil {
			t.Fatal(err)
		}
		w := &api.ResumablePod{}
		if err = p.Client.Get(ctx, client.ObjectKey{Namespace: handle.Namespace, Name: handle.Name}, w); err != nil {
			t.Fatal(err)
		}
		w.Status.Phase = "Running"
		w.Status.PodName = "missing-pod"
		w.Status.PodUID = "pod-uid"
		if err = p.Client.Status().Update(ctx, w); err != nil {
			t.Fatal(err)
		}
	}
	counts := &inventoryReadClient{Client: p.Client}
	p.Client = counts
	for round := 0; round < 2; round++ {
		rows, err := p.ListResources(ctx, spec.Namespace, "client-a")
		if err != nil || len(rows) != 5 {
			t.Fatalf("resource list: %v %+v", err, rows)
		}
		for _, row := range rows {
			var box map[string]any
			if err = json.Unmarshal(row.Box, &box); err != nil || box["phase"] != "running" {
				t.Fatalf("CR state not preserved: %v %+v", err, box)
			}
		}
		if counts.lists != round+1 || counts.gets != 0 || counts.podLists != 0 {
			t.Fatalf("unexpected reads: %+v", counts)
		}
	}
	rows, err := p.List(ctx, spec.Namespace, "client-a")
	if err != nil || len(rows) != 5 {
		t.Fatalf("verified list: %v %+v", err, rows)
	}
	if counts.lists != 4 || counts.podLists != 1 || counts.gets != 0 {
		t.Fatalf("default list no longer verifies Pods: %+v", counts)
	}
	var box map[string]any
	_ = json.Unmarshal(rows[0].Box, &box)
	if box["phase"] != "failed" {
		t.Fatal("default list failed to detect missing Pod")
	}
}

func TestListScopesClientAndRetainsAllLifecyclePhases(t *testing.T) {
	p, spec, ctx := fixture(t)
	record := inventory.Record{ClientID: "client-a", Box: json.RawMessage(`{"id":"` + spec.BoxID + `","createdAt":"2026-10-01T00:00:00Z"}`)}
	spec.Inventory, _ = json.Marshal(record)
	handle, err := p.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{}
	if err = p.Client.Get(ctx, client.ObjectKey{Namespace: handle.Namespace, Name: handle.Name}, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase = "Running"
	w.Status.Cycle = 2
	w.Status.PodName = "current"
	w.Status.PodUID = "pod-uid"
	if err = p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	yes := true
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "current", Namespace: w.Namespace, UID: types.UID("pod-uid"), Labels: map[string]string{api.OwnerLabel: string(w.UID)}, OwnerReferences: []meta.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: api.Kind, UID: w.UID, Controller: &yes}}}}
	if err = p.Client.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	rows, err := p.List(ctx, w.Namespace, "client-a")
	if err != nil || len(rows) != 1 {
		t.Fatalf("list: %v %+v", err, rows)
	}
	var box map[string]any
	_ = json.Unmarshal(rows[0].Box, &box)
	if box["phase"] != "running" || box["generation"] != float64(2) {
		t.Fatalf("wrong observed state: %+v", box)
	}
	rows, err = p.List(ctx, w.Namespace, "foreign")
	if err != nil || len(rows) != 0 {
		t.Fatal("foreign client workload exposed")
	}
	if err = p.Client.Get(ctx, client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
	w.Spec.DesiredState = "Suspended"
	if err = p.Client.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase = "Suspended"
	if err = p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	for external, expected := range map[string]string{"Creating": "creating", "Checkpointing": "checkpointing", "Suspending": "suspending", "Suspended": "suspended", "Restoring": "resuming", "Failed": "failed", "Deleting": "deleting"} {
		w.Status.Phase = external
		if err = p.Client.Status().Update(ctx, w); err != nil {
			t.Fatal(err)
		}
		rows, err = p.List(ctx, w.Namespace, "client-a")
		if err != nil || len(rows) != 1 {
			t.Fatalf("phase %s filtered: %v %+v", external, err, rows)
		}
		_ = json.Unmarshal(rows[0].Box, &box)
		if box["phase"] != expected {
			t.Fatalf("phase %s mapped to %v", external, box["phase"])
		}
	}
}
