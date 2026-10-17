// SPDX-License-Identifier: Apache-2.0

package resumable

import (
	"encoding/json"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/warmtemplates"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestSetWarmTemplatesReplacesFullRuntimeTargets(t *testing.T) {
	p, spec, ctx := fixture(t)
	spec.PersistentHome = true // pooled roots must never bind a user's HOME.
	if err := p.SetWarmTemplates(ctx, spec.Namespace, []boxprovider.WarmTemplate{{ID: "tpl-one", Slots: 2, Spec: spec}}); err != nil {
		t.Fatal(err)
	}
	got, err := warmtemplates.Read(ctx, p.Client, spec.Namespace)
	if err != nil || len(got) != 1 || got[0].ID != "tpl-one" || got[0].Slots != 2 || got[0].Spec.NodeName != spec.NodeName {
		t.Fatalf("warm template write failed: %#v err=%v", got, err)
	}
	if !got[0].Spec.PersistentHome {
		t.Fatal("template identity lost the persistentHome setting used by resume compatibility")
	}
	if err = p.SetWarmTemplates(ctx, spec.Namespace, []boxprovider.WarmTemplate{{ID: "tpl-two", Slots: 1, Spec: spec}}); err != nil {
		t.Fatal(err)
	}
	got, err = warmtemplates.Read(ctx, p.Client, spec.Namespace)
	if err != nil || len(got) != 1 || got[0].ID != "tpl-two" {
		t.Fatalf("full sync did not remove deleted templates: %#v err=%v", got, err)
	}
	if err = p.SetWarmTemplates(ctx, spec.Namespace, nil); err != nil {
		t.Fatal(err)
	}
	got, err = warmtemplates.Read(ctx, p.Client, spec.Namespace)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty sync did not disable warming: %#v err=%v", got, err)
	}
	cm := &core.ConfigMap{}
	if err = p.Client.Get(ctx, types.NamespacedName{Namespace: spec.Namespace, Name: warmtemplates.ConfigMapName}, cm); err != nil || len(cm.Data) != 0 {
		t.Fatalf("expected empty authoritative ConfigMap, got %#v err=%v", cm.Data, err)
	}
}

func TestSetWarmTemplatesRejectsInvalidTargets(t *testing.T) {
	p, spec, ctx := fixture(t)
	for _, tc := range []struct {
		name string
		tpl  boxprovider.WarmTemplate
	}{
		{name: "bad ID", tpl: boxprovider.WarmTemplate{ID: "bad/id", Slots: 1, Spec: spec}},
		{name: "negative slots", tpl: boxprovider.WarmTemplate{ID: "tpl-one", Slots: -1, Spec: spec}},
		{name: "namespace mismatch", tpl: boxprovider.WarmTemplate{ID: "tpl-one", Slots: 1, Spec: func() boxprovider.Spec { s := spec; s.Namespace = "elsewhere"; return s }()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := p.SetWarmTemplates(ctx, spec.Namespace, []boxprovider.WarmTemplate{tc.tpl}); err == nil {
				t.Fatal("invalid target accepted")
			}
		})
	}
}

func TestObserveWarmTemplatesJoinsNodeSnapshotAndMarksStale(t *testing.T) {
	p, spec, ctx := fixture(t)
	if err := p.SetWarmTemplates(ctx, spec.Namespace, []boxprovider.WarmTemplate{{ID: "rev-one", Slots: 2, Spec: spec}}); err != nil {
		t.Fatal(err)
	}
	desired := &core.ConfigMap{}
	if err := p.Client.Get(ctx, types.NamespacedName{Namespace: spec.Namespace, Name: warmtemplates.ConfigMapName}, desired); err != nil {
		t.Fatal(err)
	}
	pool := boxprovider.WarmPoolObservation{PoolID: "pool-1", TemplateIDs: []string{"rev-one"}, DesiredHash: warmtemplates.DesiredHash(desired.Data), ObservedAt: time.Now().UTC(), RequestedSlots: 2, AllocatedSlots: 1, ReadySlots: 1}
	snapshot := warmtemplates.Snapshot{NodeName: spec.NodeName, DesiredHash: warmtemplates.DesiredHash(desired.Data), ObservedGeneration: desired.Generation, ObservedAt: time.Now().UTC(), Pools: []boxprovider.WarmPoolObservation{pool}}
	data, err := warmtemplates.EncodeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	cm := &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Namespace: spec.Namespace, Name: warmtemplates.StatusConfigMapName(spec.NodeName)}, Data: data}
	if err := p.Client.Create(ctx, cm); err != nil {
		t.Fatal(err)
	}
	got, err := p.ObserveWarmTemplates(ctx, spec.Namespace, []string{spec.NodeName})
	if err != nil || len(got) != 1 || got[0].Stale || got[0].RequestedSlots != 2 || got[0].ReadySlots != 1 {
		t.Fatalf("matching observation = %#v, err=%v", got, err)
	}
	if err := p.SetWarmTemplates(ctx, spec.Namespace, []boxprovider.WarmTemplate{{ID: "rev-one", Slots: 3, Spec: spec}}); err != nil {
		t.Fatal(err)
	}
	got, err = p.ObserveWarmTemplates(ctx, spec.Namespace, []string{spec.NodeName})
	if err != nil || len(got) != 1 || !got[0].Stale {
		t.Fatalf("stale observation = %#v, err=%v", got, err)
	}
	if got[0].ReadySlots != 1 {
		t.Fatal("stale snapshot counters were replaced with synthetic zeroes")
	}
}

func TestObserveWarmTemplatesDoesNotInventZerosWithoutSnapshot(t *testing.T) {
	p, spec, ctx := fixture(t)
	if err := p.SetWarmTemplates(ctx, spec.Namespace, []boxprovider.WarmTemplate{{ID: "rev-one", Slots: 0, Spec: spec}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ObserveWarmTemplates(ctx, spec.Namespace, []string{spec.NodeName}); err == nil {
		t.Fatal("missing controller snapshot reported as a known empty pool")
	}
}

func TestObserveWarmTemplatesRequiresFreshEmptySnapshotAfterWithdrawal(t *testing.T) {
	p, spec, ctx := fixture(t)
	if err := p.SetWarmTemplates(ctx, spec.Namespace, nil); err != nil {
		t.Fatal(err)
	}
	desired := &core.ConfigMap{}
	if err := p.Client.Get(ctx, types.NamespacedName{Namespace: spec.Namespace, Name: warmtemplates.ConfigMapName}, desired); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		hash    string
		wantErr bool
	}{
		{name: "fresh acknowledged empty demand", hash: warmtemplates.DesiredHash(desired.Data)},
		{name: "stale empty demand", hash: "old-desired-hash", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := warmtemplates.StatusConfigMapName(spec.NodeName)
			_ = p.Client.Delete(ctx, &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Namespace: spec.Namespace, Name: name}})
			data, err := warmtemplates.EncodeSnapshot(warmtemplates.Snapshot{NodeName: spec.NodeName, DesiredHash: tc.hash, ObservedGeneration: desired.Generation, ObservedAt: time.Now().UTC(), Pools: []boxprovider.WarmPoolObservation{}})
			if err != nil {
				t.Fatal(err)
			}
			if err = p.Client.Create(ctx, &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Namespace: spec.Namespace, Name: name}, Data: data}); err != nil {
				t.Fatal(err)
			}
			got, err := p.ObserveWarmTemplates(ctx, spec.Namespace, []string{spec.NodeName})
			if tc.wantErr && err == nil {
				t.Fatalf("stale empty snapshot accepted: %#v", got)
			}
			if !tc.wantErr && (err != nil || len(got) != 0) {
				t.Fatalf("fresh empty snapshot = %#v, err=%v", got, err)
			}
		})
	}
}

func TestWarmSnapshotWireShape(t *testing.T) {
	data, err := warmtemplates.EncodeSnapshot(warmtemplates.Snapshot{NodeName: "node-a", DesiredHash: "hash", Pools: []boxprovider.WarmPoolObservation{{PoolID: "pool", TemplateIDs: []string{"rev"}, Resources: boxprovider.WarmPoolResources{CPURequestMilli: 500}}}})
	if err != nil {
		t.Fatal(err)
	}
	var got warmtemplates.Snapshot
	if err = json.Unmarshal([]byte(data["snapshot.json"]), &got); err != nil || got.Pools[0].Resources.CPURequestMilli != 500 {
		t.Fatalf("wire snapshot = %#v, err=%v", got, err)
	}
}
