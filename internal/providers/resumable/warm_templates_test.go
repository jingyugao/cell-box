// SPDX-License-Identifier: Apache-2.0

package resumable

import (
	"testing"
	"time"

	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/warmtemplates"
)

func TestBuildAndObserveWarmTemplatesWithoutKubernetesConfigMaps(t *testing.T) {
	_, spec, _ := fixture(t)
	spec.Namespace = "cellbox"
	spec.NodeName = "node-a"
	templates, hash, err := BuildWarmTemplates("cellbox", []boxprovider.WarmTemplate{{ID: "revision-one", Slots: 2, Spec: spec}})
	if err != nil {
		t.Fatal(err)
	}
	if len(templates) != 1 || templates[0].Spec.Container.Image != spec.Image || hash == "" {
		t.Fatalf("built templates = %#v hash=%q", templates, hash)
	}
	if _, _, err := BuildWarmTemplates("cellbox", []boxprovider.WarmTemplate{{ID: "revision-one", Slots: 1, Spec: spec}, {ID: "revision-one", Slots: 2, Spec: spec}}); err == nil {
		t.Fatal("duplicate revision was accepted")
	}
	provider := &Provider{}
	if _, err := provider.ObserveWarmTemplates(t.Context(), "cellbox", []string{"node-a"}); err == nil {
		t.Fatal("missing snapshot should remain unknown")
	}
	provider.SetWarmSnapshot(warmtemplates.Snapshot{NodeName: "node-a", DesiredHash: hash, ObservedAt: time.Now(), Pools: []boxprovider.WarmPoolObservation{{PoolID: "pool", ReadySlots: 1}}})
	observed, err := provider.ObserveWarmTemplates(t.Context(), "cellbox", []string{"node-a"})
	if err != nil || len(observed.Pools) != 1 || observed.Pools[0].Stale || observed.Pools[0].ReadySlots != 1 || observed.DesiredHash != hash {
		t.Fatalf("fresh observation = %#v err=%v", observed, err)
	}
	provider.SetWarmSnapshot(warmtemplates.Snapshot{NodeName: "node-a", DesiredHash: hash, ObservedAt: time.Now().Add(-time.Minute), Pools: []boxprovider.WarmPoolObservation{{PoolID: "pool"}}})
	observed, err = provider.ObserveWarmTemplates(t.Context(), "cellbox", []string{"node-a"})
	if err != nil || len(observed.Pools) != 1 || !observed.Pools[0].Stale {
		t.Fatalf("stale observation = %#v err=%v", observed, err)
	}
	provider.SetWarmSnapshot(warmtemplates.Snapshot{NodeName: "node-a", DesiredHash: "empty-config-hash", ObservedAt: time.Now()})
	observed, err = provider.ObserveWarmTemplates(t.Context(), "cellbox", []string{"node-a"})
	if err != nil || observed.DesiredHash != "empty-config-hash" || observed.ObservedAt.IsZero() || len(observed.Pools) != 0 {
		t.Fatalf("empty observed pool was not distinguishable from missing: %#v err=%v", observed, err)
	}
}

func TestPersistentHomeWarmTemplateHasNoBoxVolume(t *testing.T) {
	p, spec, ctx := fixture(t)
	spec.PersistentHome = true
	templates, _, err := BuildWarmTemplates(spec.Namespace, []boxprovider.WarmTemplate{{ID: "revision-home", Slots: 1, Spec: spec}})
	if err != nil {
		t.Fatal(err)
	}
	got := templates[0].Spec
	if !got.PersistentHome || got.HomeVolumeUID != "" || len(got.Mounts) != 0 {
		t.Fatalf("warm template must retain HOME mode without attaching a Box volume: %#v", got)
	}
	if _, err := p.Create(ctx, spec); err == nil {
		t.Fatal("created a persistent Box without an independent volume")
	}
}
