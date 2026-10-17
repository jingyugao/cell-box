// SPDX-License-Identifier: Apache-2.0

package resumable

import (
	"testing"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/warmtemplates"
	core "k8s.io/api/core/v1"
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
