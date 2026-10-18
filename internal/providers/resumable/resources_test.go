package resumable

import (
	"testing"

	"cellbox.local/cellbox/internal/boxprovider"
	core "k8s.io/api/core/v1"
)

func TestUnlimitedResourcesAppliesToColdAndWarmPodsOnly(t *testing.T) {
	_, spec, _ := fixture(t)
	normal, err := buildWanted(spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(normal.Spec.Container.Resources.Limits) != 2 {
		t.Fatalf("normal profile limits = %#v, want CPU and memory limits", normal.Spec.Container.Resources.Limits)
	}

	unlimitedSpec := spec
	unlimitedSpec.UnlimitedResources = true
	unlimited, err := buildWanted(unlimitedSpec)
	if err != nil {
		t.Fatal(err)
	}
	if len(unlimited.Spec.Container.Resources.Limits) != 0 {
		t.Fatalf("unlimited cold Pod limits = %#v, want none", unlimited.Spec.Container.Resources.Limits)
	}
	normalCPURequest := normal.Spec.Container.Resources.Requests[core.ResourceCPU]
	normalMemoryRequest := normal.Spec.Container.Resources.Requests[core.ResourceMemory]
	unlimitedCPURequest := unlimited.Spec.Container.Resources.Requests[core.ResourceCPU]
	unlimitedMemoryRequest := unlimited.Spec.Container.Resources.Requests[core.ResourceMemory]
	if !normalCPURequest.IsZero() || !normalMemoryRequest.IsZero() || !unlimitedCPURequest.IsZero() || !unlimitedMemoryRequest.IsZero() {
		t.Fatalf("requests changed: normal=%#v unlimited=%#v", normal.Spec.Container.Resources.Requests, unlimited.Spec.Container.Resources.Requests)
	}
	if normal.Spec.Container.Resources.Limits == nil || unlimited.Spec.Container.Resources.Limits != nil {
		t.Fatal("normal and unlimited container resource specs should remain distinct")
	}

	var normalHash, unlimitedHash string
	for _, tc := range []struct {
		name     string
		spec     boxprovider.Spec
		wantNone bool
	}{{"normal", spec, false}, {"unlimited", unlimitedSpec, true}} {
		t.Run(tc.name+" warm template", func(t *testing.T) {
			templates, hash, err := BuildWarmTemplates(spec.Namespace, []boxprovider.WarmTemplate{{ID: "revision-one", Slots: 1, Spec: tc.spec}})
			if err != nil {
				t.Fatal(err)
			}
			limits := templates[0].Spec.Container.Resources.Limits
			if tc.wantNone && len(limits) != 0 {
				t.Fatalf("unlimited warm Pod limits = %#v, want none", limits)
			}
			if !tc.wantNone && len(limits) != 2 {
				t.Fatalf("normal warm Pod limits = %#v, want CPU and memory limits", limits)
			}
			if tc.wantNone {
				unlimitedHash = hash
			} else {
				normalHash = hash
			}
		})
	}
	if normalHash == unlimitedHash {
		t.Fatalf("normal and unlimited warm templates share hash %q", normalHash)
	}

	invalid := unlimitedSpec
	invalid.CPU = 0
	if _, err := buildWanted(invalid); err == nil {
		t.Fatal("unlimited profile bypassed positive CPU validation")
	}
}
