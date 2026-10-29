package resumable

import (
	"math"
	"testing"

	"github.com/jingyugao/cell-box/internal/boxprovider"
	core "k8s.io/api/core/v1"
)

func TestResourceLimitsAreOptInForColdAndWarmPods(t *testing.T) {
	hashes := map[string]string{}
	for _, tc := range []struct {
		name   string
		cpu    float64
		memory int64
		limits int
	}{
		{"omitted", 0, 0, 0},
		{"CPU only", 0.5, 0, 1},
		{"memory only", 0, 256, 1},
		{"both", 0.5, 256, 2},
		{"caller chosen values", 2048, 2097152, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, spec, _ := fixture(t)
			spec.CPU, spec.MemoryMiB = tc.cpu, tc.memory
			cold, err := buildWanted(spec)
			if err != nil {
				t.Fatal(err)
			}
			warm, hash, err := BuildWarmTemplates(spec.Namespace, []boxprovider.WarmTemplate{{ID: "revision-one", Slots: 1, Spec: spec}})
			if err != nil {
				t.Fatal(err)
			}
			if previous, exists := hashes[hash]; exists {
				t.Fatalf("%s shares a warm pool with %s despite different resource limits", tc.name, previous)
			}
			hashes[hash] = tc.name
			for kind, resources := range map[string]core.ResourceRequirements{"cold": cold.Spec.Container.Resources, "warm": warm[0].Spec.Container.Resources} {
				if len(resources.Limits) != tc.limits || resources.Limits.Cpu().MilliValue() != int64(tc.cpu*1000) || resources.Limits.Memory().Value() != tc.memory*1024*1024 {
					t.Fatalf("%s Pod limits differ from caller values: %#v", kind, resources.Limits)
				}
				for _, name := range []core.ResourceName{core.ResourceCPU, core.ResourceMemory} {
					request, exists := resources.Requests[name]
					if !exists || !request.IsZero() {
						t.Fatalf("%s Pod has nonzero or defaulted %s request", kind, name)
					}
				}
			}
		})
	}
}

func TestResourceLimitsRejectInvalidValuesAndUnitOverflow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cpu    float64
		memory int64
	}{
		{"negative CPU", -1, 0},
		{"NaN CPU", math.NaN(), 0},
		{"infinite CPU", math.Inf(1), 0},
		{"negative memory", 0, -1},
		{"CPU unit overflow", math.MaxFloat64, 0},
		{"memory unit overflow", 0, math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, spec, _ := fixture(t)
			spec.CPU, spec.MemoryMiB = tc.cpu, tc.memory
			if _, err := buildWanted(spec); err == nil {
				t.Fatal("invalid resource limit was accepted")
			}
		})
	}
}
