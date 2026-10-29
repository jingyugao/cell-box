package v1alpha1

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/jingyugao/cell-box/internal/guestapi"
	core "k8s.io/api/core/v1"
)

func TestSpecFingerprintPreservesLegacyEncodingAndIncludesVolumeUID(t *testing.T) {
	w := &ResumablePod{Spec: Spec{NodeName: "node", Container: core.Container{Image: "img"}}}
	const legacy = "6577f6e078d6ef7bcf1ca5cfea512fe58c7df5ddcbf12fe4904aabae7e1aaac6"
	if got := SpecFingerprint(w); got != legacy {
		t.Fatalf("legacy fingerprint changed: got %s, want %s", got, legacy)
	}
	w.Spec.PersistentHome = true
	withoutVolume := SpecFingerprint(w)
	w.Spec.HomeVolumeUID = "volume-one"
	if got := SpecFingerprint(w); got == withoutVolume {
		t.Fatal("fingerprint did not include HomeVolumeUID")
	}
}

func guestArgs(t *testing.T, instanceID string, env map[string]string) []string {
	t.Helper()
	config := guestapi.DefaultConfig()
	config.Env = map[string]string{}
	for k, v := range env {
		config.Env[k] = v
	}
	if instanceID != "" {
		config.Env[guestapi.InstanceIDEnv] = instanceID
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return []string{"serve", "--config-base64", base64.StdEncoding.EncodeToString(data)}
}

func guestPod(args []string) *ResumablePod {
	return &ResumablePod{Spec: Spec{NodeName: "node", Container: core.Container{Name: "guest", Image: "image@sha256:1", Args: args}}}
}

// Warm slots are shared between Boxes. Each Box's routing marker must not split
// the pool, while any other difference in the guest configuration still must.
func TestWarmFingerprintIgnoresOnlyTheInstanceMarker(t *testing.T) {
	a := guestPod(guestArgs(t, "guest-a", nil))
	b := guestPod(guestArgs(t, "guest-b", nil))
	other := guestPod(guestArgs(t, "guest-a", map[string]string{"EXTRA": "1"}))
	unmarked := guestPod(guestArgs(t, "", nil))
	if WarmSpecFingerprint(a) != WarmSpecFingerprint(b) {
		t.Fatal("different instance markers must share one warm pool")
	}
	if WarmSpecFingerprint(a) != WarmSpecFingerprint(unmarked) {
		t.Fatal("a warm template without a marker must match Boxes that have one")
	}
	if WarmSpecFingerprint(a) == WarmSpecFingerprint(other) {
		t.Fatal("a different guest environment must not share a warm pool")
	}
	if SpecFingerprint(a) == SpecFingerprint(b) {
		t.Fatal("the checkpoint fingerprint must keep Boxes apart")
	}
	a.Spec.HomeVolumeUID = "volume-1"
	if WarmSpecFingerprint(a) != WarmSpecFingerprint(b) {
		t.Fatal("the selected HOME volume must not split the warm pool")
	}
}
