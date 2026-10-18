package v1alpha1

import (
	"testing"

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
