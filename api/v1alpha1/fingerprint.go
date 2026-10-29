package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/jingyugao/cell-box/internal/guestapi"

	core "k8s.io/api/core/v1"
)

// WarmSpecFingerprint omits the Box's volume selection and routing marker;
// neither changes the empty warm sandbox that will receive its checkpoint.
func WarmSpecFingerprint(w *ResumablePod) string {
	if w == nil {
		return ""
	}
	copy := w.DeepCopy()
	copy.Spec.HomeVolumeUID = ""
	copy.Spec.Mounts = nil
	copy.Spec.Container.Args = guestapi.WithoutInstanceID(copy.Spec.Container.Args)
	return SpecFingerprint(copy)
}

// SpecFingerprint hashes the immutable runtime profile. Its legacy encoding is
// intentionally stable: optional fields wrap the old encoding only when set.
func SpecFingerprint(w *ResumablePod) string {
	if w == nil {
		return ""
	}
	legacy := struct {
		Node      string
		Container core.Container
		Ports     []core.ServicePort
	}{w.Spec.NodeName, w.Spec.Container, w.Spec.ServicePorts}
	var b []byte
	if w.Spec.SharedReadOnlyHostPath != "" {
		b, _ = json.Marshal(struct {
			Node                   string
			Container              core.Container
			Ports                  []core.ServicePort
			SharedReadOnlyHostPath string
			DebugReadOnlyHostPath  string
			DebugReadWriteHostPath string
		}{legacy.Node, legacy.Container, legacy.Ports, w.Spec.SharedReadOnlyHostPath, w.Spec.DebugReadOnlyHostPath, w.Spec.DebugReadWriteHostPath})
	} else if w.Spec.DebugReadOnlyHostPath == "" && w.Spec.DebugReadWriteHostPath == "" {
		b, _ = json.Marshal(legacy)
	} else if w.Spec.DebugReadWriteHostPath == "" {
		b, _ = json.Marshal(struct {
			Node                  string
			Container             core.Container
			Ports                 []core.ServicePort
			DebugReadOnlyHostPath string
		}{legacy.Node, legacy.Container, legacy.Ports, w.Spec.DebugReadOnlyHostPath})
	} else {
		b, _ = json.Marshal(struct {
			Node                   string
			Container              core.Container
			Ports                  []core.ServicePort
			DebugReadWriteHostPath string
		}{legacy.Node, legacy.Container, legacy.Ports, w.Spec.DebugReadWriteHostPath})
	}
	if w.Spec.PersistentHome {
		b, _ = json.Marshal(struct {
			Legacy         json.RawMessage
			PersistentHome bool
		}{b, true})
	}
	if w.Spec.HomeVolumeUID != "" {
		b, _ = json.Marshal(struct {
			Legacy        json.RawMessage
			HomeVolumeUID string
		}{b, w.Spec.HomeVolumeUID})
	}
	if w.Spec.Mounts != nil {
		b, _ = json.Marshal(struct {
			Legacy json.RawMessage
			Mounts []VolumeMount
		}{b, w.Spec.Mounts})
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
