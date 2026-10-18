package service

import (
	"strings"
	"testing"
)

// useK3sProfile adapts the common fake provider fixture to exercise the
// resumable Kubernetes profile paths. The fake provider itself remains the
// same; tests select it through the production provider key.
func useK3sProfile(t *testing.T, f *coreFixture) {
	t.Helper()
	for i := range f.config.Profiles {
		p := &f.config.Profiles[i]
		p.Provider = "resumable-k8s-pod"
		p.Namespace = "test"
		p.NodeName = "test-node"
		p.Image = "registry.example.test/agent@sha256:" + strings.Repeat("a", 64)
		p.PersistentHome = true
		p.Guest.Workspace = ""
	}
	if err := f.config.Validate(); err != nil {
		t.Fatal(err)
	}
	f.service.config.Profiles = append([]Profile(nil), f.config.Profiles...)
	f.service.providers["resumable-k8s-pod"] = f.provider
}
