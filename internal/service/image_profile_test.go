package service

import (
	"reflect"
	"strings"
	"testing"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/guestapi"
)

func TestApplyImageConfigurationAutoTrustsExactImageAndClonesProfile(t *testing.T) {
	image := "registry.example.test/tooling@sha256:" + strings.Repeat("b", 64)
	profile := Profile{
		ID: "profile", Provider: "resumable-k8s-pod", Image: "registry.example.test/base@sha256:" + strings.Repeat("a", 64),
		AutoTrustImportedImages: true, SharedReadOnlyHostPath: "/opt/shared",
		Guest: guestapi.Config{Command: []string{"/bin/old"}, CommandDir: "/old", Env: map[string]string{"KEEP": "profile", "SHARED": "/opt/shared/tools"}, Tools: []guestapi.Tool{{ID: "tool", Executable: "/opt/cellbox/tools/tool"}}},
	}
	command := []string{"/bin/sh", "-c", "run"}
	env := map[string]string{"IMAGE": "value", "KEEP": "image"}
	got := applyImageConfiguration(profile, image, command, "/app", env)

	if got.Image != image || !reflect.DeepEqual(got.Guest.Command, command) || got.Guest.CommandDir != "/app" {
		t.Fatalf("image startup config was not applied: %+v", got)
	}
	if got.Guest.Env["IMAGE"] != "value" || got.Guest.Env["KEEP"] != "profile" || got.Guest.Env["SHARED"] != "/opt/shared/tools" {
		t.Fatalf("image/profile environment merge is wrong: %#v", got.Guest.Env)
	}
	if len(got.Guest.Tools) != 1 || !reflect.DeepEqual(got.TrustedToolImages, []string{image}) || !reflect.DeepEqual(got.MountedToolRuntimeImages, []string{image}) || !reflect.DeepEqual(got.DebugHomeImages, []string{image}) {
		t.Fatalf("exact image was not admitted for the auto-trust policy: %+v", got)
	}
	if profile.Image == image || len(profile.TrustedToolImages) != 0 || profile.Guest.Command[0] != "/bin/old" || profile.Guest.Env["KEEP"] != "profile" {
		t.Fatal("applying an image mutated the source profile")
	}
	command[0] = "/mutated"
	env["IMAGE"] = "mutated"
	if got.Guest.Command[0] != "/bin/sh" || got.Guest.Env["IMAGE"] != "value" {
		t.Fatal("output retained caller-owned slices or maps")
	}
}

func TestApplyImageConfigurationRemovesUntrustedToolAndHostMounts(t *testing.T) {
	profile := Profile{
		ID: "profile", Provider: "resumable-k8s-pod", Image: "registry.example.test/base@sha256:" + strings.Repeat("a", 64),
		SharedReadOnlyHostPath: "/opt/shared", DebugReadWriteHostPath: "/opt/shared/runtime/debug-homes",
		Guest: guestapi.Config{Env: map[string]string{"SHARED": api.SharedMountPath + "/tools"}, Tools: []guestapi.Tool{{ID: "tool", Executable: "/opt/cellbox/tools/tool"}}},
	}
	got := applyImageConfiguration(profile, "registry.example.test/other@sha256:"+strings.Repeat("c", 64), []string{"run"}, "/app", nil)
	if got.Guest.Tools != nil || got.SharedReadOnlyHostPath != "" || got.DebugReadOnlyHostPath != "" || got.DebugReadWriteHostPath != "" {
		t.Fatalf("untrusted image retained privileged tools or host paths: %+v", got)
	}
	if _, exists := got.Guest.Env["SHARED"]; exists {
		t.Fatalf("environment still points at a removed shared mount: %#v", got.Guest.Env)
	}
}
