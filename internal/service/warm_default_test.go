package service

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"cellbox.local/cellbox/internal/boxprovider"
)

type warmControlProvider struct {
	boxprovider.Provider
	target boxprovider.WarmDefaultImage
	calls  int
	err    error
}

func (p *warmControlProvider) SetWarmDefaultImage(_ context.Context, namespace string, target boxprovider.WarmDefaultImage) error {
	if namespace != "boxes" {
		return errors.New("wrong namespace")
	}
	if p.err != nil {
		return p.err
	}
	p.calls++
	p.target = target
	return nil
}
func (p *warmControlProvider) GetWarmDefaultImages(context.Context, string) ([]boxprovider.WarmDefaultImage, error) {
	return []boxprovider.WarmDefaultImage{p.target}, nil
}

func TestWarmDefaultAPISelectsOwnedDigestAndLeavesBoxesAndProfileUnchanged(t *testing.T) {
	f := newCoreFixture(t)
	p := &warmControlProvider{Provider: f.provider}
	f.service.providers["docker"] = p
	base := "registry.example/base@sha256:" + strings.Repeat("a", 64)
	selected := "registry.example/prepared@sha256:" + strings.Repeat("b", 64)
	f.service.config.Profiles[0].Image = base
	f.service.config.Profiles[0].NodeName = "node-a"
	f.service.config.Profiles[0].Namespace = "boxes"
	if err := f.service.store.Update(func(st *State) error {
		st.ImportedImages["owned"] = importedImageRecord{ClientID: "client-a", ImportedImage: ImportedImage{ID: "owned", Image: selected}}
		st.ImportedImages["foreign"] = importedImageRecord{ClientID: "client-b", ImportedImage: ImportedImage{ID: "foreign", Image: selected}}
		st.ImportedImages["deleting"] = importedImageRecord{ClientID: "client-a", ImportedImage: ImportedImage{ID: "deleting", Image: selected, Deleting: true}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	path := "/v1/profiles/profile-a/warm-default"
	status, body := f.call(t, "PUT", path, testClientToken, "", map[string]string{"importedImageId": "owned"})
	wantStatus(t, status, 200, body)
	if p.target.Image != selected || p.target.NodeName != "node-a" || p.target.ProfileID != "profile-a" {
		t.Fatalf("wrong warm target: %+v", p.target)
	}
	if f.service.config.Profiles[0].Image != base || f.provider.createCalls != 0 || f.provider.destroyCalls != 0 {
		t.Fatal("default switch changed base profile or existing execution")
	}
	f.service.config.ImageBuild = ImageBuildConfig{Repository: "registry.example/prepared"}
	status, body = f.call(t, "GET", "/v1/images/owned/usage", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	usage := decodeResponse[imageUsage](t, body)
	if usage.Deletable || !slices.Contains(usage.Blockers, "warm-default-reference-image") {
		t.Fatal("warm default was not protected from cleanup")
	}
	status, body = f.call(t, "DELETE", "/v1/images/owned", testClientToken, "delete-current-default", nil)
	wantStatus(t, status, 409, body)
	for _, id := range []string{"foreign", "missing", "deleting"} {
		status, body = f.call(t, "PUT", path, testClientToken, "", map[string]string{"importedImageId": id})
		if status == 200 {
			t.Fatalf("unavailable image %q was accepted: %s", id, body)
		}
	}
	if p.calls != 1 {
		t.Fatal("failed choices changed warm target")
	}
	status, body = f.call(t, "PUT", path, testClientToken, "", map[string]string{})
	wantStatus(t, status, 200, body)
	if p.target.Image != base {
		t.Fatal("system default did not restore base warm image")
	}
	p.err = errors.New("control unavailable")
	status, body = f.call(t, "PUT", path, testClientToken, "", map[string]string{"importedImageId": "owned"})
	if status == 200 || p.target.Image != base {
		t.Fatal("failed runtime publish silently changed target")
	}
}
