package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cellbox.local/cellbox/internal/boxprovider"
)

type templateWarmProvider struct {
	boxprovider.Provider
	targets []boxprovider.WarmTemplate
	err     error
}

func (p *templateWarmProvider) SetWarmTemplates(_ context.Context, _ string, targets []boxprovider.WarmTemplate) error {
	if p.err != nil {
		return p.err
	}
	p.targets = targets
	return nil
}

func TestTemplatePinsConfigurationAndPersistsAcrossRestart(t *testing.T) {
	f := newCoreFixture(t)
	input := createTemplateRequest{Name: "python", Version: "v1", ProfileID: "profile-a"}
	status, body := f.call(t, "POST", "/v1/templates", testClientToken, "", input)
	wantStatus(t, status, 201, body)
	first := decodeResponse[Template](t, body)
	original := f.service.config.Profiles[0].Image
	f.service.config.Profiles[0].Image = "sha256:" + strings.Repeat("b", 64)
	status, body = f.call(t, "POST", "/v1/templates", testClientToken, "", input)
	wantStatus(t, status, 200, body)
	if got := decodeResponse[Template](t, body); got.ID != first.ID || got.Image != original {
		t.Fatalf("retry changed template: %+v", got)
	}
	input.Version = "v2"
	status, body = f.call(t, "POST", "/v1/templates", testClientToken, "", input)
	wantStatus(t, status, 201, body)
	if got := decodeResponse[Template](t, body); got.ID == first.ID || got.Image == original {
		t.Fatal("new version did not capture new image")
	}
	f.reopen(t)
	status, body = f.call(t, "GET", "/v1/templates/"+first.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if got := decodeResponse[Template](t, body); got != first {
		t.Fatalf("restart changed template: %+v", got)
	}
	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "pinned-template", createRequest{TemplateID: first.ID, OwnerKey: "owner"})
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	f.waitOperation(t, op.ID, "succeeded")
	b, err := f.service.rawBox(op.TargetID)
	if err != nil {
		t.Fatal(err)
	}
	if b.Box.TemplateID != first.ID || b.Box.TemplateVersion != "v1" || b.Profile.Image != original {
		t.Fatalf("Box not pinned: %+v", b.Box)
	}
	status, body = f.call(t, "DELETE", "/v1/templates/"+first.ID, testClientToken, "", nil)
	wantStatus(t, status, 409, body)
	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "old-contract", map[string]string{"profileId": "profile-a", "ownerKey": "owner"})
	wantStatus(t, status, 400, body)
}

func TestTemplateOwnershipAndImmutableVersion(t *testing.T) {
	f := newCoreFixture(t)
	input := createTemplateRequest{Name: "agent", Version: "v1", ProfileID: "profile-a"}
	status, body := f.call(t, "POST", "/v1/templates", testClientToken, "", input)
	wantStatus(t, status, 201, body)
	template := decodeResponse[Template](t, body)
	input.ImportedImageID = "different-image"
	status, body = f.call(t, "POST", "/v1/templates", testClientToken, "", input)
	wantStatus(t, status, 409, body)
	for _, method := range []string{"GET", "DELETE"} {
		status, body = f.call(t, method, "/v1/templates/"+template.ID, otherClientToken, "", nil)
		wantStatus(t, status, 404, body)
	}
	status, body = f.call(t, "POST", "/v1/boxes", otherClientToken, "foreign-template", createRequest{TemplateID: template.ID, OwnerKey: "owner"})
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "GET", "/v1/templates", otherClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if len(decodeResponse[[]Template](t, body)) != 0 {
		t.Fatal("foreign template listed")
	}
	if err := f.service.store.Update(func(st *State) error {
		st.Archives["arc-a"] = archiveRecord{ClientID: "client-a", Archive: Archive{ID: "arc-a", TemplateID: template.ID}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	status, body = f.call(t, "DELETE", "/v1/templates/"+template.ID, testClientToken, "", nil)
	wantStatus(t, status, 409, body)
}

func TestTemplatePrewarmPublicationRetriesAndProtectsImageUntilWithdrawn(t *testing.T) {
	f := newCoreFixture(t)
	p := &templateWarmProvider{Provider: f.provider}
	f.service.providers["docker"] = p
	imageID := "img-" + strings.Repeat("1", 32)
	image := "registry.example/prepared@sha256:" + strings.Repeat("a", 64)
	if err := f.service.store.Update(func(st *State) error {
		st.ImportedImages[imageID] = importedImageRecord{ClientID: "client-a", ImportedImage: ImportedImage{ID: imageID, Image: image}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	input := createTemplateRequest{Name: "agent", Version: "v1", ProfileID: "profile-a", ImportedImageID: imageID, WarmSlots: 2}
	p.err = errors.New("publisher unavailable")
	status, body := f.call(t, "POST", "/v1/templates", testClientToken, "", input)
	wantStatus(t, status, 504, body)
	p.err = nil
	status, body = f.call(t, "POST", "/v1/templates", testClientToken, "", input)
	wantStatus(t, status, 200, body)
	template := decodeResponse[Template](t, body)
	if len(p.targets) != 1 || p.targets[0].Slots != 2 || p.targets[0].Spec.Image != image || p.targets[0].Spec.BoxID != template.ID {
		t.Fatalf("wrong targets: %+v", p.targets)
	}
	if f.provider.createCalls != 0 {
		t.Fatal("template publication created a user Box")
	}
	status, body = f.call(t, "PUT", "/v1/templates/"+template.ID+"/prewarm", testClientToken, "", map[string]int{"warmSlots": 0})
	wantStatus(t, status, 200, body)
	if len(p.targets) != 0 {
		t.Fatal("zero slots retained warm target")
	}
	p.err = errors.New("publisher unavailable")
	status, body = f.call(t, "DELETE", "/v1/templates/"+template.ID, testClientToken, "", nil)
	wantStatus(t, status, 504, body)
	status, body = f.call(t, "GET", "/v1/templates/"+template.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if !decodeResponse[Template](t, body).Deleting {
		t.Fatal("pending deletion not exposed")
	}
	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "deleting-template", createRequest{TemplateID: template.ID, OwnerKey: "owner"})
	wantStatus(t, status, 409, body)
	if err := f.service.store.View(func(st State) error {
		usage := f.service.imageUsage(st, imageID)
		for _, blocker := range usage.Blockers {
			if blocker == "templates-reference-image" {
				return nil
			}
		}
		return errors.New("deleting template no longer protects its image")
	}); err != nil {
		t.Fatal(err)
	}
	p.err = nil
	status, body = f.call(t, "DELETE", "/v1/templates/"+template.ID, testClientToken, "", nil)
	wantStatus(t, status, 204, body)
	status, body = f.call(t, "GET", "/v1/templates/"+template.ID, testClientToken, "", nil)
	wantStatus(t, status, 404, body)
}

func TestTemplateRecordStoreRoundTrip(t *testing.T) {
	// The sharded metadata store must persist templates even without any Box.
	objects := &ledgerObjects{data: map[string][]byte{}}
	store, err := OpenObjectStore(context.Background(), objects)
	if err != nil {
		t.Fatal(err)
	}
	expected := templateRecord{ClientID: "owner", RequestHash: "request", Template: Template{ID: "tpl-one", Name: "agent", Version: "v1", WarmSlots: 2}, Profile: Profile{ID: "default", Image: "fixed"}}
	if err = store.Update(func(st *State) error { st.Templates[expected.Template.ID] = expected; return nil }); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenObjectStore(context.Background(), objects)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.View(func(st State) error {
		got, ok := st.Templates[expected.Template.ID]
		if !ok || got.Template != expected.Template || got.Profile.Image != expected.Profile.Image || got.RequestHash != expected.RequestHash {
			t.Fatalf("template missing after reopen: %+v", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTemplateStagedInstancesShareOneWarmBudget(t *testing.T) {
	f := newCoreFixture(t)
	p := &templateWarmProvider{Provider: f.provider}
	f.service.providers["docker"] = p
	status, body := f.call(t, "POST", "/v1/templates", testClientToken, "", createTemplateRequest{Name: "agent", Version: "v1", ProfileID: "profile-a", WarmSlots: 2})
	wantStatus(t, status, 201, body)
	template := decodeResponse[Template](t, body)
	if len(p.targets) != 1 || p.targets[0].Spec.Staged || p.targets[0].Slots != 2 {
		t.Fatalf("empty template should prewarm ordinary launch: %+v", p.targets)
	}
	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "staged-warm", createRequest{TemplateID: template.ID, OwnerKey: "owner", Staged: true})
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	f.waitOperation(t, op.ID, "succeeded")
	if err := f.service.syncTemplates(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.targets) != 1 || !p.targets[0].Spec.Staged || p.targets[0].Slots != 2 {
		t.Fatalf("staged checkpoint would miss warm pool: %+v", p.targets)
	}
	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "ordinary-warm", createRequest{TemplateID: template.ID, OwnerKey: "owner"})
	wantStatus(t, status, 202, body)
	op = decodeResponse[Operation](t, body)
	f.waitOperation(t, op.ID, "succeeded")
	if err := f.service.syncTemplates(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.targets) != 2 || p.targets[0].Slots != 1 || p.targets[1].Slots != 1 || p.targets[0].Spec.Staged == p.targets[1].Spec.Staged {
		t.Fatalf("launch variants did not share budget: %+v", p.targets)
	}
}

func TestTemplateConfigurationRejectsMultipleNodes(t *testing.T) {
	c := Config{Profiles: []Profile{
		{ID: "a", Provider: "resumable-k8s-pod", NodeName: "node-a", Namespace: "boxes", Image: "registry.example/a@sha256:" + strings.Repeat("a", 64)},
		{ID: "b", Provider: "resumable-k8s-pod", NodeName: "node-b", Namespace: "boxes", Image: "registry.example/b@sha256:" + strings.Repeat("b", 64)},
	}}
	if err := c.Validate(); err == nil {
		t.Fatal("single-node service accepted multiple nodes")
	}
	c.Profiles[1].NodeName = "node-a"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

type templateUpgradeProvider struct {
	*fakeCoreProvider
}

func (p *templateUpgradeProvider) Upgrade(_ context.Context, h boxprovider.Handle, spec boxprovider.Spec, _ string) (boxprovider.Handle, error) {
	p.setExecutionID("template-upgrade-execution")
	h.ImageID = spec.Image
	return h, nil
}

func TestTemplateUpgradePinsVersionAndRejectsIncompatibleTarget(t *testing.T) {
	f := newCoreFixture(t)
	p := &templateUpgradeProvider{fakeCoreProvider: f.provider}
	f.service.providers["resumable-k8s-pod"] = p
	profile := f.service.config.Profiles[0]
	profile.Provider = "resumable-k8s-pod"
	profile.NodeName = "node-a"
	profile.Namespace = "boxes"
	profile.PersistentHome = true
	profile.Image = "registry.example/agent@sha256:" + strings.Repeat("a", 64)
	f.service.config.Profiles[0] = profile
	_, box := f.createBox(t, "upgrade-from-template")
	// An already suspended instance needs no quiesce call in this fake runtime.
	if err := f.service.store.Update(func(st *State) error {
		b := st.Boxes[box.ID]
		b.Box.Phase = "suspended"
		st.Boxes[box.ID] = b
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f.service.config.Profiles[0].Guest.Workspace = "/different-workspace"
	wrong := f.template(t)
	status, body := f.call(t, "POST", "/v1/boxes/"+box.ID+":upgrade", testClientToken, "wrong-template", map[string]string{"templateId": wrong})
	wantStatus(t, status, 409, body)
	before, err := f.service.rawBox(box.ID)
	if err != nil {
		t.Fatal(err)
	}
	if before.Box.TemplateID != box.TemplateID || before.Box.Phase != "suspended" || before.Box.OperationID != "" {
		t.Fatal("rejected upgrade changed the instance", before.Box)
	}
	f.service.config.Profiles[0] = profile
	f.service.config.Profiles[0].Image = "registry.example/agent@sha256:" + strings.Repeat("b", 64)
	f.service.config.Profiles[0].CPU = 2
	next := f.template(t)
	status, body = f.call(t, "POST", "/v1/boxes/"+box.ID+":upgrade", testClientToken, "upgrade-template", map[string]string{"templateId": next})
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	f.waitOperation(t, op.ID, "succeeded")
	after, err := f.service.rawBox(box.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Box.TemplateID != next || after.Profile.Image == profile.Image || after.Profile.CPU != 2 || after.Box.Phase != "staged" {
		t.Fatalf("upgrade did not use exact template: %+v", after.Box)
	}
	status, body = f.call(t, "DELETE", "/v1/templates/"+box.TemplateID, testClientToken, "", nil)
	wantStatus(t, status, 204, body)
}
