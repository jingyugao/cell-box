package service

import (
	"context"
	"net/http"
	"testing"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/catalog"
)

type catalogRuntimeProbe struct {
	boxprovider.Provider
	warmCalls    int
	runtimeCalls int
	inspectCalls int
}

func (p *catalogRuntimeProbe) Create(context.Context, boxprovider.Spec) (boxprovider.Handle, error) {
	p.runtimeCalls++
	return boxprovider.Handle{}, nil
}

func (p *catalogRuntimeProbe) Inspect(context.Context, boxprovider.Handle) (boxprovider.Observation, error) {
	p.runtimeCalls++
	p.inspectCalls++
	return boxprovider.Observation{}, nil
}

func (p *catalogRuntimeProbe) Action(context.Context, boxprovider.Handle, string) error {
	p.runtimeCalls++
	return nil
}

func (p *catalogRuntimeProbe) Destroy(context.Context, boxprovider.Handle) error {
	p.runtimeCalls++
	return nil
}

func (p *catalogRuntimeProbe) Guest(context.Context, boxprovider.Handle) (boxprovider.Connection, error) {
	p.runtimeCalls++
	return boxprovider.Connection{}, nil
}

func (p *catalogRuntimeProbe) CacheImage(context.Context, string, string, string) error {
	p.runtimeCalls++
	return nil
}

func (p *catalogRuntimeProbe) SetWarmTemplates(context.Context, string, []boxprovider.WarmTemplate) error {
	p.warmCalls++
	return nil
}

func buildCatalogRevision(t *testing.T, f *coreFixture, templateID, userVersion, baseImage, key string) catalog.Revision {
	t.Helper()
	status, body := f.call(t, "POST", "/v1/templates/"+templateID+"/revisions", testClientToken, key, map[string]any{
		"userVersion": userVersion, "baseImage": baseImage, "profileId": "profile-a",
	})
	wantStatus(t, status, http.StatusAccepted, body)
	op := decodeResponse[catalog.Operation](t, body)
	f.waitOperation(t, op.ID, "succeeded")
	status, body = f.call(t, "GET", "/v1/templates/"+templateID+"/revisions?userVersion="+userVersion, testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	revisions := decodeResponse[[]catalog.Revision](t, body)
	if len(revisions) == 0 {
		t.Fatal("build completed without a revision")
	}
	return revisions[0]
}

func TestCatalogRevisionsPinSandboxAndStableRetry(t *testing.T) {
	f := newCoreFixture(t)
	familyID := f.template(t)
	status, body := f.call(t, "GET", "/v1/templates/"+familyID, testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	family := decodeResponse[catalog.Template](t, body)
	status, body = f.call(t, "GET", "/v1/templates/"+familyID+"/revisions", testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	revisions := decodeResponse[[]catalog.Revision](t, body)
	if len(revisions) != 1 {
		t.Fatalf("expected initial revision, got %+v", revisions)
	}
	v1 := revisions[0]

	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "stable-once", map[string]string{"templateId": familyID, "ownerKey": "owner"})
	wantStatus(t, status, http.StatusAccepted, body)
	firstOp := decodeResponse[Operation](t, body)
	f.waitOperation(t, firstOp.ID, "succeeded")

	v2 := buildCatalogRevision(t, f, familyID, "1.01", "registry.example.test/base-v2:latest", "build-v2")
	status, body = f.call(t, "PATCH", "/v1/templates/"+familyID, testClientToken, "", map[string]any{
		"stableRevisionId": v2.ID, "expectedGeneration": family.Generation,
	})
	wantStatus(t, status, http.StatusOK, body)

	// A retry resolves to the original operation and therefore retains the
	// original stable revision even after the stable pointer moves.
	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "stable-once", map[string]string{"templateId": familyID, "ownerKey": "owner"})
	wantStatus(t, status, http.StatusAccepted, body)
	if got := decodeResponse[Operation](t, body); got.ID != firstOp.ID || got.TargetID != firstOp.TargetID {
		t.Fatalf("stable retry created a new operation: %+v", got)
	}
	box, err := f.service.rawBox(firstOp.TargetID)
	if err != nil {
		t.Fatal(err)
	}
	if box.Box.TemplateID != familyID || box.Box.RevisionID != v1.ID || box.Box.TemplateVersion != v1.Version {
		t.Fatalf("sandbox did not retain its concrete revision: %+v", box.Box)
	}

	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "stable-twice", map[string]string{"templateId": familyID, "ownerKey": "owner"})
	wantStatus(t, status, http.StatusAccepted, body)
	secondOp := decodeResponse[Operation](t, body)
	f.waitOperation(t, secondOp.ID, "succeeded")
	box, err = f.service.rawBox(secondOp.TargetID)
	if err != nil || box.Box.RevisionID != v2.ID {
		t.Fatalf("new sandbox did not resolve current stable: box=%+v err=%v", box.Box, err)
	}

	status, body = f.call(t, "DELETE", "/v1/templates/"+familyID, testClientToken, "", nil)
	wantStatus(t, status, http.StatusConflict, body)
	f.reopen(t)
	status, body = f.call(t, "GET", "/v1/templates/"+familyID, testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	if got := decodeResponse[catalog.Template](t, body); got.StableRevisionID != v2.ID {
		t.Fatalf("catalog state was not persisted: %+v", got)
	}
}

func TestTemplateManagementUsesCatalogWithoutRuntimeCalls(t *testing.T) {
	f := newCoreFixture(t)
	probe := &catalogRuntimeProbe{Provider: f.provider}
	f.service.providers["docker"] = probe
	f.provider.mu.Lock()
	beforeCreates := f.provider.createCalls
	f.provider.mu.Unlock()
	f.provider.cacheImage = func(context.Context, string, string, string) error {
		t.Fatal("template management called runtime image cache")
		return nil
	}
	familyID := f.template(t)
	status, body := f.call(t, "GET", "/v1/templates", testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	status, body = f.call(t, "GET", "/v1/templates/"+familyID+"/revisions", testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	revision := decodeResponse[[]catalog.Revision](t, body)[0]
	status, body = f.call(t, "PATCH", "/v1/template-revisions/"+revision.ID, testClientToken, "", map[string]any{
		"warmSlots": 3, "expectedGeneration": revision.Generation,
	})
	wantStatus(t, status, http.StatusOK, body)
	if got := decodeResponse[catalog.Revision](t, body); got.WarmSlots != 3 {
		t.Fatalf("warmSlots update was not persisted: %+v", got)
	}
	f.provider.mu.Lock()
	afterCreates := f.provider.createCalls
	f.provider.mu.Unlock()
	if afterCreates != beforeCreates {
		t.Fatalf("template management created runtime instances: before=%d after=%d", beforeCreates, afterCreates)
	}
	if probe.warmCalls != 0 || probe.runtimeCalls != 0 || probe.inspectCalls != 0 {
		t.Fatalf("template management touched runtime APIs: calls=%d inspect=%d warm=%d", probe.runtimeCalls, probe.inspectCalls, probe.warmCalls)
	}
}
