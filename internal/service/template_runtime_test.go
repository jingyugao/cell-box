package service

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/catalog"
	"cellbox.local/cellbox/internal/inventory"
)

type warmRuntimeProbe struct {
	boxprovider.Provider
	templates []boxprovider.WarmTemplate
	calls     int
	fail      bool
	rows      []inventory.Record
}

func (p *warmRuntimeProbe) SetWarmTemplates(_ context.Context, _ string, templates []boxprovider.WarmTemplate) error {
	p.calls++
	if p.fail {
		return errors.New("cluster unavailable")
	}
	p.templates = append([]boxprovider.WarmTemplate(nil), templates...)
	return nil
}
func (p *warmRuntimeProbe) ObserveWarmTemplates(_ context.Context, _ string, _ []string) ([]boxprovider.WarmPoolObservation, error) {
	if p.fail {
		return nil, errors.New("cluster unavailable")
	}
	pools := []boxprovider.WarmPoolObservation{}
	for _, t := range p.templates {
		pools = append(pools, boxprovider.WarmPoolObservation{PoolID: "pool-" + t.ID, TemplateIDs: []string{t.ID}, RequestedSlots: t.Slots, AllocatedSlots: t.Slots, ReadySlots: t.Slots, ObservedAt: time.Now().UTC()})
	}
	return pools, nil
}
func (p *warmRuntimeProbe) ListResources(context.Context, string, string) ([]inventory.Record, error) {
	if p.fail {
		return nil, errors.New("cluster unavailable")
	}
	return p.rows, nil
}

func TestTemplateWarmReconciliationAndCachedQueries(t *testing.T) {
	f := newCoreFixture(t)
	probe := &warmRuntimeProbe{Provider: f.provider}
	f.service.config.Profiles[0].Provider = "resumable-k8s-pod"
	f.service.config.Profiles[0].Namespace = "test"
	f.service.config.Profiles[0].NodeName = "node"
	f.service.providers["resumable-k8s-pod"] = probe
	// No boxes are ever created: building a revision is sufficient to publish demand.
	familyID := f.templateFor(t, f.service.config.Profiles[0], "warm-test", "1.01", "", 2)
	records, err := f.service.catalog.RuntimeRevisions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	v := records[0].Revision
	f.service.syncTemplateRuntime(context.Background())
	if len(probe.templates) != 1 || probe.templates[0].ID != v.ID || probe.templates[0].Slots != 2 || probe.templates[0].Spec.Image != v.Image {
		t.Fatalf("bad warm demand: %+v", probe.templates)
	}
	if created, _ := f.provider.counts(); created != 0 {
		t.Fatal("warm publication created a business sandbox")
	}
	calls := probe.calls
	status, body := f.call(t, "GET", "/v1/template-revisions/"+v.ID+"/warm-pool", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	state := decodeResponse[RevisionWarmStatus](t, body)
	if state.SyncStatus != "applied" || state.ObservedGeneration != v.Generation || state.Stale || state.Pool.ReadySlots != 2 {
		t.Fatalf("bad warm state: %+v", state)
	}
	status, body = f.call(t, "GET", "/v1/templates/"+familyID+"/runtime", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	runtime := decodeResponse[TemplateRuntimeStatus](t, body)
	if len(runtime.Revisions) != 1 || runtime.Revisions[0].SandboxCount == nil || *runtime.Revisions[0].SandboxCount != 0 || runtime.Revisions[0].Stale {
		t.Fatalf("bad usage: %+v", runtime)
	}
	if probe.calls != calls {
		t.Fatal("GET performed runtime reconciliation")
	}
	for _, path := range []string{"/v1/templates/" + familyID + "/runtime", "/v1/template-revisions/" + v.ID + "/warm-pool"} {
		status, body = f.call(t, "GET", path, otherClientToken, "", nil)
		wantStatus(t, status, 404, body)
	}
	// Metadata writes remain available during a cluster outage; reads report the
	// last snapshot as stale and must not treat its generation as current.
	probe.fail = true
	status, body = f.call(t, "PATCH", "/v1/template-revisions/"+v.ID, testClientToken, "", map[string]any{"warmSlots": 3, "expectedGeneration": v.Generation})
	wantStatus(t, status, 200, body)
	v = decodeResponse[catalog.Revision](t, body)
	state = f.service.revisionWarmStatus(v)
	if state.SyncStatus != "pending" || !state.Stale || state.DesiredSlots != 3 {
		t.Fatalf("generation not fenced: %+v", state)
	}
	f.service.syncTemplateRuntime(context.Background())
	state = f.service.revisionWarmStatus(v)
	if state.SyncStatus != "error" || !state.Stale || state.Pool == nil || !state.Pool.Stale || state.ObservedGeneration == v.Generation {
		t.Fatalf("failure hidden: %+v", state)
	}
	status, body = f.call(t, "GET", "/v1/templates/"+familyID+"/runtime", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	runtime = decodeResponse[TemplateRuntimeStatus](t, body)
	if !runtime.Revisions[0].Stale || runtime.Revisions[0].Error == "" {
		t.Fatal("inventory failure hidden")
	}
	probe.fail = false
	f.service.syncTemplateRuntime(context.Background())
	if probe.templates[0].Slots != 3 || f.service.revisionWarmStatus(v).SyncStatus != "applied" {
		t.Fatal("retry failed")
	}
	// Clearing stable and retiring withdraws the entire demand, including after
	// all revisions have zero effective demand.
	status, body = f.call(t, "GET", "/v1/templates/"+familyID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	family := decodeResponse[catalog.Template](t, body)
	status, body = f.call(t, "PATCH", "/v1/templates/"+familyID, testClientToken, "", map[string]any{"stableRevisionId": "", "expectedGeneration": family.Generation})
	wantStatus(t, status, 200, body)
	status, body = f.call(t, "PATCH", "/v1/template-revisions/"+v.ID, testClientToken, "", map[string]any{"status": "retired", "expectedGeneration": v.Generation})
	wantStatus(t, status, http.StatusOK, body)
	v = decodeResponse[catalog.Revision](t, body)
	f.service.syncTemplateRuntime(context.Background())
	if len(probe.templates) != 0 {
		t.Fatal("retired revision still published")
	}
	state = f.service.revisionWarmStatus(v)
	if state.DesiredSlots != 0 || state.SyncStatus != "applied" || state.Pool != nil || state.Stale {
		t.Fatalf("retire status: %+v", state)
	}
	// A process restart loses the observation cache and must replay durable
	// catalog desired state, including an empty withdrawal.
	f.service.templateRuntime = templateRuntimeCache{}
	f.service.syncTemplateRuntime(context.Background())
	if len(probe.templates) != 0 || f.service.revisionWarmStatus(v).SyncStatus != "applied" {
		t.Fatal("empty desired state was not replayed")
	}
}
