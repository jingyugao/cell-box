package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/catalog"
	"github.com/jingyugao/cell-box/internal/inventory"
)

type warmRuntimeProbe struct {
	boxprovider.Provider
	snapshot         boxprovider.WarmSnapshot
	rows             []inventory.Record
	observeCalls     int
	listCalls        int
	failWarmSnapshot bool
	failInventory    bool
}

func (p *warmRuntimeProbe) ObserveWarmTemplates(context.Context, string, []string) (boxprovider.WarmSnapshot, error) {
	p.observeCalls++
	if p.failWarmSnapshot {
		return boxprovider.WarmSnapshot{}, errors.New("warm snapshot unavailable")
	}
	return p.snapshot, nil
}

func (p *warmRuntimeProbe) ListResources(context.Context, string, string) ([]inventory.Record, error) {
	p.listCalls++
	if p.failInventory {
		return nil, errors.New("runtime inventory unavailable")
	}
	return append([]inventory.Record(nil), p.rows...), nil
}

func TestTemplateRuntimeReadsDesiredConfigAndUsagePerRequest(t *testing.T) {
	f := newCoreFixture(t)
	profile := f.service.config.Profiles[0]
	profile.Provider = "resumable-k8s-pod"
	profile.Namespace = "test"
	profile.NodeName = "node"
	f.service.config.Profiles[0] = profile
	probe := &warmRuntimeProbe{Provider: f.provider}
	f.service.providers[profile.Provider] = probe

	familyID := f.templateFor(t, profile, "warm-test", "1.01", "", 2)
	records, err := f.service.catalog.RuntimeRevisions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("runtime revisions=%d, want 1", len(records))
	}
	revision := records[0].Revision
	desired, err := f.service.DesiredWarmTemplates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(desired) != 1 || desired[0].ID != revision.ID || desired[0].Slots != 2 || desired[0].Spec.Image != revision.Image {
		t.Fatalf("desired warm configuration=%+v", desired)
	}
	desiredHash := WarmDesiredHash(desired)
	if desiredHash == "" || desiredHash != WarmDesiredHash(desired) {
		t.Fatalf("desired hash is empty or unstable: %q", desiredHash)
	}
	probe.snapshot = boxprovider.WarmSnapshot{
		DesiredHash: desiredHash,
		ObservedAt:  time.Now().UTC(),
		Pools: []boxprovider.WarmPoolObservation{{
			PoolID: "pool-" + revision.ID, TemplateIDs: []string{revision.ID},
			RequestedSlots: 2, AllocatedSlots: 2, ReadySlots: 2, ObservedAt: time.Now().UTC(),
		}},
	}

	status, body := f.call(t, "GET", "/v1/template-revisions/"+revision.ID+"/warm-pool", testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	warm := decodeResponse[RevisionWarmStatus](t, body)
	if warm.SyncStatus != "applied" || warm.ObservedGeneration != revision.Generation || warm.Stale || warm.Pool == nil || warm.Pool.ReadySlots != 2 {
		t.Fatalf("warm snapshot=%+v", warm)
	}

	path := "/v1/templates/" + familyID + "/runtime"
	status, body = f.call(t, "GET", path, testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	runtime := decodeResponse[TemplateRuntimeStatus](t, body)
	if len(runtime.Revisions) != 1 || runtime.Revisions[0].SandboxCount == nil || *runtime.Revisions[0].SandboxCount != 0 || runtime.Revisions[0].Stale {
		t.Fatalf("empty usage snapshot=%+v", runtime)
	}
	if probe.observeCalls != 2 || probe.listCalls != 1 {
		t.Fatalf("query did not perform one fresh warm and usage observation: warm=%d inventory=%d", probe.observeCalls, probe.listCalls)
	}

	// The next request must use the current inventory instead of a service cache.
	boxData, err := json.Marshal(Box{ID: "sandbox-1", RevisionID: revision.ID, Phase: "running"})
	if err != nil {
		t.Fatal(err)
	}
	probe.rows = []inventory.Record{{ClientID: "client-a", Box: boxData}}
	status, body = f.call(t, "GET", path, testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	runtime = decodeResponse[TemplateRuntimeStatus](t, body)
	if got := runtime.Revisions[0].SandboxCount; got == nil || *got != 1 || runtime.Revisions[0].Phases["running"] != 1 {
		t.Fatalf("updated usage was not observed: %+v", runtime.Revisions[0])
	}
	if probe.listCalls != 2 {
		t.Fatalf("inventory calls=%d, want a fresh read per request", probe.listCalls)
	}

	// Runtime snapshots are scoped to the client namespace.
	status, body = f.call(t, "GET", path, otherClientToken, "", nil)
	wantStatus(t, status, http.StatusNotFound, body)

	// Warm demand comes directly from the updated SQLite revision. The provider's
	// older observation is marked stale until its controller publishes the new hash.
	status, body = f.call(t, "PATCH", "/v1/template-revisions/"+revision.ID, testClientToken, "", map[string]any{
		"warmSlots": 3, "expectedGeneration": revision.Generation,
	})
	wantStatus(t, status, http.StatusOK, body)
	revision = decodeResponse[catalog.Revision](t, body)
	desired, err = f.service.DesiredWarmTemplates(context.Background())
	if err != nil || len(desired) != 1 || desired[0].Slots != 3 {
		t.Fatalf("SQLite desired state after update=%+v err=%v", desired, err)
	}
	newHash := WarmDesiredHash(desired)
	if newHash == desiredHash {
		t.Fatal("warm desired hash did not change after SQLite update")
	}
	status, body = f.call(t, "GET", "/v1/template-revisions/"+revision.ID+"/warm-pool", testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	warm = decodeResponse[RevisionWarmStatus](t, body)
	if !warm.Stale || warm.SyncStatus == "applied" || warm.ObservedGeneration == revision.Generation {
		t.Fatalf("old provider snapshot was reported current: %+v", warm)
	}

	probe.failWarmSnapshot = true
	status, body = f.call(t, "GET", "/v1/template-revisions/"+revision.ID+"/warm-pool", testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	warm = decodeResponse[RevisionWarmStatus](t, body)
	if !warm.Stale || warm.Error == "" {
		t.Fatalf("warm observation error was hidden: %+v", warm)
	}

	probe.failWarmSnapshot = false
	probe.failInventory = true
	status, body = f.call(t, "GET", path, testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	runtime = decodeResponse[TemplateRuntimeStatus](t, body)
	if !runtime.Revisions[0].Stale || runtime.Revisions[0].Error == "" || runtime.Revisions[0].SandboxCount != nil {
		t.Fatalf("inventory failure was hidden: %+v", runtime.Revisions[0])
	}
}
