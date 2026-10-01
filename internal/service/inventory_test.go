package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/inventory"
	"context"
	"encoding/json"
	"testing"
)

type runtimeInventory struct {
	boxprovider.Provider
	rows  []inventory.Record
	calls int
}

type resourceRuntimeInventory struct {
	runtimeInventory
	resourceCalls int
}

func (p *resourceRuntimeInventory) ListResources(_ context.Context, _, _ string) ([]inventory.Record, error) {
	p.resourceCalls++
	return p.rows, nil
}

func TestResourceInventoryUsesExplicitCapabilityAndPreservesIDFiltering(t *testing.T) {
	f := newCoreFixture(t)
	p := &resourceRuntimeInventory{runtimeInventory: runtimeInventory{Provider: f.provider, rows: []inventory.Record{
		{Box: mustBoxJSON(Box{ID: "one", Phase: "running"})}, {Box: mustBoxJSON(Box{ID: "two", Phase: "suspended"})},
	}}}
	f.service.providers["docker"] = p
	status, body := f.call(t, "GET", "/v1/boxes?observation=resource&id=one", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	rows := decodeResponse[[]Box](t, body)
	if len(rows) != 1 || rows[0].ID != "one" || p.resourceCalls != 1 || p.calls != 0 {
		t.Fatalf("wrong query path: %s", body)
	}
	status, body = f.call(t, "GET", "/v1/boxes?observation=resource&id=missing", testClientToken, "", nil)
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "GET", "/v1/boxes?observation=invalid", testClientToken, "", nil)
	wantStatus(t, status, 400, body)
	f.service.providers["docker"] = &p.runtimeInventory
	status, body = f.call(t, "GET", "/v1/boxes?observation=resource", testClientToken, "", nil)
	wantStatus(t, status, 422, body)
	if p.calls != 0 {
		t.Fatal("resource query fell back to verified inventory")
	}
}

func (p *runtimeInventory) List(_ context.Context, _, _ string) ([]inventory.Record, error) {
	p.calls++
	return p.rows, nil
}
func TestInventoryListsUseExternalSources(t *testing.T) {
	f := newCoreFixture(t)
	objects := &ledgerObjects{}
	f.service.objects = objects
	f.service.store.objects = objects
	// Objects exist outside the mutation cache and must be visible immediately.
	id := "img-11111111111111111111111111111111"
	data, _ := json.Marshal(storedImage{Revision: "external", Record: &importedImageRecord{ClientID: "client-a", ImportedImage: ImportedImage{ID: id, Source: "oss-only"}}})
	objects.data = map[string][]byte{"images/" + id + "/metadata.json": data}
	status, body := f.call(t, "GET", "/v1/images", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	images := decodeResponse[[]ImportedImage](t, body)
	if len(images) != 1 || images[0].Source != "oss-only" {
		t.Fatalf("memory used instead of OSS: %s", body)
	}
	row := inventory.Record{ClientID: "client-a", RuntimeID: "runtime-only", Snapshot: "checkpoint-1", Box: mustBoxJSON(Box{ID: "box-external", Phase: "suspended"})}
	data, _ = json.Marshal(row)
	objects.data["checkpoints/runtime-only/metadata.json"] = data
	status, body = f.call(t, "GET", "/v1/checkpoints", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if rows := decodeResponse[[]Box](t, body); len(rows) != 1 || rows[0].ID != "box-external" {
		t.Fatalf("checkpoint OSS enumeration failed: %s", body)
	}
	for _, phase := range []string{"checkpointing", "suspending", "resuming", "restoring", "failed", "deleting"} {
		row.Box = mustBoxJSON(Box{ID: "box-external", Phase: phase})
		objects.data["checkpoints/runtime-only/metadata.json"], _ = json.Marshal(row)
		status, body = f.call(t, "GET", "/v1/checkpoints", testClientToken, "", nil)
		wantStatus(t, status, 200, body)
		rows := decodeResponse[[]Box](t, body)
		if len(rows) != 1 || rows[0].Phase != phase {
			t.Fatalf("OSS phase %s hidden: %s", phase, body)
		}
	}
	status, body = f.call(t, "GET", "/v1/checkpoints", otherClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if len(decodeResponse[[]Box](t, body)) != 0 {
		t.Fatal("foreign checkpoint leaked")
	}
	provider := &runtimeInventory{Provider: f.provider, rows: []inventory.Record{{Box: mustBoxJSON(Box{ID: "box-runtime-only", Phase: "running"})}, {Box: mustBoxJSON(Box{ID: "box-other", Phase: "creating"})}}}
	f.service.providers["docker"] = provider
	status, body = f.call(t, "GET", "/v1/boxes?id=box-runtime-only", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if rows := decodeResponse[[]Box](t, body); len(rows) != 1 || rows[0].ID != "box-runtime-only" || provider.calls != 1 {
		t.Fatalf("runtime list/filter failed: %s", body)
	}
	objects.unavailable = true
	status, body = f.call(t, "GET", "/v1/images", testClientToken, "", nil)
	if status == 200 {
		t.Fatalf("OSS outage hidden: %s", body)
	}
	status, body = f.call(t, "GET", "/v1/checkpoints", testClientToken, "", nil)
	if status == 200 {
		t.Fatalf("checkpoint outage hidden: %s", body)
	}
	// Disable injected remote store before fixture teardown.
	f.service.store.objects = nil
}
