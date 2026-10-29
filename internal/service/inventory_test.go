package service

import (
	"context"
	"errors"
	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/inventory"
	"net/http"
	"testing"
	"time"
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

func (p *runtimeInventory) List(_ context.Context, _, client string) ([]inventory.Record, error) {
	p.calls++
	rows := make([]inventory.Record, 0, len(p.rows))
	for _, row := range p.rows {
		if row.ClientID == "" || row.ClientID == client {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func TestRuntimeBoxInventoryFiltersAndPaginatesDeterministically(t *testing.T) {
	f := newCoreFixture(t)
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	rows := []Box{
		{ID: "box-c", TemplateID: "tpl-a", RevisionID: "rev-2", Phase: "running", CreatedAt: base.Add(2 * time.Second)},
		{ID: "box-b", TemplateID: "tpl-a", RevisionID: "rev-1", Phase: "suspended", CreatedAt: base.Add(time.Second)},
		{ID: "box-a", TemplateID: "tpl-a", RevisionID: "rev-1", Phase: "running", CreatedAt: base},
		{ID: "box-other", TemplateID: "tpl-b", RevisionID: "rev-9", Phase: "running", CreatedAt: base},
	}
	records := make([]inventory.Record, 0, len(rows))
	for _, box := range rows {
		records = append(records, inventory.Record{ClientID: "client-a", Box: mustBoxJSON(box)})
	}
	provider := &runtimeInventory{Provider: f.provider, rows: records}
	f.service.providers["docker"] = provider

	status, body := f.call(t, "GET", "/v1/boxes?templateId=tpl-a&phase=running&limit=1&offset=1", testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	got := decodeResponse[[]Box](t, body)
	if len(got) != 1 || got[0].ID != "box-c" {
		t.Fatalf("filtered page = %+v, want box-c: %s", got, body)
	}

	status, body = f.call(t, "GET", "/v1/boxes?revisionId=rev-1", testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	got = decodeResponse[[]Box](t, body)
	if len(got) != 2 || got[0].ID != "box-a" || got[1].ID != "box-b" {
		t.Fatalf("default sorted revision results = %+v", got)
	}

	// A known, authorized explicit ID remains a successful lookup even when
	// additional filters exclude it. Missing or foreign IDs still return 404.
	status, body = f.call(t, "GET", "/v1/boxes?id=box-a&templateId=tpl-b", testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	if got = decodeResponse[[]Box](t, body); len(got) != 0 {
		t.Fatalf("ID/filter mismatch returned rows: %+v", got)
	}
	status, body = f.call(t, "GET", "/v1/boxes?id=missing&templateId=tpl-b", testClientToken, "", nil)
	wantStatus(t, status, http.StatusNotFound, body)
	status, body = f.call(t, "GET", "/v1/boxes?id=box-a", otherClientToken, "", nil)
	wantStatus(t, status, http.StatusNotFound, body)
	if provider.calls != 5 {
		t.Fatalf("provider list calls=%d, want 5", provider.calls)
	}
}

func TestBoxInventoryPaginationRejectsInvalidParametersBeforeListing(t *testing.T) {
	f := newCoreFixture(t)
	provider := &runtimeInventory{Provider: f.provider}
	f.service.providers["docker"] = provider
	for _, query := range []string{
		"limit=0", "limit=501", "limit=abc", "limit=1&limit=2",
		"offset=-1", "offset=abc", "offset=1&offset=2",
		"templateId=", "templateId=a&templateId=b", "revisionId=", "phase=",
	} {
		status, body := f.call(t, "GET", "/v1/boxes?"+query, testClientToken, "", nil)
		wantStatus(t, status, http.StatusBadRequest, body)
	}
	if provider.calls != 0 {
		t.Fatalf("invalid query reached runtime provider %d times", provider.calls)
	}
}

func TestLocalBoxInventoryUsesSameFiltersAndPagination(t *testing.T) {
	f := newCoreFixture(t)
	_, first := f.createBox(t, "local-list-first")
	_, second := f.createBoxWithTemplate(t, "local-list-second", first.TemplateID)
	f.service.providers["docker"] = nil
	status, body := f.call(t, "GET", "/v1/boxes?templateId="+first.TemplateID+"&limit=1&offset=1", testClientToken, "", nil)
	wantStatus(t, status, http.StatusOK, body)
	got := decodeResponse[[]Box](t, body)
	if len(got) != 1 || got[0].ID != second.ID {
		t.Fatalf("local filtered page = %+v; want %s", got, second.ID)
	}
}

func TestCheckpointInventoryReadsSQLiteAndIsIndependentOfArchiveStorage(t *testing.T) {
	f := newCoreFixture(t)
	f.service.objects = &ledgerObjects{unavailable: true}
	row := inventory.Record{ClientID: "client-a", RuntimeID: "runtime-only", Snapshot: "checkpoint-1", Box: mustBoxJSON(Box{ID: "box-external", Phase: "suspended"})}
	if err := f.service.store.PutCheckpoint(context.Background(), row); err != nil {
		t.Fatal(err)
	}
	status, body := f.call(t, "GET", "/v1/checkpoints", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if rows := decodeResponse[[]Box](t, body); len(rows) != 1 || rows[0].ID != "box-external" {
		t.Fatalf("SQLite checkpoint missing: %s", body)
	}
	status, body = f.call(t, "GET", "/v1/checkpoints", otherClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if len(decodeResponse[[]Box](t, body)) != 0 {
		t.Fatal("foreign checkpoint leaked")
	}
	if err := f.service.store.DeleteCheckpoint(context.Background(), row.RuntimeID, "older-snapshot"); err == nil {
		t.Fatal("deleted a newer snapshot")
	}
	if _, err := f.service.store.GetCheckpoint(context.Background(), row.RuntimeID); err != nil {
		t.Fatal(err)
	}
	if err := f.service.store.DeleteCheckpoint(context.Background(), row.RuntimeID, row.Snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.store.GetCheckpoint(context.Background(), row.RuntimeID); !errors.Is(err, inventory.ErrNotFound) {
		t.Fatalf("checkpoint deletion: %v", err)
	}
}
