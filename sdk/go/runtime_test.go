package cellbox

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRuntimeQueriesDecodeSnapshotsAndSandboxFilters(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertHeaders(t, r)
		switch {
		case r.URL.Path == "/v1/templates/tpl-1/runtime":
			writeJSONTest(w, map[string]any{"templateId": "tpl-1", "revisions": []any{map[string]any{"revisionId": "rev-1", "sandboxCount": nil, "phases": map[string]int{}, "stale": true, "warmPool": map[string]any{"revisionId": "rev-1", "generation": 2, "observedGeneration": 1, "desiredSlots": 2, "syncStatus": "pending", "stale": true, "pool": nil}}}})
		case r.URL.Path == "/v1/template-revisions/rev-1/warm-pool":
			writeJSONTest(w, map[string]any{"revisionId": "rev-1", "generation": 2, "observedGeneration": 2, "desiredSlots": 2, "syncStatus": "applied", "stale": false, "pool": map[string]any{"poolId": "pool-a", "requestedSlots": 2, "allocatedSlots": 2, "readySlots": 1, "preparingSlots": 1, "failedSlots": 0, "resources": map[string]int64{"cpuRequestMilli": 1000, "cpuLimitMilli": 2000, "memoryRequestBytes": 536870912, "memoryLimitBytes": 1073741824}, "stale": false}})
		case r.URL.Path == "/v1/boxes":
			if got, want := r.URL.Query().Encode(), "limit=20&offset=10&phase=running&revisionId=rev-1&templateId=tpl-1"; got != want {
				t.Errorf("query = %q, want %q", got, want)
			}
			writeJSONTest(w, []SandboxInfo{})
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := testClient(t, server, "")

	runtime, err := c.GetTemplateRuntime(context.Background(), "tpl-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.Revisions) != 1 || runtime.Revisions[0].SandboxCount != nil || runtime.Revisions[0].WarmPool.ObservedGeneration != 1 {
		t.Fatalf("runtime snapshot = %+v", runtime)
	}
	warm, err := c.GetRevisionWarmPool(context.Background(), "rev-1")
	if err != nil {
		t.Fatal(err)
	}
	if warm.Pool == nil || warm.Pool.Resources.MemoryRequestBytes != 536870912 || warm.Pool.ReadySlots != 1 {
		t.Fatalf("warm status = %+v", warm)
	}
	if _, err := c.ListSandboxes(context.Background(), SandboxListOptions{TemplateID: "tpl-1", RevisionID: "rev-1", Phase: "running", Limit: 20, Offset: 10}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(warm.Pool.PoolID, " ") {
		t.Fatal("unexpected pool id")
	}
}

func TestListSandboxesRejectsInvalidOptionsWithoutRequest(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; writeJSONTest(w, []SandboxInfo{}) }))
	defer server.Close()
	c := testClient(t, server, "")
	if _, err := c.ListSandboxes(context.Background(), SandboxListOptions{Limit: 501}); err == nil {
		t.Fatal("expected invalid limit")
	}
	if _, err := c.ListSandboxes(context.Background(), SandboxListOptions{}, SandboxListOptions{}); err == nil {
		t.Fatal("expected duplicate options error")
	}
	if calls != 0 {
		t.Fatalf("invalid options sent %d requests", calls)
	}
}
