package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeBuilder struct {
	builds atomic.Int32
	mu     sync.Mutex
	inputs []BuildInput
	block  <-chan struct{}
}

func (b *fakeBuilder) Build(ctx context.Context, in BuildInput, logWriter io.Writer, stage func(string) error) (BuildResult, error) {
	b.builds.Add(1)
	b.mu.Lock()
	b.inputs = append(b.inputs, in)
	b.mu.Unlock()
	if _, err := fmt.Fprintf(logWriter, "building %s\n", in.BuildRequest.UserVersion); err != nil {
		return BuildResult{}, err
	}
	if err := stage("building"); err != nil {
		return BuildResult{}, err
	}
	if b.block != nil {
		select {
		case <-ctx.Done():
			return BuildResult{}, ctx.Err()
		case <-b.block:
		}
	}
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	return BuildResult{ResolvedBaseImage: "registry.example/base@sha256:" + digest, Image: "registry.example/result@sha256:" + digest, Command: []string{"/bin/sh", "-c", "exec launcher"}, Env: map[string]string{"X": "y"}, WorkingDir: "/workspace"}, nil
}

func (b *fakeBuilder) captured() []BuildInput {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]BuildInput(nil), b.inputs...)
}

func openTestManager(t *testing.T, dir string, builder Builder) *Manager {
	t.Helper()
	m, err := Open(Config{
		Dir:     dir,
		Builder: builder,
		ResolveProfile: func(client, id string) (Profile, error) {
			if id != "p" {
				return Profile{}, failure("NOT_FOUND", "profile missing")
			}
			return Profile{ID: id, CPU: 1, MemoryMiB: 512, Snapshot: json.RawMessage(`{"id":"p"}`)}, nil
		},
		FinalizeProfile: func(p Profile, _ BuildResult) (json.RawMessage, error) {
			return append(json.RawMessage(nil), p.Snapshot...), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Errorf("close catalog: %v", err)
		}
	})
	return m
}

func testHTTP(t *testing.T, m *Manager) (*httptest.Server, *http.Client) {
	t.Helper()
	mux := http.NewServeMux()
	m.Register(mux, func(r *http.Request) string {
		if c := r.Header.Get("X-Cellbox-Client-ID"); c != "" {
			return c
		}
		return "a"
	})
	mux.HandleFunc("GET /v1/operations/{id}", m.ServeOperation)
	s := httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s, s.Client()
}

func requestJSON(t *testing.T, client *http.Client, method, url, clientID, idem string, body any) (*http.Response, json.RawMessage) {
	t.Helper()
	var payload strings.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		payload = *strings.NewReader(string(b))
	}
	req, err := http.NewRequest(method, url, &payload)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if clientID != "" {
		req.Header.Set("X-Cellbox-Client-ID", clientID)
	}
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {}()
	var out json.RawMessage
	if res.Body != nil {
		defer res.Body.Close()
		if err := json.NewDecoder(res.Body).Decode(&out); err != nil && res.StatusCode != 204 {
			t.Fatalf("decode %s response: %v", url, err)
		}
	}
	return res, out
}

func response[T any](t *testing.T, raw json.RawMessage) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func mustTemplate(t *testing.T, s *httptest.Server, c *http.Client, clientID, name string, warm int) Template {
	t.Helper()
	r, body := requestJSON(t, c, http.MethodPost, s.URL+"/v1/templates", clientID, "", map[string]any{"name": name, "defaultWarmSlots": warm})
	if r.StatusCode != 201 {
		t.Fatalf("create template status=%d body=%s", r.StatusCode, body)
	}
	return response[Template](t, body)
}

func submitBuild(t *testing.T, s *httptest.Server, c *http.Client, clientID, templateID, key, version, command string, warm *int) (int, Operation) {
	t.Helper()
	body := map[string]any{"userVersion": version, "baseImage": "registry.example/base:1.01", "profileId": "p", "buildCommand": command}
	if warm != nil {
		body["warmSlots"] = *warm
	}
	r, raw := requestJSON(t, c, http.MethodPost, s.URL+"/v1/templates/"+templateID+"/revisions", clientID, key, body)
	var op Operation
	if raw != nil {
		op = response[Operation](t, raw)
	}
	return r.StatusCode, op
}

func awaitOperation(t *testing.T, m *Manager, client, id string) Operation {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		op, err := m.Operation(context.Background(), client, id)
		if err != nil {
			t.Fatal(err)
		}
		if terminal(op.Status) {
			return op
		}
		time.Sleep(5 * time.Millisecond)
	}
	op, _ := m.Operation(context.Background(), client, id)
	t.Fatalf("operation did not finish: %+v", op)
	return Operation{}
}

func TestCatalogBuildVersionsIdempotencyTenantIsolationAndSQLiteRestart(t *testing.T) {
	dir := t.TempDir()
	builder := &fakeBuilder{}
	m := openTestManager(t, dir, builder)
	s, c := testHTTP(t, m)
	tpl := mustTemplate(t, s, c, "", "agent", 3) // absent client header defaults to a.

	// Two explicit builds with different keys make successive revisions.
	for _, tc := range []struct {
		key, command string
		warm         *int
	}{{"build-1", "npm install codex", nil}, {"build-2", "npm install codex@next", func() *int { n := 0; return &n }()}} {
		status, op := submitBuild(t, s, c, "", tpl.ID, tc.key, "1.01", tc.command, tc.warm)
		if status != 202 {
			t.Fatalf("submit build status=%d", status)
		}
		finished := awaitOperation(t, m, "a", op.ID)
		if finished.Status != "succeeded" {
			t.Fatalf("build failed: %+v", finished)
		}
	}
	if n := builder.builds.Load(); n != 2 {
		t.Fatalf("builder called %d times, want 2", n)
	}
	inputs := builder.captured()
	if len(inputs) != 2 || inputs[0].BuildRequest.WarmSlots == nil || *inputs[0].BuildRequest.WarmSlots != 3 || inputs[1].BuildRequest.WarmSlots == nil || *inputs[1].BuildRequest.WarmSlots != 0 {
		t.Fatalf("warm slots default/override not captured: %+v", inputs)
	}

	res, raw := requestJSON(t, c, http.MethodGet, s.URL+"/v1/templates/"+tpl.ID+"/revisions?userVersion=1.01", "", "", nil)
	if res.StatusCode != 200 {
		t.Fatalf("list revisions status=%d", res.StatusCode)
	}
	var revisions []Revision
	if err := json.Unmarshal(raw, &revisions); err != nil {
		t.Fatal(err)
	}
	if len(revisions) != 2 || revisions[0].Revision != 2 || revisions[1].Revision != 1 {
		t.Fatalf("revisions are not sequential: %+v", revisions)
	}
	if revisions[0].Version != "1.01-r2" || revisions[1].Version != "1.01-r1" {
		t.Fatalf("version labels = %q,%q", revisions[0].Version, revisions[1].Version)
	}

	// Identical idempotent retry returns the original task and never invokes Builder again.
	status, first := submitBuild(t, s, c, "a", tpl.ID, "retry-key", "1.01", "npm install stable", nil)
	if status != 202 {
		t.Fatalf("first idempotent build status=%d", status)
	}
	awaitOperation(t, m, "a", first.ID)
	status, retry := submitBuild(t, s, c, "a", tpl.ID, "retry-key", "1.01", "npm install stable", nil)
	if status != 202 || retry.ID != first.ID || builder.builds.Load() != 3 {
		t.Fatalf("idempotent retry status=%d op=%+v builds=%d", status, retry, builder.builds.Load())
	}
	status, _ = submitBuild(t, s, c, "a", tpl.ID, "retry-key", "1.01", "npm install changed", nil)
	if status != 409 {
		t.Fatalf("same idempotency key with changed input status=%d", status)
	}

	// Client B cannot read A's family or revisions; unfiltered lists are isolated.
	r, body := requestJSON(t, c, http.MethodGet, s.URL+"/v1/templates/"+tpl.ID, "b", "", nil)
	if r.StatusCode != 404 {
		t.Fatalf("cross-client template read status=%d body=%v", r.StatusCode, body)
	}
	r, body = requestJSON(t, c, http.MethodGet, s.URL+"/v1/templates", "b", "", nil)
	if r.StatusCode != 200 {
		t.Fatalf("client B list status=%d", r.StatusCode)
	}
	var bTemplates []Template
	if err := json.Unmarshal(body, &bTemplates); err != nil {
		t.Fatal(err)
	}
	if len(bTemplates) != 0 {
		t.Fatalf("client B saw templates: %+v", bTemplates)
	}

	// Exactly the three catalog tables are persisted, and a fresh Manager reads them back.
	var tableCount int
	if err := m.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 3 {
		t.Fatalf("catalog has %d user tables, want 3", tableCount)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m2 := openTestManager(t, dir, &fakeBuilder{})
	s2, c2 := testHTTP(t, m2)
	r, body = requestJSON(t, c2, http.MethodGet, s2.URL+"/v1/templates/"+tpl.ID, "", "", nil)
	if r.StatusCode != 200 || response[Template](t, body).ID != tpl.ID {
		t.Fatalf("restarted template read status=%d body=%v", r.StatusCode, body)
	}
	r, raw = requestJSON(t, c2, http.MethodGet, s2.URL+"/v1/templates/"+tpl.ID+"/revisions?userVersion=1.01", "", "", nil)
	if r.StatusCode != 200 {
		t.Fatalf("restarted revisions status=%d", r.StatusCode)
	}
	if err := json.Unmarshal(raw, &revisions); err != nil || len(revisions) != 3 {
		t.Fatalf("restart revisions=%+v err=%v", revisions, err)
	}
}

func TestCatalogConcurrentIdempotencyAndStableGenerationCAS(t *testing.T) {
	builder := &fakeBuilder{}
	m := openTestManager(t, t.TempDir(), builder)
	s, c := testHTTP(t, m)
	tpl := mustTemplate(t, s, c, "a", "agent", 1)
	// Concurrent duplicate submissions must converge on one durable operation.
	const n = 2
	var wg sync.WaitGroup
	wg.Add(n)
	ids := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			resp, body := requestJSON(t, c, http.MethodPost, s.URL+"/v1/templates/"+tpl.ID+"/revisions", "a", "concurrent-key", map[string]any{"userVersion": "1.0", "baseImage": "registry.example/base:1", "profileId": "p"})
			if resp.StatusCode != 202 {
				errs[i] = fmt.Errorf("status %d", resp.StatusCode)
				return
			}
			ids[i] = response[Operation](t, body).ID
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if ids[0] == "" || ids[0] != ids[1] {
		t.Fatalf("concurrent submissions returned IDs %v", ids)
	}
	awaitOperation(t, m, "a", ids[0])
	if builder.builds.Load() != 1 {
		t.Fatalf("duplicate request invoked builder %d times", builder.builds.Load())
	}

	// Stable must refer to an active revision belonging to the same family; CAS rejects stale generation.
	status, other := submitBuild(t, s, c, "a", tpl.ID, "other-key", "2.0", "", nil)
	if status != 202 {
		t.Fatalf("second build status=%d", status)
	}
	awaitOperation(t, m, "a", other.ID)
	var list []Revision
	resp, raw := requestJSON(t, c, http.MethodGet, s.URL+"/v1/templates/"+tpl.ID+"/revisions", "a", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("revision list status=%d", resp.StatusCode)
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatal(err)
	}
	rev1, rev2 := list[0], list[1]
	retire, _ := requestJSON(t, c, http.MethodPatch, s.URL+"/v1/template-revisions/"+rev2.ID, "a", "", map[string]any{"status": "retired", "expectedGeneration": rev2.Generation})
	if retire.StatusCode != 200 {
		t.Fatalf("retire revision status=%d", retire.StatusCode)
	}
	patch := func(templateID, revision string, generation uint64) int {
		r, _ := requestJSON(t, c, http.MethodPatch, s.URL+"/v1/templates/"+templateID, "a", "", map[string]any{"stableRevisionId": revision, "expectedGeneration": generation})
		return r.StatusCode
	}
	if got := patch(tpl.ID, rev1.ID, tpl.Generation); got != 200 {
		t.Fatalf("set stable status=%d", got)
	}
	if got := patch(tpl.ID, rev2.ID, tpl.Generation); got != 409 {
		t.Fatalf("stale generation status=%d", got)
	}
	if got := patch(tpl.ID, rev2.ID, tpl.Generation+1); got != 409 {
		t.Fatalf("retired stable target status=%d", got)
	}
	otherTemplate := mustTemplate(t, s, c, "a", "other", 0)
	if status, op := submitBuild(t, s, c, "a", otherTemplate.ID, "other-family-build", "1.0", "", nil); status != 202 {
		t.Fatalf("other family build status=%d", status)
	} else {
		awaitOperation(t, m, "a", op.ID)
	}
	_, otherRevisionsRaw := requestJSON(t, c, http.MethodGet, s.URL+"/v1/templates/"+otherTemplate.ID+"/revisions", "a", "", nil)
	var otherRevisions []Revision
	if err := json.Unmarshal(otherRevisionsRaw, &otherRevisions); err != nil || len(otherRevisions) != 1 {
		t.Fatalf("other family revisions=%+v err=%v", otherRevisions, err)
	}
	if got := patch(tpl.ID, otherRevisions[0].ID, tpl.Generation+1); got != 409 {
		t.Fatalf("cross-family stable status=%d", got)
	}
}

func TestCatalogCancelAndRestartDoNotCreateOrRerunRevision(t *testing.T) {
	// First build blocks in the fake to keep a second request queued for cancellation.
	gate := make(chan struct{})
	builder := &fakeBuilder{block: gate}
	dir := t.TempDir()
	m := openTestManager(t, dir, builder)
	s, c := testHTTP(t, m)
	tpl := mustTemplate(t, s, c, "a", "agent", 0)
	_, active := submitBuild(t, s, c, "a", tpl.ID, "active", "1.0", "", nil)
	deadline := time.Now().Add(3 * time.Second)
	for builder.builds.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if builder.builds.Load() == 0 {
		t.Fatal("first build did not reach fake builder")
	}
	_, queued := submitBuild(t, s, c, "a", tpl.ID, "queued", "1.0", "", nil)
	resp, _ := requestJSON(t, c, http.MethodPost, s.URL+"/v1/operations/"+active.ID+":cancel", "a", "", map[string]any{})
	if resp.StatusCode != 200 {
		t.Fatalf("cancel running status=%d", resp.StatusCode)
	}
	resp, _ = requestJSON(t, c, http.MethodPost, s.URL+"/v1/operations/"+queued.ID+":cancel", "a", "", map[string]any{})
	if resp.StatusCode != 200 {
		t.Fatalf("cancel queued status=%d", resp.StatusCode)
	}
	queuedOp := awaitOperation(t, m, "a", queued.ID)
	if queuedOp.Status != "cancelled" {
		t.Fatalf("queued operation status=%s", queuedOp.Status)
	}
	close(gate)
	activeOp := awaitOperation(t, m, "a", active.ID)
	if activeOp.Status != "cancelled" {
		t.Fatalf("running operation status=%s", activeOp.Status)
	}
	var revisions int
	if err := m.db.QueryRow(`SELECT count(*) FROM template_revisions`).Scan(&revisions); err != nil || revisions != 0 {
		t.Fatalf("revision count=%d err=%v", revisions, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a process dying with a queued operation: startup marks it interrupted and never invokes Builder.
	seed, err := sql.Open("sqlite", filepath.Join(dir, "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	id := newID("build-")
	input := BuildInput{BuildRequest: BuildRequest{UserVersion: "9.0", BaseImage: "registry.example/base:9", ProfileID: "p"}, Profile: Profile{ID: "p", Snapshot: json.RawMessage(`{"id":"p"}`)}, TemplateID: tpl.ID, OperationID: id}
	op := Operation{ID: id, Kind: "template-build", TargetID: tpl.ID, Status: "queued", Stage: "queued", Version: 1, Input: input.BuildRequest, CreatedAt: now, UpdatedAt: now}
	_, err = seed.Exec(`INSERT INTO operations(id,client_id,template_id,idempotency_key,request_hash,status,created_at,record,input) VALUES(?,?,?,?,?,?,?,?,?)`, id, "a", tpl.ID, "interrupted", "hash", "queued", now.Format(time.RFC3339Nano), encode(op), encode(input))
	if err != nil {
		seed.Close()
		t.Fatal(err)
	}
	if err = seed.Close(); err != nil {
		t.Fatal(err)
	}
	newBuilder := &fakeBuilder{}
	m2 := openTestManager(t, dir, newBuilder)
	got, err := m2.Operation(context.Background(), "a", id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "failed" || got.Error == nil || got.Error.Code != "OPERATION_INTERRUPTED" {
		t.Fatalf("interrupted op=%+v", got)
	}
	if newBuilder.builds.Load() != 0 {
		t.Fatalf("restart reran a pending command %d times", newBuilder.builds.Load())
	}
}

func TestCatalogOperationLogsUseJSONOffsets(t *testing.T) {
	builder := &fakeBuilder{}
	m := openTestManager(t, t.TempDir(), builder)
	s, c := testHTTP(t, m)
	tpl := mustTemplate(t, s, c, "a", "agent", 0)
	_, op := submitBuild(t, s, c, "a", tpl.ID, "logs", "1.0", "", nil)
	awaitOperation(t, m, "a", op.ID)
	r, raw := requestJSON(t, c, http.MethodGet, s.URL+"/v1/operations/"+op.ID+"/logs?offset=0&limit=8", "a", "", nil)
	if r.StatusCode != 200 {
		t.Fatalf("logs status=%d", r.StatusCode)
	}
	logs := response[Logs](t, raw)
	if len(logs.Text) != 8 || logs.NextOffset != 8 || logs.EOF {
		t.Fatalf("first log chunk=%+v", logs)
	}
	r, raw = requestJSON(t, c, http.MethodGet, s.URL+"/v1/operations/"+op.ID+fmt.Sprintf("/logs?offset=%d", logs.NextOffset), "a", "", nil)
	if r.StatusCode != 200 {
		t.Fatalf("logs continuation status=%d", r.StatusCode)
	}
	logs = response[Logs](t, raw)
	if !logs.EOF || !strings.Contains(logs.Text, "building 1.0") {
		t.Fatalf("final log chunk=%+v", logs)
	}
}

func TestCatalogIdempotencyKeyIsScopedToTemplate(t *testing.T) {
	builder := &fakeBuilder{}
	m := openTestManager(t, t.TempDir(), builder)
	s, c := testHTTP(t, m)
	firstTemplate := mustTemplate(t, s, c, "a", "first", 0)
	secondTemplate := mustTemplate(t, s, c, "a", "second", 0)

	status1, first := submitBuild(t, s, c, "a", firstTemplate.ID, "same-key", "1.0", "npm install codex", nil)
	status2, second := submitBuild(t, s, c, "a", secondTemplate.ID, "same-key", "1.0", "npm install other", nil)
	if status1 != http.StatusAccepted || status2 != http.StatusAccepted || first.ID == second.ID {
		t.Fatalf("same key across families: statuses=(%d,%d), operations=(%s,%s)", status1, status2, first.ID, second.ID)
	}
	if got := awaitOperation(t, m, "a", first.ID); got.Status != "succeeded" {
		t.Fatalf("first family build: %+v", got)
	}
	if got := awaitOperation(t, m, "a", second.ID); got.Status != "succeeded" {
		t.Fatalf("second family build: %+v", got)
	}
	if builder.builds.Load() != 2 {
		t.Fatalf("builder calls=%d, want 2", builder.builds.Load())
	}

	status, _ := submitBuild(t, s, c, "a", firstTemplate.ID, "same-key", "1.0", "npm install changed", nil)
	if status != http.StatusConflict {
		t.Fatalf("changed input in same family status=%d, want 409", status)
	}
}

func TestCatalogRevisionInsertFailureTerminatesOperationAndKeepsImage(t *testing.T) {
	builder := &fakeBuilder{}
	m := openTestManager(t, t.TempDir(), builder)
	s, c := testHTTP(t, m)
	tpl := mustTemplate(t, s, c, "a", "agent", 2)
	if _, err := m.db.Exec(`CREATE TRIGGER reject_revision BEFORE INSERT ON template_revisions BEGIN SELECT RAISE(ABORT,'test failure'); END;`); err != nil {
		t.Fatal(err)
	}

	status, submitted := submitBuild(t, s, c, "a", tpl.ID, "triggered", "1.0", "npm install codex", nil)
	if status != http.StatusAccepted {
		t.Fatalf("submit status=%d", status)
	}
	finished := awaitOperation(t, m, "a", submitted.ID)
	if finished.Status != "failed" || finished.Error == nil {
		t.Fatalf("revision insert failure operation=%+v", finished)
	}
	if finished.Result["image"] != "registry.example/result@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("failed operation lost published image reference: result=%v", finished.Result)
	}
	var count int
	if err := m.db.QueryRow(`SELECT count(*) FROM template_revisions WHERE template_id=?`, tpl.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("revision insert left %d revision rows", count)
	}
	current, err := getTemplate(context.Background(), m.db, "a", tpl.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.StableRevisionID != "" {
		t.Fatalf("failed build changed stable target to %q", current.StableRevisionID)
	}
}
