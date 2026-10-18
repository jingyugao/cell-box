package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
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

func requestJSON(t *testing.T, client *http.Client, method, url, clientID string, body any) (*http.Response, json.RawMessage) {
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
	r, body := requestJSON(t, c, http.MethodPost, s.URL+"/v1/templates", clientID, map[string]any{"name": name, "defaultWarmSlots": warm})
	if r.StatusCode != 201 {
		t.Fatalf("create template status=%d body=%s", r.StatusCode, body)
	}
	return response[Template](t, body)
}

func submitBuild(t *testing.T, s *httptest.Server, c *http.Client, clientID, templateID, version, command string, warm *int) (int, Operation) {
	t.Helper()
	body := map[string]any{"userVersion": version, "baseImage": "registry.example/base:1.01", "profileId": "p", "buildCommand": command}
	if warm != nil {
		body["warmSlots"] = *warm
	}
	r, raw := requestJSON(t, c, http.MethodPost, s.URL+"/v1/templates/"+templateID+"/revisions", clientID, body)
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

func TestCatalogBuildRequestsCreateIndependentRevisionsTenantIsolationAndSQLiteRestart(t *testing.T) {
	dir := t.TempDir()
	builder := &fakeBuilder{}
	m := openTestManager(t, dir, builder)
	s, c := testHTTP(t, m)
	tpl := mustTemplate(t, s, c, "", "agent", 3) // absent client header defaults to a.

	// Every accepted POST is an independent operation, even for identical input.
	var originalOperationIDs []string
	for _, tc := range []struct {
		command string
		warm    *int
	}{{"npm install codex", nil}, {"npm install codex@next", func() *int { n := 0; return &n }()}, {"npm install codex", nil}} {
		status, op := submitBuild(t, s, c, "", tpl.ID, "1.01", tc.command, tc.warm)
		if status != 202 {
			t.Fatalf("submit build status=%d", status)
		}
		originalOperationIDs = append(originalOperationIDs, op.ID)
		finished := awaitOperation(t, m, "a", op.ID)
		if finished.Status != "succeeded" {
			t.Fatalf("build failed: %+v", finished)
		}
	}
	if n := builder.builds.Load(); n != 3 {
		t.Fatalf("builder called %d times, want 3", n)
	}
	inputs := builder.captured()
	if len(inputs) != 3 || inputs[0].BuildRequest.WarmSlots == nil || *inputs[0].BuildRequest.WarmSlots != 3 || inputs[1].BuildRequest.WarmSlots == nil || *inputs[1].BuildRequest.WarmSlots != 0 {
		t.Fatalf("warm slots default/override not captured: %+v", inputs)
	}

	res, raw := requestJSON(t, c, http.MethodGet, s.URL+"/v1/templates/"+tpl.ID+"/revisions?userVersion=1.01", "", nil)
	if res.StatusCode != 200 {
		t.Fatalf("list revisions status=%d", res.StatusCode)
	}
	var revisions []Revision
	if err := json.Unmarshal(raw, &revisions); err != nil {
		t.Fatal(err)
	}
	if len(revisions) != 3 || revisions[0].Revision != 3 || revisions[1].Revision != 2 || revisions[2].Revision != 1 {
		t.Fatalf("revisions are not sequential: %+v", revisions)
	}
	if revisions[0].Version != "1.01-r3" || revisions[1].Version != "1.01-r2" || revisions[2].Version != "1.01-r1" {
		t.Fatalf("version labels = %+v", revisions)
	}

	// Client B cannot read A's family or revisions; unfiltered lists are isolated.
	r, body := requestJSON(t, c, http.MethodGet, s.URL+"/v1/templates/"+tpl.ID, "b", nil)
	if r.StatusCode != 404 {
		t.Fatalf("cross-client template read status=%d body=%v", r.StatusCode, body)
	}
	r, body = requestJSON(t, c, http.MethodGet, s.URL+"/v1/templates", "b", nil)
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
	r, body = requestJSON(t, c2, http.MethodGet, s2.URL+"/v1/templates/"+tpl.ID, "", nil)
	if r.StatusCode != 200 || response[Template](t, body).ID != tpl.ID {
		t.Fatalf("restarted template read status=%d body=%v", r.StatusCode, body)
	}
	r, raw = requestJSON(t, c2, http.MethodGet, s2.URL+"/v1/templates/"+tpl.ID+"/revisions?userVersion=1.01", "", nil)
	if r.StatusCode != 200 {
		t.Fatalf("restarted revisions status=%d", r.StatusCode)
	}
	if err := json.Unmarshal(raw, &revisions); err != nil || len(revisions) != 3 {
		t.Fatalf("restart revisions=%+v err=%v", revisions, err)
	}
	// Repeating the same request after restart creates a fresh operation.
	status, repeated := submitBuild(t, s2, c2, "a", tpl.ID, "1.01", "npm install codex", nil)
	if status != http.StatusAccepted || repeated.ID == "" {
		t.Fatalf("post-restart build status=%d op=%+v", status, repeated)
	}
	for _, id := range originalOperationIDs {
		if repeated.ID == id {
			t.Fatalf("post-restart request reused operation %s", id)
		}
	}
	if got := awaitOperation(t, m2, "a", repeated.ID); got.Status != "succeeded" {
		t.Fatalf("post-restart build=%+v", got)
	}
}

func TestCatalogConcurrentBuildRequestsAreIndependentAndStableGenerationCAS(t *testing.T) {
	builder := &fakeBuilder{}
	m := openTestManager(t, t.TempDir(), builder)
	s, c := testHTTP(t, m)
	tpl := mustTemplate(t, s, c, "a", "agent", 1)
	// Concurrent identical submissions each get their own durable operation.
	const n = 2
	var wg sync.WaitGroup
	wg.Add(n)
	ids := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			resp, body := requestJSON(t, c, http.MethodPost, s.URL+"/v1/templates/"+tpl.ID+"/revisions", "a", map[string]any{"userVersion": "1.0", "baseImage": "registry.example/base:1", "profileId": "p"})
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
	if ids[0] == "" || ids[0] == ids[1] {
		t.Fatalf("concurrent submissions should have distinct operation IDs, got %v", ids)
	}
	for _, id := range ids {
		awaitOperation(t, m, "a", id)
	}
	if builder.builds.Load() != 2 {
		t.Fatalf("builder calls=%d, want 2", builder.builds.Load())
	}

	// Stable must refer to an active revision belonging to the same family; CAS rejects stale generation.
	status, other := submitBuild(t, s, c, "a", tpl.ID, "2.0", "", nil)
	if status != 202 {
		t.Fatalf("second build status=%d", status)
	}
	awaitOperation(t, m, "a", other.ID)
	var list []Revision
	resp, raw := requestJSON(t, c, http.MethodGet, s.URL+"/v1/templates/"+tpl.ID+"/revisions", "a", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("revision list status=%d", resp.StatusCode)
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatal(err)
	}
	rev1, rev2 := list[0], list[1]
	retire, _ := requestJSON(t, c, http.MethodPatch, s.URL+"/v1/template-revisions/"+rev2.ID, "a", map[string]any{"status": "retired", "expectedGeneration": rev2.Generation})
	if retire.StatusCode != 200 {
		t.Fatalf("retire revision status=%d", retire.StatusCode)
	}
	patch := func(templateID, revision string, generation uint64) int {
		r, _ := requestJSON(t, c, http.MethodPatch, s.URL+"/v1/templates/"+templateID, "a", map[string]any{"stableRevisionId": revision, "expectedGeneration": generation})
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
	if status, op := submitBuild(t, s, c, "a", otherTemplate.ID, "1.0", "", nil); status != 202 {
		t.Fatalf("other family build status=%d", status)
	} else {
		awaitOperation(t, m, "a", op.ID)
	}
	_, otherRevisionsRaw := requestJSON(t, c, http.MethodGet, s.URL+"/v1/templates/"+otherTemplate.ID+"/revisions", "a", nil)
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
	_, active := submitBuild(t, s, c, "a", tpl.ID, "1.0", "", nil)
	deadline := time.Now().Add(3 * time.Second)
	for builder.builds.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if builder.builds.Load() == 0 {
		t.Fatal("first build did not reach fake builder")
	}
	_, queued := submitBuild(t, s, c, "a", tpl.ID, "1.0", "", nil)
	resp, _ := requestJSON(t, c, http.MethodPost, s.URL+"/v1/operations/"+active.ID+":cancel", "a", map[string]any{})
	if resp.StatusCode != 200 {
		t.Fatalf("cancel running status=%d", resp.StatusCode)
	}
	resp, _ = requestJSON(t, c, http.MethodPost, s.URL+"/v1/operations/"+queued.ID+":cancel", "a", map[string]any{})
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

	// Restart marks queued and running builds as unknown; it never invokes Builder again.
	seed, err := sql.Open("sqlite", filepath.Join(dir, "cellbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ids := []string{newID("build-"), newID("build-")}
	for i, status := range []string{"queued", "running"} {
		id := ids[i]
		input := BuildInput{BuildRequest: BuildRequest{UserVersion: "9.0", BaseImage: "registry.example/base:9", ProfileID: "p"}, Profile: Profile{ID: "p", Snapshot: json.RawMessage(`{"id":"p"}`)}, TemplateID: tpl.ID, OperationID: id}
		op := Operation{ID: id, Kind: "template-build", TargetID: tpl.ID, Status: status, Stage: status, Version: 1, Input: input.BuildRequest, CreatedAt: now, UpdatedAt: now}
		_, err = seed.Exec(`INSERT INTO operations(id,client_id,template_id,status,created_at,record,input) VALUES(?,?,?,?,?,?,?)`, id, "a", tpl.ID, status, now.Format(time.RFC3339Nano), encode(op), encode(input))
		if err != nil {
			seed.Close()
			t.Fatal(err)
		}
	}
	if err = seed.Close(); err != nil {
		t.Fatal(err)
	}
	newBuilder := &fakeBuilder{}
	m2 := openTestManager(t, dir, newBuilder)
	for _, id := range ids {
		got, err := m2.Operation(context.Background(), "a", id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != "failed" || got.Error == nil || got.Error.Code != "UNKNOWN_OUTCOME" {
			t.Fatalf("interrupted op=%+v", got)
		}
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
	_, op := submitBuild(t, s, c, "a", tpl.ID, "1.0", "", nil)
	awaitOperation(t, m, "a", op.ID)
	r, raw := requestJSON(t, c, http.MethodGet, s.URL+"/v1/operations/"+op.ID+"/logs?offset=0&limit=8", "a", nil)
	if r.StatusCode != 200 {
		t.Fatalf("logs status=%d", r.StatusCode)
	}
	logs := response[Logs](t, raw)
	if len(logs.Text) != 8 || logs.NextOffset != 8 || logs.EOF {
		t.Fatalf("first log chunk=%+v", logs)
	}
	r, raw = requestJSON(t, c, http.MethodGet, s.URL+"/v1/operations/"+op.ID+fmt.Sprintf("/logs?offset=%d", logs.NextOffset), "a", nil)
	if r.StatusCode != 200 {
		t.Fatalf("logs continuation status=%d", r.StatusCode)
	}
	logs = response[Logs](t, raw)
	if !logs.EOF || !strings.Contains(logs.Text, "building 1.0") {
		t.Fatalf("final log chunk=%+v", logs)
	}
}

func TestCatalogIgnoresIdempotencyKeyHeader(t *testing.T) {
	builder := &fakeBuilder{}
	m := openTestManager(t, t.TempDir(), builder)
	s, c := testHTTP(t, m)
	tpl := mustTemplate(t, s, c, "a", "agent", 0)
	body := `{"userVersion":"1.0","baseImage":"registry.example/base:1","profileId":"p"}`
	ids := make([]string, 2)
	for i := range ids {
		req, err := http.NewRequest(http.MethodPost, s.URL+"/v1/templates/"+tpl.ID+"/revisions", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "same-key")
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var op Operation
		if err := json.NewDecoder(resp.Body).Decode(&op); err != nil {
			resp.Body.Close()
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("request %d status=%d", i, resp.StatusCode)
		}
		ids[i] = op.ID
		if got := awaitOperation(t, m, "a", op.ID); got.Status != "succeeded" {
			t.Fatalf("request %d operation=%+v", i, got)
		}
	}
	if ids[0] == "" || ids[0] == ids[1] {
		t.Fatalf("same header reused an operation ID: %v", ids)
	}
	if builder.builds.Load() != 2 {
		t.Fatalf("builder calls=%d, want 2", builder.builds.Load())
	}
}

func TestCatalogStandaloneUsesCellboxDatabaseAndSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "cellbox.db")); err != nil {
		t.Fatalf("standalone database path: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "catalog.db")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected legacy catalog.db: stat error=%v", err)
	}
	seed, err := sql.Open("sqlite", filepath.Join(dir, "cellbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Close()
	var version int
	if err := seed.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 3 {
		t.Fatalf("standalone schema version=%d err=%v, want 3", version, err)
	}
}

func TestCatalogBorrowedDBDoesNotConfigureOrCloseConnection(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "shared.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA user_version=77; CREATE TABLE runtime_records (id TEXT PRIMARY KEY)"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	m, err := Open(Config{Dir: dir, DB: db})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("catalog closed borrowed DB: %v", err)
	}
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 77 {
		t.Fatalf("borrowed database user_version=%d err=%v, want unchanged 77", version, err)
	}
	var count int
	if err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='runtime_records'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("shared runtime table count=%d err=%v", count, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "catalog.lock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("borrowed DB acquired standalone lock: stat error=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestResolveReadsWhileSharedWriterHoldsCatalogLock(t *testing.T) {
	dir := t.TempDir()
	databaseURL := url.URL{Scheme: "file", Path: filepath.Join(dir, "shared.db")}
	databaseURL.RawQuery = url.Values{
		"_pragma": {"busy_timeout(5000)", "foreign_keys(1)", "synchronous(FULL)"},
		"_txlock": {"immediate"},
	}.Encode()
	db, err := sql.Open("sqlite", databaseURL.String())
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(4)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	m, err := Open(Config{Dir: dir, DB: db, Builder: &fakeBuilder{}})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Errorf("close catalog: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Errorf("close shared DB: %v", err)
		}
	})

	templateID, revisionID := "template-shared", "revision-shared"
	tpl := Template{ID: templateID, Name: "shared", StableRevisionID: revisionID}
	rev := Revision{ID: revisionID, TemplateID: templateID, Status: "active"}
	if _, err := db.Exec(`INSERT INTO templates(id,client_id,name,record) VALUES(?,?,?,?)`, templateID, "client-a", tpl.Name, encode(tpl)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO template_revisions(id,client_id,template_id,user_version,revision,record,profile) VALUES(?,?,?,?,?,?,?)`, revisionID, "client-a", templateID, "1.0", 1, encode(rev), `{}`); err != nil {
		t.Fatal(err)
	}

	// Hold the service's immediate write transaction open, then place a catalog
	// writer behind SQLite while it owns the catalog mutex.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE template_revisions SET record=record WHERE id=?`, revisionID); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	writerLocked := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		m.mu.Lock()
		close(writerLocked)
		_, writeErr := m.db.ExecContext(context.Background(), `UPDATE template_revisions SET record=record WHERE id=?`, revisionID)
		m.mu.Unlock()
		writerDone <- writeErr
	}()
	<-writerLocked

	// The second pooled connection is in use by the catalog write while the
	// service transaction owns the first one.
	deadline := time.Now().Add(2 * time.Second)
	for db.Stats().InUse < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if db.Stats().InUse < 2 {
		_ = tx.Rollback()
		<-writerDone
		t.Fatal("catalog writer did not acquire a connection while service transaction was held")
	}
	select {
	case err := <-writerDone:
		_ = tx.Rollback()
		t.Fatalf("catalog writer completed before service transaction was released: %v", err)
	default:
	}

	resolveCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type result struct {
		revision Revision
		profile  json.RawMessage
		err      error
	}
	resolved := make(chan result, 1)
	go func() {
		r, profile, resolveErr := m.Resolve(resolveCtx, "client-a", templateID, "")
		resolved <- result{revision: r, profile: profile, err: resolveErr}
	}()
	var got result
	select {
	case got = <-resolved:
	case <-resolveCtx.Done():
		_ = tx.Rollback()
		<-writerDone
		t.Fatal("Resolve blocked behind a catalog writer holding m.mu")
	}
	if got.err != nil || got.revision.ID != revisionID || string(got.profile) != `{}` {
		t.Fatalf("Resolve result=%+v profile=%s", got.revision, got.profile)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-writerDone; err != nil {
		t.Fatalf("catalog writer after transaction release: %v", err)
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

	status, submitted := submitBuild(t, s, c, "a", tpl.ID, "1.0", "npm install codex", nil)
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
