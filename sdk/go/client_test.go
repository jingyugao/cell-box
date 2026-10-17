package cellbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(t *testing.T, server *httptest.Server, basePath string) *Client {
	t.Helper()
	client, err := NewClient(Config{BaseURL: server.URL + basePath, ClientID: "sdk-test", Token: "secret", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func assertHeaders(t *testing.T, r *http.Request) {
	t.Helper()
	if got := r.Header.Get("X-Cellbox-Client-ID"); got != "sdk-test" {
		t.Errorf("client namespace = %q, want sdk-test", got)
	}
	if got := r.Header.Get("Authorization"); got != "Bearer secret" {
		t.Errorf("Authorization = %q, want Bearer secret", got)
	}
}

func writeJSONTest(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func TestCreateWaitsForOperationAndConnects(t *testing.T) {
	var postCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertHeaders(t, r)
		if !strings.HasPrefix(r.URL.Path, "/gateway") {
			t.Errorf("request path %q lost optional prefix", r.URL.Path)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/gateway/v1/boxes":
			postCount.Add(1)
			var body CreateOptions
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.TemplateID != "tpl-python-v1" || body.OwnerKey != "job-7" {
				t.Errorf("create body = %+v", body)
			}
			if got := r.Header.Get("Idempotency-Key"); got != "create-key" {
				t.Errorf("idempotency key = %q", got)
			}
			w.WriteHeader(http.StatusAccepted)
			writeJSONTest(w, Operation{ID: "op-1", Kind: "create", TargetID: "box-1", Status: "running"})
		case r.Method == http.MethodGet && r.URL.Path == "/gateway/v1/operations/op-1":
			if r.URL.Query().Get("waitMs") != "10000" {
				t.Errorf("expected operation long polling, got %s", r.URL.String())
			}
			writeJSONTest(w, Operation{ID: "op-1", Kind: "create", TargetID: "box-1", Status: "succeeded", Result: map[string]string{"boxId": "box-1"}})
		case r.Method == http.MethodGet && r.URL.Path == "/gateway/v1/boxes/box-1":
			writeJSONTest(w, SandboxInfo{ID: "box-1", Phase: "running", Generation: 3, OwnerKey: "job-7"})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := testClient(t, server, "/gateway")
	sandbox, err := client.Create(context.Background(), CreateOptions{TemplateID: "tpl-python-v1", OwnerKey: "job-7", IdempotencyKey: "create-key"})
	if err != nil {
		t.Fatal(err)
	}
	if sandbox.ID != "box-1" || sandbox.Generation != 3 || sandbox.Commands == nil {
		t.Fatalf("sandbox = %+v", sandbox)
	}
	if got := postCount.Load(); got != 1 {
		t.Fatalf("create requests = %d, want 1", got)
	}
}

func TestWaitOperationTreatsCancellingAsPending(t *testing.T) {
	var gets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/operations/op-cancel" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
			return
		}
		n := gets.Add(1)
		status := "queued"
		if n == 2 {
			status = "cancelling"
		} else if n >= 3 {
			status = "succeeded"
		}
		writeJSONTest(w, Operation{ID: "op-cancel", Kind: "template-build", Status: status})
	}))
	defer server.Close()
	client := testClient(t, server, "")
	op, err := client.WaitOperation(context.Background(), "op-cancel")
	if err != nil {
		t.Fatalf("WaitOperation while cancelling: %v", err)
	}
	if op.Status != "succeeded" || gets.Load() < 3 {
		t.Fatalf("operation=%+v GETs=%d, want succeeded after cancelling", op, gets.Load())
	}
}

func TestTemplateFamilyAndRevisionAPIContract(t *testing.T) {
	templateBody := `{"id":"tpl-1","name":"python","description":"python env","defaultWarmSlots":2,"stableRevisionId":"rev-1","generation":3,"createdAt":"2026-01-02T03:04:05Z","updatedAt":"2026-01-02T03:04:05Z"}`
	revisionBody := `{"id":"rev-1","templateId":"tpl-1","userVersion":"1.01","revision":2,"version":"1.01-r2","profileId":"small","baseImage":"python:3","resolvedBaseImage":"python@sha256:abc","image":"cellbox/python@sha256:def","buildCommand":"npm install -g codex","runCommand":"node launcher.js","platform":"linux/amd64","cpu":1,"memoryMiB":512,"warmSlots":2,"status":"active","generation":1}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertHeaders(t, r)
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates":
			var body CreateTemplateOptions
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.Name != "python" || body.Description != "python env" || body.DefaultWarmSlots != 2 {
				t.Errorf("create template body = %+v", body)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(templateBody))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/templates" && r.URL.Query().Get("name") == "python" && r.URL.Query().Get("limit") == "10":
			writeJSONTest(w, []Template{{ID: "tpl-1", Name: "python"}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/templates/tpl-1":
			_, _ = w.Write([]byte(templateBody))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/templates/tpl-1":
			var body UpdateTemplateOptions
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.ExpectedGeneration != 3 || body.StableRevisionID == nil || *body.StableRevisionID != "rev-1" {
				t.Errorf("template patch body = %+v", body)
			}
			_, _ = w.Write([]byte(templateBody))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates/tpl-1/revisions":
			var body CreateRevisionOptions
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.UserVersion != "1.01" || body.BaseImage != "python:3" || body.BuildCommand == "" || r.Header.Get("Idempotency-Key") != "build-key" {
				t.Errorf("revision submission body/header = %+v / %q", body, r.Header.Get("Idempotency-Key"))
			}
			w.WriteHeader(http.StatusAccepted)
			writeJSONTest(w, Operation{ID: "op-build", Kind: "template-build", TargetID: "rev-1", Status: "queued"})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/templates/tpl-1/revisions" && r.URL.Query().Get("userVersion") == "1.01":
			writeJSONTest(w, []TemplateRevision{{ID: "rev-1", Version: "1.01-r2"}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/template-revisions/rev-1":
			_, _ = w.Write([]byte(revisionBody))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/template-revisions/rev-1":
			var body UpdateRevisionOptions
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.ExpectedGeneration != 1 || body.WarmSlots == nil || *body.WarmSlots != 4 {
				t.Errorf("revision patch = %+v", body)
			}
			_, _ = w.Write([]byte(revisionBody))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/templates/tpl-1":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := testClient(t, server, "")

	template, err := client.CreateTemplate(context.Background(), CreateTemplateOptions{Name: "python", Description: "python env", DefaultWarmSlots: 2})
	if err != nil || template.ID != "tpl-1" || template.StableRevisionID != "rev-1" {
		t.Fatalf("CreateTemplate = %+v, %v", template, err)
	}
	listed, err := client.ListTemplates(context.Background(), TemplateListOptions{Name: "python", Limit: 10})
	if err != nil || len(listed) != 1 || listed[0].ID != "tpl-1" {
		t.Fatalf("ListTemplates = %+v, %v", listed, err)
	}
	got, err := client.GetTemplate(context.Background(), "tpl-1")
	if err != nil || got.Name != "python" {
		t.Fatalf("GetTemplate = %+v, %v", got, err)
	}
	stable := "rev-1"
	updated, err := client.UpdateTemplate(context.Background(), "tpl-1", UpdateTemplateOptions{StableRevisionID: &stable, ExpectedGeneration: 3})
	if err != nil || updated.ID != "tpl-1" {
		t.Fatalf("UpdateTemplate = %+v, %v", updated, err)
	}
	warm := 4
	op, err := client.BuildRevision(context.Background(), "tpl-1", CreateRevisionOptions{UserVersion: "1.01", BaseImage: "python:3", ProfileID: "small", BuildCommand: "npm install -g codex"}, "build-key")
	if err != nil || op.ID != "op-build" {
		t.Fatalf("BuildRevision = %+v, %v", op, err)
	}
	revisions, err := client.ListTemplateRevisions(context.Background(), "tpl-1", RevisionListOptions{UserVersion: "1.01"})
	if err != nil || len(revisions) != 1 {
		t.Fatalf("ListTemplateRevisions = %+v, %v", revisions, err)
	}
	loaded, err := client.GetTemplateRevision(context.Background(), "rev-1")
	if err != nil || loaded.Image == "" {
		t.Fatalf("GetTemplateRevision = %+v, %v", loaded, err)
	}
	_, err = client.UpdateTemplateRevision(context.Background(), "rev-1", UpdateRevisionOptions{WarmSlots: &warm, ExpectedGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteTemplate(context.Background(), "tpl-1"); err != nil {
		t.Fatalf("DeleteTemplate: %v", err)
	}
}

func TestCreateRequiresTemplateIDAndWarmSlotsCannotBeNegative(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); http.NotFound(w, r) }))
	defer server.Close()
	client := testClient(t, server, "")
	if _, err := client.CreateAsync(context.Background(), CreateOptions{OwnerKey: "owner"}); err == nil {
		t.Fatal("CreateAsync accepted missing template ID")
	}
	if _, err := client.CreateTemplate(context.Background(), CreateTemplateOptions{Name: "python", DefaultWarmSlots: -1}); err == nil {
		t.Fatal("CreateTemplate accepted negative warm slots")
	}
	if _, err := client.BuildRevision(context.Background(), "tpl", CreateRevisionOptions{UserVersion: "1.0", BaseImage: "base", ProfileID: "p", WarmSlots: func() *int { x := -1; return &x }()}, "k"); err == nil {
		t.Fatal("BuildRevision accepted negative warm slots")
	}
	if requests.Load() != 0 {
		t.Fatalf("invalid requests sent %d requests", requests.Load())
	}
}

func TestWaitBuildRevisionRequiresRevisionResultAndPreservesOperationOnGetFailure(t *testing.T) {
	t.Run("family ID is not revision ID", func(t *testing.T) {
		var revisionGets atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/operations/op-build" {
				writeJSONTest(w, Operation{ID: "op-build", Kind: "template-build", TargetID: "tpl-family", Status: "succeeded"})
				return
			}
			if strings.HasPrefix(r.URL.Path, "/v1/template-revisions/") {
				revisionGets.Add(1)
			}
			http.NotFound(w, r)
		}))
		defer server.Close()
		client := testClient(t, server, "")
		_, err := client.WaitBuildRevision(context.Background(), &Operation{ID: "op-build", Kind: "template-build", Status: "queued"})
		var opErr *OperationError
		if !errors.As(err, &opErr) || opErr.Operation == nil || opErr.Operation.TargetID != "tpl-family" {
			t.Fatalf("expected operation error preserving family operation, got %#v", err)
		}
		if revisionGets.Load() != 0 {
			t.Fatalf("attempted revision GET without result.revisionId")
		}
	})

	t.Run("revision lookup failure retains completed operation", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/v1/operations/op-build":
				writeJSONTest(w, Operation{ID: "op-build", Kind: "template-build", TargetID: "tpl-family", Status: "succeeded", Result: map[string]string{"revisionId": "rev-3"}})
			case "/v1/template-revisions/rev-3":
				w.WriteHeader(http.StatusNotFound)
				writeJSONTest(w, map[string]any{"error": APIError{Code: "NOT_FOUND", Message: "not found"}})
			default:
				http.NotFound(w, r)
			}
		}))
		defer server.Close()
		client := testClient(t, server, "")
		_, err := client.WaitBuildRevision(context.Background(), &Operation{ID: "op-build", Kind: "template-build", Status: "queued"})
		var opErr *OperationError
		if !errors.As(err, &opErr) || opErr.Operation == nil || opErr.Operation.Result["revisionId"] != "rev-3" {
			t.Fatalf("expected operation error preserving revision result, got %#v", err)
		}
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Code != "NOT_FOUND" {
			t.Fatalf("expected wrapped API error, got %#v", err)
		}
	})

	t.Run("rejects non-build operation", func(t *testing.T) {
		var gets atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { gets.Add(1) }))
		defer server.Close()
		client := testClient(t, server, "")
		if _, err := client.WaitBuildRevision(context.Background(), &Operation{ID: "op-runtime", Kind: "create"}); err == nil {
			t.Fatal("accepted runtime operation")
		}
		if gets.Load() != 0 {
			t.Fatal("made request for runtime operation")
		}
	})
}

func TestTemplateBuildOperationDecodesPublicFields(t *testing.T) {
	var op Operation
	body := `{"id":"op-1","kind":"template-build","targetId":"tpl-1","status":"running","version":4,"input":{"userVersion":"1.01","baseImage":"alpine:3","profileId":"small","buildCommand":"npm i -g codex"},"createdAt":"2026-01-02T03:04:05Z","startedAt":"2026-01-02T03:05:05Z","updatedAt":"2026-01-02T03:06:05Z"}`
	if err := json.Unmarshal([]byte(body), &op); err != nil {
		t.Fatal(err)
	}
	if op.Input == nil || op.Input.UserVersion != "1.01" || op.Input.BuildCommand != "npm i -g codex" || op.StartedAt == nil || op.UpdatedAt.IsZero() {
		t.Fatalf("operation fields were not decoded: %+v", op)
	}
	var capabilities Capabilities
	if err := json.Unmarshal([]byte(`{"diskArchives":true,"protectedTools":true,"credentialBatch":true,"internalServices":true,"rootDebug":true,"sharedDirectory":true,"mountedToolRuntime":true,"mountedDebugHome":true,"persistentHome":true}`), &capabilities); err != nil {
		t.Fatal(err)
	}
	if !capabilities.DiskArchives || !capabilities.ProtectedTools || !capabilities.CredentialBatch || !capabilities.InternalServices || !capabilities.RootDebug || !capabilities.SharedDirectory || !capabilities.MountedToolRuntime || !capabilities.MountedDebugHome || !capabilities.PersistentHome {
		t.Fatalf("capability fields missing: %+v", capabilities)
	}
}

func TestManagementValidationRejectsInvalidOptionsWithoutRequests(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); http.NotFound(w, r) }))
	defer server.Close()
	client := testClient(t, server, "")
	badLimit := TemplateListOptions{Limit: 501}
	badOffset := RevisionListOptions{Offset: -1}
	if _, err := client.ListTemplates(context.Background(), badLimit); err == nil {
		t.Fatal("accepted limit above 500")
	}
	if _, err := client.ListTemplateRevisions(context.Background(), "tpl", badOffset); err == nil {
		t.Fatal("accepted negative offset")
	}
	if _, err := client.ListOperations(context.Background(), OperationListOptions{}, OperationListOptions{}); err == nil {
		t.Fatal("accepted multiple list options")
	}
	if _, err := client.GetOperationLogs(context.Background(), "op", -1, 100); err == nil {
		t.Fatal("accepted negative log offset")
	}
	if _, err := client.GetOperationLogs(context.Background(), "op", 0, 1<<20+1); err == nil {
		t.Fatal("accepted oversized log limit")
	}
	emptyDescription := ""
	if _, err := client.UpdateTemplate(context.Background(), "tpl", UpdateTemplateOptions{ExpectedGeneration: 1}); err == nil {
		t.Fatal("accepted empty template patch")
	}
	if _, err := client.UpdateTemplate(context.Background(), "tpl", UpdateTemplateOptions{Description: &emptyDescription}); err == nil {
		t.Fatal("accepted template patch without generation")
	}
	if _, err := client.UpdateTemplateRevision(context.Background(), "rev", UpdateRevisionOptions{ExpectedGeneration: 1}); err == nil {
		t.Fatal("accepted empty revision patch")
	}
	badStatus := "deleted"
	if _, err := client.UpdateTemplateRevision(context.Background(), "rev", UpdateRevisionOptions{Status: &badStatus, ExpectedGeneration: 1}); err == nil {
		t.Fatal("accepted invalid revision status")
	}
	if requests.Load() != 0 {
		t.Fatalf("invalid requests sent %d requests", requests.Load())
	}
}

func TestSetTemplateStableRevisionCanClearStableRevision(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPatch || r.URL.Path != "/v1/templates/tpl" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
		}
		var body UpdateTemplateOptions
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.ExpectedGeneration != 7 || body.StableRevisionID == nil || *body.StableRevisionID != "" {
			t.Errorf("stable clear patch = %+v", body)
		}
		writeJSONTest(w, Template{ID: "tpl", Generation: 8})
	}))
	defer server.Close()
	client := testClient(t, server, "")
	if _, err := client.SetTemplateStableRevision(context.Background(), "tpl", "", 7); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests = %d, want 1", requests.Load())
	}
}

func TestRunAndRunArgvSendDistinctCommandContracts(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Commands) (*CommandResult, error)
		want []string
	}{
		{name: "shell", run: func(c *Commands) (*CommandResult, error) {
			return c.Run(context.Background(), "printf '%s' 'a b; $HOME'", &CommandOptions{Cwd: "work", Env: map[string]string{"X": "a b"}, Timeout: 1500 * time.Millisecond, IdempotencyKey: "shell-key"})
		}, want: []string{"/bin/sh", "-c", "printf '%s' 'a b; $HOME'"}},
		{name: "argv", run: func(c *Commands) (*CommandResult, error) {
			return c.RunArgv(context.Background(), []string{"/usr/bin/printf", "%s", "a b; $HOME"}, &CommandOptions{IdempotencyKey: "argv-key"})
		}, want: []string{"/usr/bin/printf", "%s", "a b; $HOME"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var postCount atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assertHeaders(t, r)
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/v1/boxes/box-1/execs":
					postCount.Add(1)
					var body struct {
						Argv               []string          `json:"argv"`
						ExpectedGeneration uint64            `json:"expectedGeneration"`
						Cwd                string            `json:"cwd"`
						Env                map[string]string `json:"env"`
						TimeoutMS          int64             `json:"timeoutMs"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if fmt.Sprint(body.Argv) != fmt.Sprint(tc.want) {
						t.Errorf("argv = %#v, want %#v", body.Argv, tc.want)
					}
					if body.ExpectedGeneration != 9 {
						t.Errorf("generation = %d", body.ExpectedGeneration)
					}
					if tc.name == "shell" && (body.Cwd != "work" || body.Env["X"] != "a b" || body.TimeoutMS != 1500) {
						t.Errorf("command options = %+v", body)
					}
					wantKey := tc.name + "-key"
					if got := r.Header.Get("Idempotency-Key"); got != wantKey {
						t.Errorf("key = %q, want %q", got, wantKey)
					}
					writeJSONTest(w, Operation{ID: "op-exec", Kind: "exec", Status: "succeeded", Result: map[string]string{"execId": "exec-1"}})
				case r.Method == http.MethodGet && r.URL.Path == "/v1/execs/exec-1":
					writeJSONTest(w, Execution{ID: "exec-1", State: "exited", Result: &CommandResult{Stdout: "ok", ExitCode: 0}})
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := testClient(t, server, "")
			sandbox := client.sandbox(SandboxInfo{ID: "box-1", Generation: 9})
			result, err := tc.run(sandbox.Commands)
			if err != nil {
				t.Fatal(err)
			}
			if result.Stdout != "ok" || result.ExitCode != 0 {
				t.Fatalf("result = %+v", result)
			}
			if postCount.Load() != 1 {
				t.Fatalf("exec POST count = %d", postCount.Load())
			}
		})
	}
}

func TestNonzeroExitReturnsOutputAndCommandExitError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/boxes/box/execs":
			writeJSONTest(w, Operation{ID: "op", Status: "succeeded", Result: map[string]string{"execId": "exec"}})
		case "/v1/execs/exec":
			writeJSONTest(w, Execution{ID: "exec", State: "exited", Result: &CommandResult{Stdout: "partial", Stderr: "bad", ExitCode: 7}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := testClient(t, server, "")
	sandbox := client.sandbox(SandboxInfo{ID: "box", Generation: 1})
	result, err := sandbox.Commands.RunArgv(context.Background(), []string{"/bin/false"}, nil)
	var exitErr *CommandExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("error = %v, want CommandExitError", err)
	}
	if result == nil || result.Stdout != "partial" || result.Stderr != "bad" || result.ExitCode != 7 || exitErr.Result != result {
		t.Fatalf("result/error = %+v / %+v", result, exitErr)
	}
}

func TestFailedOperationAndUnknownExecutionDoNotResubmit(t *testing.T) {
	for _, tc := range []struct {
		name, status   string
		errorBody      *APIError
		executionState string
		wantCode       string
	}{
		{name: "operation failure", status: "failed", errorBody: &APIError{Code: "CONFLICT", Message: "failed"}},
		{name: "unknown execution", status: "succeeded", executionState: "unknown", wantCode: "UNKNOWN_OUTCOME"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var posts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost:
					posts.Add(1)
					writeJSONTest(w, Operation{ID: "op", Status: tc.status, Error: tc.errorBody, Result: map[string]string{"execId": "exec"}})
				case r.Method == http.MethodGet && r.URL.Path == "/v1/execs/exec":
					writeJSONTest(w, Execution{ID: "exec", State: tc.executionState})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := testClient(t, server, "")
			cmd, err := client.sandbox(SandboxInfo{ID: "box", Generation: 1}).Commands.StartArgv(context.Background(), []string{"/bin/true"}, &CommandOptions{IdempotencyKey: "stable"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = cmd.Wait(context.Background())
			var apiErr *APIError
			if tc.wantCode != "" {
				if !errors.As(err, &apiErr) || apiErr.Code != tc.wantCode {
					t.Fatalf("error = %v, want %s", err, tc.wantCode)
				}
			} else {
				var opErr *OperationError
				if !errors.As(err, &opErr) || opErr.Operation.ID != "op" {
					t.Fatalf("error = %v, want OperationError", err)
				}
			}
			if posts.Load() != 1 {
				t.Fatalf("mutations submitted %d times", posts.Load())
			}
		})
	}
}

func TestCanceledWaitCanBeRepeatedWithoutResubmitting(t *testing.T) {
	var posts, polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost:
			posts.Add(1)
			writeJSONTest(w, Operation{ID: "op", Status: "running", Result: map[string]string{"execId": "exec"}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op":
			if r.URL.Query().Get("waitMs") != "10000" {
				t.Errorf("waitMs = %q", r.URL.Query().Get("waitMs"))
			}
			if polls.Add(1) == 1 {
				<-r.Context().Done()
				return
			}
			writeJSONTest(w, Operation{ID: "op", Status: "succeeded", Result: map[string]string{"execId": "exec"}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/execs/exec":
			writeJSONTest(w, Execution{ID: "exec", State: "exited", Result: &CommandResult{Stdout: "done", ExitCode: 0}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := testClient(t, server, "")
	cmd, err := client.sandbox(SandboxInfo{ID: "box", Generation: 1}).Commands.Start(context.Background(), "true", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = cmd.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first wait error = %v", err)
	}
	result, err := cmd.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Stdout != "done" || posts.Load() != 1 {
		t.Fatalf("result=%+v POSTs=%d", result, posts.Load())
	}
}

func TestHTTPStructuredAndFallbackErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		code   string
	}{
		{name: "structured expired", status: http.StatusGone, body: `{"error":{"code":"RESULT_EXPIRED","message":"gone"}}`, code: "RESULT_EXPIRED"},
		{name: "non-json", status: http.StatusBadGateway, body: "upstream unavailable", code: "HTTP_ERROR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := testClient(t, server, "")
			_, err := client.GetExecution(context.Background(), "exec")
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Code != tc.code || apiErr.StatusCode != tc.status {
				t.Fatalf("error = %#v, want %s/%d", err, tc.code, tc.status)
			}
		})
	}
}

func TestSubmitErrorRetainsIdempotencyKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Idempotency-Key"); got != "recover-this" {
			t.Errorf("key = %q", got)
		}
		http.Error(w, "temporary", http.StatusBadGateway)
	}))
	defer server.Close()
	client := testClient(t, server, "")
	_, err := client.CreateAsync(context.Background(), CreateOptions{TemplateID: "tpl", OwnerKey: "o", IdempotencyKey: "recover-this"})
	var requestErr *RequestError
	if !errors.As(err, &requestErr) || requestErr.IdempotencyKey != "recover-this" {
		t.Fatalf("error = %#v", err)
	}
}

func TestInvalidCommandTimeoutsAreRejectedBeforeRequest(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); http.NotFound(w, r) }))
	defer server.Close()
	commands := testClient(t, server, "").sandbox(SandboxInfo{ID: "box", Generation: 1}).Commands
	for _, timeout := range []time.Duration{-time.Millisecond, 500 * time.Microsecond, 5*time.Minute + time.Millisecond} {
		if _, err := commands.Start(context.Background(), "true", &CommandOptions{Timeout: timeout}); err == nil {
			t.Errorf("timeout %s accepted", timeout)
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("invalid options sent %d requests", requests.Load())
	}
}
