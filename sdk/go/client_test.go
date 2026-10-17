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

func TestTemplateLifecycleUsesVersionedTemplateEndpoints(t *testing.T) {
	templateBody := `{"id":"tpl-1","name":"python","version":"v1","profileId":"small","image":"python:3","cpu":1,"memoryMiB":512,"warmSlots":2,"createdAt":"2026-01-02T03:04:05Z"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertHeaders(t, r)
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates":
			var body CreateTemplateOptions
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.Name != "python" || body.Version != "v1" || body.ProfileID != "small" || body.ImportedImageID != "image-1" || body.WarmSlots != 2 {
				t.Errorf("create template body = %+v", body)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(templateBody))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/templates":
			writeJSONTest(w, []Template{{ID: "tpl-1", Name: "python", Version: "v1"}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/templates/tpl-1":
			_, _ = w.Write([]byte(templateBody))
		case r.Method == http.MethodPut && r.URL.Path == "/v1/templates/tpl-1/prewarm":
			var body struct {
				WarmSlots int `json:"warmSlots"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.WarmSlots != 0 {
				t.Errorf("warmSlots = %d, want 0", body.WarmSlots)
			}
			_, _ = w.Write([]byte(templateBody))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/templates/tpl-1":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := testClient(t, server, "")

	template, err := client.CreateTemplate(context.Background(), CreateTemplateOptions{Name: "python", Version: "v1", ProfileID: "small", ImportedImageID: "image-1", WarmSlots: 2})
	if err != nil || template.ID != "tpl-1" || template.MemoryMiB != 512 {
		t.Fatalf("CreateTemplate = %+v, %v", template, err)
	}
	listed, err := client.ListTemplates(context.Background())
	if err != nil || len(listed) != 1 || listed[0].ID != "tpl-1" {
		t.Fatalf("ListTemplates = %+v, %v", listed, err)
	}
	got, err := client.GetTemplate(context.Background(), "tpl-1")
	if err != nil || got.Version != "v1" {
		t.Fatalf("GetTemplate = %+v, %v", got, err)
	}
	updated, err := client.SetTemplateWarmSlots(context.Background(), "tpl-1", 0)
	if err != nil || updated.ID != "tpl-1" {
		t.Fatalf("SetTemplateWarmSlots = %+v, %v", updated, err)
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
	if _, err := client.CreateTemplate(context.Background(), CreateTemplateOptions{Name: "python", Version: "v1", ProfileID: "small", WarmSlots: -1}); err == nil {
		t.Fatal("CreateTemplate accepted negative warm slots")
	}
	if _, err := client.SetTemplateWarmSlots(context.Background(), "tpl", -1); err == nil {
		t.Fatal("SetTemplateWarmSlots accepted negative slots")
	}
	if requests.Load() != 0 {
		t.Fatalf("invalid requests sent %d requests", requests.Load())
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
