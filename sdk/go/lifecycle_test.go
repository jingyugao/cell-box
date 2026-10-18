package cellbox

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestResumeReturnsFreshGenerationWithoutMutatingOriginalHandle(t *testing.T) {
	var resumes, execs atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertHeaders(t, r)
		switch r.URL.Path {
		case "/v1/boxes/box-1:resume":
			resumes.Add(1)
			var body struct{ ExpectedGeneration uint64 }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ExpectedGeneration != 3 {
				t.Errorf("resume generation: %+v, %v", body, err)
			}
			writeJSONTest(w, Operation{ID: "op-resume", Status: "succeeded", Box: &SandboxInfo{ID: "box-1", Generation: 4, Phase: "running"}})
		case "/v1/boxes/box-1/execs":
			execs.Add(1)
			var body struct{ ExpectedGeneration uint64 }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ExpectedGeneration != 4 {
				t.Errorf("exec must use resumed generation: %+v, %v", body, err)
			}
			writeJSONTest(w, Operation{ID: "op-exec", Status: "succeeded", Result: map[string]string{"execId": "exec-1"}})
		case "/v1/execs/exec-1":
			writeJSONTest(w, Execution{State: "exited", Result: &CommandResult{Stdout: "ok"}})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	original := testClient(t, server, "").sandbox(SandboxInfo{ID: "box-1", Generation: 3})
	resumed, err := original.Resume(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if original.Generation != 3 || resumed.Generation != 4 || original == resumed || resumed.Files == nil {
		t.Fatalf("original=%+v resumed=%+v", original, resumed)
	}
	result, err := resumed.Commands.Run(context.Background(), "echo ok", nil)
	if err != nil || result.Stdout != "ok" || resumes.Load() != 1 || execs.Load() != 1 {
		t.Fatalf("result=%+v err=%v resumes=%d execs=%d", result, err, resumes.Load(), execs.Load())
	}
}

func TestLifecycleRefreshFailureRetainsOperationAndUnprovisionedBoxCanBeDestroyed(t *testing.T) {
	var destroyed atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/boxes/box-1:activate":
			writeJSONTest(w, Operation{ID: "op-activate", Status: "succeeded", TargetID: "box-1"})
		case "/v1/boxes/box-1":
			w.WriteHeader(http.StatusServiceUnavailable)
			writeJSONTest(w, map[string]any{"error": APIError{Code: "UNAVAILABLE", Message: "temporarily unavailable"}})
		case "/v1/boxes/failed-box:destroy":
			destroyed.Add(1)
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if _, ok := body["expectedGeneration"]; ok {
				t.Error("unprovisioned box must omit the zero generation")
			}
			writeJSONTest(w, Operation{ID: "op-destroy", Status: "succeeded"})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := testClient(t, server, "")
	_, err := c.sandbox(SandboxInfo{ID: "box-1", Generation: 1}).Activate(context.Background())
	var opErr *OperationError
	var apiErr *APIError
	if !errors.As(err, &opErr) || opErr.Operation.ID != "op-activate" || !errors.As(err, &apiErr) || apiErr.StatusCode != 503 {
		t.Fatalf("lost accepted operation or underlying error: %v", err)
	}
	failed := c.sandbox(SandboxInfo{ID: "failed-box"})
	if _, err := failed.Resume(context.Background()); err == nil {
		t.Fatal("resume accepted a zero generation")
	}
	if err := failed.Destroy(context.Background()); err != nil || destroyed.Load() != 1 {
		t.Fatalf("failed sandbox cleanup: %v, destroys=%d", err, destroyed.Load())
	}
}
