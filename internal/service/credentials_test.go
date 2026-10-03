package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"cellbox.local/cellbox/internal/guestapi"
)

func TestCredentialBatchAdmissionAndGenerationFence(t *testing.T) {
	f := newCoreFixture(t)
	f.service.config.Profiles[0].Guest.Tools = []guestapi.Tool{{ID: "tool", Executable: "/opt/cellbox/tools/tool", CredentialEnv: map[string]string{"FIRST": "first", "SECOND": "second"}}}
	_, box := f.createBox(t, "credential-batch")
	var calls atomic.Int32
	var change, fail atomic.Bool
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testGuestToken {
			http.Error(w, "unauthorized", 401)
			return
		}
		if r.URL.Path != "/v1/credentials" {
			t.Errorf("unexpected guest path %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		calls.Add(1)
		var batch guestapi.CredentialBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil || len(batch.Slots) != 2 {
			t.Error("incorrect forwarded batch")
			http.Error(w, "bad batch", 400)
			return
		}
		if fail.Load() {
			http.Error(w, "failed", 500)
			return
		}
		if change.Load() {
			f.provider.setExecutionID("fake-container/start-two")
		}
		w.WriteHeader(204)
	}))
	defer guest.Close()
	f.provider.mu.Lock()
	f.provider.guestURL = guest.URL
	f.provider.mu.Unlock()
	input := map[string]any{"expectedGeneration": box.Generation, "slots": map[string][]byte{"first": []byte("fixture-one"), "second": []byte("fixture-two")}}
	path := "/v1/boxes/" + box.ID + "/credentials"
	status, body := f.call(t, "PUT", path, otherClientToken, "", input)
	wantStatus(t, status, 404, body)
	input["expectedGeneration"] = box.Generation + 1
	status, body = f.call(t, "PUT", path, testClientToken, "", input)
	wantStatus(t, status, 409, body)
	input["expectedGeneration"] = box.Generation
	input["slots"] = map[string][]byte{"undeclared": []byte("fixture")}
	status, body = f.call(t, "PUT", path, testClientToken, "", input)
	wantStatus(t, status, 403, body)
	if calls.Load() != 0 {
		t.Fatal("rejected credentials reached the Guest")
	}
	input["slots"] = map[string][]byte{"first": []byte("fixture-one"), "second": []byte("fixture-two")}
	status, body = f.call(t, "PUT", path, testClientToken, "", input)
	wantStatus(t, status, 204, body)
	fail.Store(true)
	status, body = f.call(t, "PUT", path, testClientToken, "", input)
	wantStatus(t, status, 502, body)
	fail.Store(false)
	change.Store(true)
	status, body = f.call(t, "PUT", path, testClientToken, "", input)
	wantStatus(t, status, 409, body)
	if calls.Load() != 3 {
		t.Fatalf("wanted three batch requests, got %d", calls.Load())
	}
	status, body = f.call(t, "PUT", path, testClientToken, "", input)
	wantStatus(t, status, 409, body)
	if calls.Load() != 3 {
		t.Fatal("stale retry reached Guest")
	}
}
