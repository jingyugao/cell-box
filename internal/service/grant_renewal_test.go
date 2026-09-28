package service

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
	"github.com/gorilla/websocket"
)

func TestGrantRenewalKeepsExistingWebSocketPastOriginalExpiry(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	f := newGatewayFixture(t, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			kind, message, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err = conn.WriteMessage(kind, message); err != nil {
				return
			}
		}
	})
	grant, raw := f.addGrant(t, f.route, 2)
	server := httptest.NewServer(f.service.Handler())
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/s/" + f.route.ID + "/ws"
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": []string{"Bearer " + raw}})
	if err != nil {
		t.Fatalf("websocket dial: %v (%v)", err, response)
	}
	defer conn.Close()
	status, body := f.call(t, "PATCH", "/v1/grants/"+grant.ID, testClientToken, "", map[string]any{"ttlSeconds": 5})
	wantStatus(t, status, 200, body)
	renewed := decodeResponse[Grant](t, body)
	if !renewed.ExpiresAt.After(grant.ExpiresAt) || renewed.ID != grant.ID || renewed.Revoked {
		t.Fatalf("unexpected renewal: before=%v after=%v", grant, renewed)
	}
	if wait := time.Until(grant.ExpiresAt.Add(150 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte("after-original-expiry")); err != nil {
		t.Fatalf("renewed websocket closed: %v", err)
	}
	_, message, err := conn.ReadMessage()
	if err != nil || string(message) != "after-original-expiry" {
		t.Fatalf("renewed websocket failed: %q %v", message, err)
	}
}

func TestGrantRenewalRejectsOtherOwnerExpiredAndRevoked(t *testing.T) {
	f := newGatewayFixture(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	grant, _ := f.addGrant(t, f.route, 300)
	path := "/v1/grants/" + grant.ID
	status, body := f.call(t, "PATCH", path, otherClientToken, "", map[string]any{"ttlSeconds": 300})
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "PATCH", path, testClientToken, "", map[string]any{"ttlSeconds": 0})
	wantStatus(t, status, 400, body)
	status, body = f.call(t, "PATCH", path, testClientToken, "", map[string]any{"ttlSeconds": 1})
	wantStatus(t, status, 200, body)
	if renewed := decodeResponse[Grant](t, body); !renewed.ExpiresAt.Equal(grant.ExpiresAt) {
		t.Fatal("renewal shortened an existing grant")
	}
	if err := f.service.store.Update(func(st *State) error {
		record := st.Grants[grant.ID]
		record.Grant.ExpiresAt = time.Now().Add(-time.Second)
		st.Grants[grant.ID] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	status, body = f.call(t, "PATCH", path, testClientToken, "", map[string]any{"ttlSeconds": 300})
	wantStatus(t, status, 409, body)
	revoked, _ := f.addGrant(t, f.route, 300)
	status, body = f.call(t, "DELETE", "/v1/grants/"+revoked.ID, testClientToken, "", nil)
	wantStatus(t, status, 204, body)
	status, body = f.call(t, "PATCH", "/v1/grants/"+revoked.ID, testClientToken, "", map[string]any{"ttlSeconds": 300})
	wantStatus(t, status, 409, body)
}

func TestProfilesExposeOnlySupportedRuntimeBehaviorKinds(t *testing.T) {
	f := newCoreFixture(t)
	f.config.Profiles = append(f.config.Profiles, Profile{ID: "profile-k8s", Provider: "resumable-k8s-pod",
		Image: "example.invalid/agent@sha256:" + strings.Repeat("b", 64), Namespace: "test", NodeName: "test-node",
		CPU: 2, MemoryMiB: 1024, Guest: guestapi.DefaultConfig(), Clients: []string{"client-a"}})
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	f.service = nil
	var err error
	f.service, err = New(f.config, map[string]boxprovider.Provider{
		"docker": f.provider, "resumable-k8s-pod": f.provider,
	})
	if err != nil {
		t.Fatal(err)
	}
	status, body := f.call(t, "GET", "/v1/profiles", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	type profileResponse struct {
		Provider, Runtime, Behavior, Kind, Image, Workspace string
		CPU                                                 float64
		MemoryMiB                                           int64
		Agent                                               guestapi.Identity
	}
	profiles := decodeResponse[[]profileResponse](t, body)
	if len(profiles) != 2 {
		t.Fatalf("expected two admitted profiles: %s", body)
	}
	for _, profile := range profiles {
		switch profile.Provider {
		case "docker":
			if profile.Runtime != "docker" || profile.Behavior != "normal" || profile.Kind != "docker-normal" {
				t.Fatalf("wrong docker combination: %+v", profile)
			}
		case "resumable-k8s-pod":
			if profile.Runtime != "k8s" || profile.Behavior != "resumable" || profile.Kind != "k8s-resumable" {
				t.Fatalf("wrong k8s combination: %+v", profile)
			}
		default:
			t.Fatalf("unexpected provider: %+v", profile)
		}
		if profile.Image == "" || profile.Workspace == "" || profile.CPU < 1 || profile.MemoryMiB < 1 ||
			profile.Agent.UID != 11000 || profile.Agent.GID != 11000 {
			t.Fatalf("profile metadata missing: %+v", profile)
		}
	}
}
