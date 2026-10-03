package service

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestInternalServiceScopesClientAndFencesLiveWebSocketWithoutMetadataWrites(t *testing.T) {
	upgrader := websocket.Upgrader{}
	f := newGatewayFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Cellbox-Upstream-Authorization") != "" || r.Header.Get("Proxy-Authorization") != "" {
			t.Error("control-plane authorization reached the application")
		}
		if r.URL.Path == "/proxy/8080/ws" {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			for {
				kind, data, err := conn.ReadMessage()
				if err != nil {
					return
				}
				if conn.WriteMessage(kind, data) != nil {
					return
				}
			}
		}
		_, _ = w.Write([]byte(r.URL.EscapedPath() + "?" + r.URL.RawQuery))
	})
	prefix := "/v1/boxes/" + f.box.ID + "/services/8080"
	status, body := f.call(t, "GET", prefix+"/", otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "GET", "/v1/boxes/"+f.box.ID+"/services/40000/", testClientToken, "", nil)
	wantStatus(t, status, 400, body)
	req := httptest.NewRequest("GET", prefix+"/", nil)
	req.Header.Set("Origin", "https://browser.example.test")
	rec := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(rec, req)
	wantStatus(t, rec.Code, 200, rec.Body.Bytes())

	status, body = f.call(t, "GET", prefix+"/nested/a%2Fb?q=value", "", "", nil)
	wantStatus(t, status, 200, body)
	if string(body) != "/proxy/8080/nested/a%2Fb?q=value" {
		t.Fatalf("wrong proxy path: %s", body)
	}
	server := httptest.NewServer(f.service.Handler())
	defer server.Close()
	conn, res, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+prefix+"/ws", http.Header{"X-Cellbox-Upstream-Authorization": {"injected"}})
	if err != nil {
		t.Fatalf("internal websocket: %v (%v)", err, res)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte("ready")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil || string(data) != "ready" {
		t.Fatalf("websocket RPC failed: %s %v", data, err)
	}
	status, body = f.call(t, "POST", "/v1/boxes/"+f.box.ID+":destroy", testClientToken, "internal-busy", nil)
	wantStatus(t, status, 409, body)
	if err := f.service.store.View(func(st State) error {
		if len(st.Routes) != 1 || len(st.Leases) != 0 {
			t.Fatal("internal access created durable access records")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		f.service.mu.Lock()
		busy := f.service.streamBusy(f.box.ID) != nil
		f.service.mu.Unlock()
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("closed websocket kept lifecycle busy")
		}
		time.Sleep(time.Millisecond)
	}
}
