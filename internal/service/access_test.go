package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/guestapi"
	"github.com/gorilla/websocket"
)

type gatewayFixture struct {
	*coreFixture
	box   Box
	route Route
	guest *httptest.Server
}

func newGatewayFixture(t *testing.T, upstream http.HandlerFunc) *gatewayFixture {
	t.Helper()
	core := newCoreFixture(t)
	core.config.ServiceDomain = "apps.example.test"
	core.reopen(t)
	_, box := core.createBox(t, "gateway-box")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testGuestToken {
			http.Error(w, "guest auth required", 401)
			return
		}
		upstream(w, r)
	}))
	t.Cleanup(server.Close)
	core.provider.mu.Lock()
	core.provider.guestURL = server.URL
	core.provider.mu.Unlock()
	f := &gatewayFixture{coreFixture: core, box: box, guest: server}
	f.route = f.addRoute(t, 8080)
	return f
}
func (f *gatewayFixture) addRoute(t *testing.T, port int) Route {
	t.Helper()
	status, body := f.call(t, "POST", "/v1/routes", testClientToken, "", map[string]any{"boxId": f.box.ID, "port": port})
	wantStatus(t, status, 201, body)
	return decodeResponse[Route](t, body)
}
func (f *gatewayFixture) gatewayCall(t *testing.T, method, host, path string, headers http.Header, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "https://"+host+path, nil)
	req.Host = host
	for k, values := range headers {
		for _, value := range values {
			req.Header.Add(k, value)
		}
	}
	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}
	rec := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(rec, req)
	return rec
}
func routeHost(route Route) string {
	return strings.TrimPrefix(strings.TrimSuffix(route.URL, "/"), "https://")
}

func TestGatewayNeedsNoGrantAndForwardsApplicationAuthorization(t *testing.T) {
	f := newGatewayFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Cellbox-Upstream-Authorization") != "Bearer application-token" {
			t.Error("application authorization was lost")
		}
		w.WriteHeader(http.StatusNoContent)
	})
	header := http.Header{"Authorization": {"Bearer application-token"}, "Origin": {"https://internal-ui.example.test"}}
	rec := f.gatewayCall(t, "GET", routeHost(f.route), "/app", header)
	wantStatus(t, rec.Code, http.StatusNoContent, rec.Body.Bytes())
	if rec.Header().Get("Location") != "" || len(rec.Result().Cookies()) != 0 {
		t.Fatal("preview started a browser authorization flow")
	}

}

func TestGatewayStripsControlHeadersAndForwardsApplicationCookies(t *testing.T) {
	seen := make(chan http.Header, 1)
	f := newGatewayFixture(t, func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		w.Header().Add("Set-Cookie", "app=ok; Path=/")
		w.WriteHeader(200)
	})
	headers := http.Header{}
	headers.Set("Cookie", "app-input=keep")
	headers.Set("X-Cellbox-Private", "spoof")
	headers.Set("X-Cellbox-Upstream-Authorization", "spoof")
	headers.Set("X-Forwarded-For", "spoof")
	headers.Set("Forwarded", "for=spoof")
	headers.Set("Proxy-Authorization", "spoof")
	rec := f.gatewayCall(t, "GET", routeHost(f.route), "/headers", headers)
	wantStatus(t, rec.Code, 200, rec.Body.Bytes())
	upstream := <-seen
	if upstream.Get("Authorization") != "Bearer "+testGuestToken {
		t.Fatalf("control auth was not replaced: %q", upstream.Get("Authorization"))
	}
	for _, name := range []string{"X-Cellbox-Private", "X-Cellbox-Upstream-Authorization", "X-Forwarded-For", "Forwarded", "Proxy-Authorization"} {
		if upstream.Get(name) != "" {
			t.Errorf("spoof header %s reached guest: %q", name, upstream.Get(name))
		}
	}
	if cookies := upstream.Get("Cookie"); cookies != "app-input=keep" {
		t.Fatalf("forwarded cookies = %q", cookies)
	}
	if !strings.Contains(strings.Join(rec.Header().Values("Set-Cookie"), ";"), "app=ok") {
		t.Fatal("application cookie was lost")
	}
}

func TestGatewayWebSocketClosesWithService(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	f := newGatewayFixture(t, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	server := httptest.NewServer(f.service.Handler())
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/s/" + f.route.ID + "/ws"
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v (%v)", err, response)
	}
	defer conn.Close()
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err = conn.ReadMessage(); err == nil {
		t.Fatal("websocket remained open after service shutdown")
	}
	if ne, ok := err.(interface{ Timeout() bool }); ok && ne.Timeout() {
		t.Fatalf("websocket did not close before deadline: %v", err)
	}
}

func TestGatewayPreviewCoexistsWithExecWhileDestroyRemainsBusy(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	execStarted := make(chan struct{}, 1)
	releaseExec := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseExec) }) }
	defer release()
	f := newGatewayFixture(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/exec":
			execStarted <- struct{}{}
			select {
			case <-releaseExec:
			case <-r.Context().Done():
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(guestapi.ExecResult{Stdout: "done", ExitCode: 0})
		case "/proxy/8080/ping":
			_, _ = w.Write([]byte("preview-ok"))
		case "/proxy/8080/ws":
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
		default:
			http.NotFound(w, r)
		}
	})
	server := httptest.NewServer(f.service.Handler())
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/s/" + f.route.ID + "/ws"
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("preview websocket dial: %v (%v)", err, response)
	}
	defer conn.Close()
	status, body := f.call(t, "POST", "/v1/boxes/"+f.box.ID+"/execs", testClientToken, "preview-exec", map[string]any{"argv": []string{"echo", "work"}, "expectedGeneration": f.box.Generation})
	wantStatus(t, status, 202, body)
	execOp := decodeResponse[Operation](t, body)
	select {
	case <-execStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("guest exec never started")
	}
	rec := f.gatewayCall(t, "GET", routeHost(f.route), "/ping", nil)
	wantStatus(t, rec.Code, 200, rec.Body.Bytes())
	if rec.Body.String() != "preview-ok" {
		t.Fatalf("preview HTTP body: %q", rec.Body.String())
	}
	if err = conn.WriteMessage(websocket.TextMessage, []byte("still-open")); err != nil {
		t.Fatal(err)
	}
	_, message, err := conn.ReadMessage()
	if err != nil || string(message) != "still-open" {
		t.Fatalf("preview websocket stopped during exec: %q %v", message, err)
	}
	status, body = f.call(t, "POST", "/v1/boxes/"+f.box.ID+":destroy", testClientToken, "destroy-during-exec", nil)
	wantStatus(t, status, 409, body)
	release()
	f.waitOperation(t, execOp.ID, "succeeded")
	status, body = f.call(t, "POST", "/v1/boxes/"+f.box.ID+":destroy", testClientToken, "destroy-during-ws", nil)
	wantStatus(t, status, 409, body)
	_ = conn.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, body = f.call(t, "POST", "/v1/boxes/"+f.box.ID+":destroy", testClientToken, "destroy-after-preview", nil)
		if status != 409 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	wantStatus(t, status, 202, body)
	f.waitOperation(t, decodeResponse[Operation](t, body).ID, "succeeded")
}
