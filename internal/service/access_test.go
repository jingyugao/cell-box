package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	core.config.Clients[0].AuthorizeURL = "https://login.example.test/authorize"
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
func (f *gatewayFixture) addGrant(t *testing.T, route Route, ttl int) (Grant, string) {
	t.Helper()
	status, body := f.call(t, "POST", "/v1/routes/"+route.ID+"/grants", testClientToken, "", map[string]any{"subject": "user-1", "ttlSeconds": ttl})
	wantStatus(t, status, 201, body)
	var response struct {
		Grant Grant  `json:"grant"`
		Token string `json:"token"`
	}
	response = decodeResponse[struct {
		Grant Grant  `json:"grant"`
		Token string `json:"token"`
	}](t, body)
	if response.Token == "" {
		t.Fatal("grant token missing")
	}
	return response.Grant, response.Token
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

func TestGatewayGrantIsScopedToRouteAndRevocation(t *testing.T) {
	f := newGatewayFixture(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(r.URL.Path)) })
	second := f.addRoute(t, 8081)
	grant, raw := f.addGrant(t, f.route, 300)
	header := http.Header{"Authorization": []string{"Bearer " + raw}}
	rec := f.gatewayCall(t, "GET", routeHost(f.route), "/hello", header)
	wantStatus(t, rec.Code, 200, rec.Body.Bytes())
	if rec.Body.String() != "/proxy/8080/hello" {
		t.Fatalf("wrong target port/path: %q", rec.Body.String())
	}
	rec = f.gatewayCall(t, "GET", routeHost(second), "/hello", header)
	wantStatus(t, rec.Code, 401, rec.Body.Bytes())
	wrongOrigin := header.Clone()
	wrongOrigin.Set("Origin", "https://elsewhere.example.test")
	rec = f.gatewayCall(t, "GET", routeHost(f.route), "/hello", wrongOrigin)
	wantStatus(t, rec.Code, 403, rec.Body.Bytes())
	status, body := f.call(t, "POST", "/v1/routes/"+f.route.ID+"/grants", otherClientToken, "", map[string]any{"subject": "intruder"})
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "DELETE", "/v1/grants/"+grant.ID, otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "DELETE", "/v1/grants/"+grant.ID, testClientToken, "", nil)
	wantStatus(t, status, 204, body)
	rec = f.gatewayCall(t, "GET", routeHost(f.route), "/hello", header)
	wantStatus(t, rec.Code, 401, rec.Body.Bytes())
	_, expiring := f.addGrant(t, f.route, 300)
	if err := f.service.store.Update(func(st *State) error {
		for id, g := range st.Grants {
			if g.Grant.RouteID == f.route.ID && !g.Grant.Revoked {
				g.Grant.ExpiresAt = time.Now().Add(-time.Second)
				st.Grants[id] = g
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	rec = f.gatewayCall(t, "GET", routeHost(f.route), "/hello", http.Header{"Authorization": []string{"Bearer " + expiring}})
	wantStatus(t, rec.Code, 401, rec.Body.Bytes())
}

func TestGatewayBrowserCodeBoundToTransactionRouteAndOneUse(t *testing.T) {
	f := newGatewayFixture(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("application")) })
	second := f.addRoute(t, 8081)
	start := f.gatewayCall(t, "GET", routeHost(f.route), "/work?view=1", http.Header{"Accept": []string{"text/html"}})
	wantStatus(t, start.Code, 303, start.Body.Bytes())
	transaction := cookieNamed(start.Result().Cookies(), transactionCookie)
	if transaction == nil || !transaction.Secure || !transaction.HttpOnly {
		t.Fatal("browser transaction cookie missing secure attributes")
	}
	authorizeURL, err := url.Parse(start.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	requestID := authorizeURL.Query().Get("request_id")
	if requestID == "" {
		t.Fatal("authorization request ID missing")
	}
	status, body := f.call(t, "POST", "/v1/access-requests/"+requestID+":approve", testClientToken, "", map[string]any{"subject": "browser-user", "ttlSeconds": 300})
	wantStatus(t, status, 200, body)
	var approval struct {
		RedirectURL string `json:"redirectUrl"`
	}
	approval = decodeResponse[struct {
		RedirectURL string `json:"redirectUrl"`
	}](t, body)
	callback, err := url.Parse(approval.RedirectURL)
	if err != nil {
		t.Fatal(err)
	}
	if callback.Query().Get("code") == "" {
		t.Fatal("approval code missing")
	}
	callbackPath := callback.RequestURI()
	rec := f.gatewayCall(t, "GET", routeHost(f.route), callbackPath, nil)
	wantStatus(t, rec.Code, 401, rec.Body.Bytes())
	rec = f.gatewayCall(t, "GET", routeHost(f.route), callbackPath, nil, &http.Cookie{Name: transactionCookie, Value: "wrong"})
	wantStatus(t, rec.Code, 401, rec.Body.Bytes())
	badCode := *callback
	query := badCode.Query()
	query.Set("code", "wrong")
	badCode.RawQuery = query.Encode()
	rec = f.gatewayCall(t, "GET", routeHost(f.route), badCode.RequestURI(), nil, transaction)
	wantStatus(t, rec.Code, 401, rec.Body.Bytes())
	rec = f.gatewayCall(t, "GET", routeHost(second), callbackPath, nil, transaction)
	wantStatus(t, rec.Code, 401, rec.Body.Bytes())
	rec = f.gatewayCall(t, "GET", routeHost(f.route), callbackPath, nil, transaction)
	wantStatus(t, rec.Code, 303, rec.Body.Bytes())
	if rec.Header().Get("Location") != "/work?view=1" {
		t.Fatalf("lost original return path: %q", rec.Header().Get("Location"))
	}
	session := cookieNamed(rec.Result().Cookies(), sessionCookie)
	if session == nil || !session.Secure || !session.HttpOnly {
		t.Fatal("session cookie missing secure attributes")
	}
	rec = f.gatewayCall(t, "GET", routeHost(f.route), callbackPath, nil, transaction)
	wantStatus(t, rec.Code, 401, rec.Body.Bytes())
	rec = f.gatewayCall(t, "GET", routeHost(f.route), "/work", nil, session)
	wantStatus(t, rec.Code, 200, rec.Body.Bytes())
	if rec.Body.String() != "application" {
		t.Fatalf("session did not reach application: %q", rec.Body.String())
	}
}
func cookieNamed(cookies []*http.Cookie, name string) *http.Cookie {
	for _, cookie := range cookies {
		if cookie.Name == name {
			return cookie
		}
	}
	return nil
}

func TestGatewayStripsControlHeadersCookiesAndResponseCookie(t *testing.T) {
	seen := make(chan http.Header, 1)
	f := newGatewayFixture(t, func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		w.Header().Add("Set-Cookie", "__Host-cellbox-session=forged; Secure; Path=/")
		w.Header().Add("Set-Cookie", "app=ok; Path=/")
		w.WriteHeader(200)
	})
	_, raw := f.addGrant(t, f.route, 300)
	headers := http.Header{}
	headers.Set("Authorization", "Bearer "+raw)
	headers.Set("Cookie", "app-input=keep; __Host-cellbox-session=spoof; __Host-cellbox-transaction=spoof")
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
	for _, value := range rec.Header().Values("Set-Cookie") {
		if strings.HasPrefix(value, "__Host-cellbox-") {
			t.Fatalf("guest injected control cookie: %q", value)
		}
	}
	if !strings.Contains(strings.Join(rec.Header().Values("Set-Cookie"), ";"), "app=ok") {
		t.Fatal("application cookie was lost")
	}
}

func TestGatewayBrowserSessionDoesNotForwardGatewayBearer(t *testing.T) {
	seen := make(chan http.Header, 3)
	f := newGatewayFixture(t, func(w http.ResponseWriter, r *http.Request) { seen <- r.Header.Clone(); w.WriteHeader(204) })
	grant, grantToken := f.addGrant(t, f.route, 300)
	sessionRaw := "test-browser-session"
	if err := f.service.store.Update(func(st *State) error {
		digest := hash(sessionRaw)
		st.Sessions[digest] = sessionRecord{TokenHash: digest, GrantID: grant.ID}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cookie := &http.Cookie{Name: sessionCookie, Value: sessionRaw}
	for _, tc := range []struct{ name, bearer, wantAppAuth string }{
		{name: "grant", bearer: grantToken},
		{name: "client", bearer: testClientToken},
		{name: "application", bearer: "application-user-token", wantAppAuth: "Bearer application-user-token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := f.gatewayCall(t, "GET", routeHost(f.route), "/session", http.Header{"Authorization": []string{"Bearer " + tc.bearer}}, cookie)
			wantStatus(t, rec.Code, 204, rec.Body.Bytes())
			headers := <-seen
			if got := headers.Get("Authorization"); got != "Bearer "+testGuestToken {
				t.Fatalf("guest authorization = %q", got)
			}
			if got := headers.Get("X-Cellbox-Upstream-Authorization"); got != tc.wantAppAuth {
				t.Fatalf("forwarded application authorization = %q, want %q", got, tc.wantAppAuth)
			}
			if strings.Contains(headers.Get("Cookie"), sessionRaw) {
				t.Fatal("browser session cookie reached guest")
			}
		})
	}
}

func TestGatewayWebSocketRevocationAndLifecycleBusy(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	f := newGatewayFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/proxy/8080/ws" {
			http.NotFound(w, r)
			return
		}
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
	grant, raw := f.addGrant(t, f.route, 300)
	server := httptest.NewServer(f.service.Handler())
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/s/" + f.route.ID + "/ws"
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": []string{"Bearer " + raw}})
	if err != nil {
		t.Fatalf("websocket dial: %v (%v)", err, response)
	}
	defer conn.Close()
	if err = conn.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	_, message, err := conn.ReadMessage()
	if err != nil || string(message) != "hello" {
		t.Fatalf("websocket echo: %q %v", message, err)
	}
	status, body := f.call(t, "POST", "/v1/boxes/"+f.box.ID+":destroy", testClientToken, "destroy-while-ws", nil)
	wantStatus(t, status, 409, body)
	status, body = f.call(t, "POST", "/v1/boxes/"+f.box.ID+"/leases", testClientToken, "", map[string]any{"purpose": "browser session", "ttlSeconds": 60})
	wantStatus(t, status, 201, body)
	lease := decodeResponse[Lease](t, body)
	status, body = f.call(t, "DELETE", "/v1/grants/"+grant.ID, testClientToken, "", nil)
	wantStatus(t, status, 204, body)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err = conn.ReadMessage(); err == nil {
		t.Fatal("revoked websocket remained open")
	}
	status, body = f.call(t, "POST", "/v1/boxes/"+f.box.ID+":destroy", testClientToken, "destroy-with-lease", nil)
	wantStatus(t, status, 409, body)
	status, body = f.call(t, "DELETE", "/v1/leases/"+lease.ID, testClientToken, "", nil)
	wantStatus(t, status, 204, body)
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, body = f.call(t, "POST", "/v1/boxes/"+f.box.ID+":destroy", testClientToken, "destroy-after-ws", nil)
		if status != 409 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	f.waitOperation(t, op.ID, "succeeded")
}

func TestGatewayWebSocketExpires(t *testing.T) {
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
	_, raw := f.addGrant(t, f.route, 1)
	server := httptest.NewServer(f.service.Handler())
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/s/" + f.route.ID + "/ws"
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": []string{"Bearer " + raw}})
	if err != nil {
		t.Fatalf("websocket dial: %v (%v)", err, response)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err = conn.ReadMessage(); err == nil {
		t.Fatal("expired websocket remained open")
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
	_, grantToken := f.addGrant(t, f.route, 300)
	server := httptest.NewServer(f.service.Handler())
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/s/" + f.route.ID + "/ws"
	conn, response, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": []string{"Bearer " + grantToken}})
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
	rec := f.gatewayCall(t, "GET", routeHost(f.route), "/ping", http.Header{"Authorization": []string{"Bearer " + grantToken}})
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

func TestGatewayBrowserReturnPathLimit(t *testing.T) {
	f := newGatewayFixture(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	rec := f.gatewayCall(t, "GET", routeHost(f.route), "/"+strings.Repeat("a", 4096), http.Header{"Accept": []string{"text/html"}})
	wantStatus(t, rec.Code, 400, rec.Body.Bytes())
	if strings.Contains(rec.Header().Get("Location"), "login.example.test") {
		t.Fatal("oversized return path started browser authorization")
	}
}
