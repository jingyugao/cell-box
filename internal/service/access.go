package service

import (
	"context"
	"crypto/subtle"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/guestapi"
)

const sessionCookie = "__Host-cellbox-session"
const transactionCookie = "__Host-cellbox-transaction"

func (s *Service) hostRoute(host string) string {
	if s.config.ServiceDomain == "" {
		return ""
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	suffix := "." + s.config.ServiceDomain
	host = strings.ToLower(host)
	if !strings.HasSuffix(host, suffix) {
		return ""
	}
	id := strings.TrimSuffix(host, suffix)
	if !strings.HasPrefix(id, "route-") || !validName.MatchString(id) {
		return ""
	}
	return id
}
func routeOwned(st *State, client, id string) (Route, error) {
	route, ok := st.Routes[id]
	if !ok {
		return route, apiError("NOT_FOUND", "Route not found")
	}
	_, err := owned(st, client, route.BoxID)
	return route, err
}
func (s *Service) createRoute(w http.ResponseWriter, r *http.Request) {
	var in struct {
		BoxID string `json:"boxId"`
		Port  int    `json:"port"`
	}
	if err := decode(w, r, &in); err != nil {
		fail(w, err)
		return
	}
	if in.Port < 1 || in.Port > 65535 || in.Port == guestapi.Port {
		fail(w, apiError("INVALID_REQUEST", "Port must be 1..65535 excluding the private guest control port"))
		return
	}
	route := Route{ID: randomID("route-"), BoxID: in.BoxID, Port: in.Port}
	if s.config.ServiceDomain != "" {
		route.URL = "https://" + route.ID + "." + s.config.ServiceDomain + "/"
	} else if s.config.PublicURL != "" {
		route.URL = strings.TrimRight(s.config.PublicURL, "/") + "/s/" + route.ID + "/"
	} else {
		fail(w, apiError("UNSUPPORTED_CAPABILITY", "Configure publicUrl or serviceDomain to publish routes"))
		return
	}
	err := s.store.Update(func(st *State) error {
		b, err := owned(st, clientID(r), in.BoxID)
		if err != nil {
			return err
		}
		if b.Box.State == "deleted" || b.Box.State == "deleting" {
			return apiError("CONFLICT", "Box is deleted")
		}
		for _, existing := range st.Routes {
			if existing.BoxID == route.BoxID && existing.Port == route.Port {
				route = existing
				return nil
			}
		}
		st.Routes[route.ID] = route
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 201, route)
}

type grantInput struct {
	Subject    string `json:"subject"`
	TTLSeconds int    `json:"ttlSeconds"`
}

func (in *grantInput) validate() error {
	if in.TTLSeconds == 0 {
		in.TTLSeconds = 900
	}
	if in.Subject == "" || len(in.Subject) > 256 || in.TTLSeconds < 1 || in.TTLSeconds > 3600 {
		return apiError("INVALID_REQUEST", "subject is required (max 256 bytes); ttlSeconds must be 1..3600")
	}
	return nil
}
func addGrant(st *State, route string, in grantInput) (Grant, string) {
	raw := token()
	g := Grant{ID: randomID("grant-"), RouteID: route, Subject: in.Subject, ExpiresAt: time.Now().UTC().Add(time.Duration(in.TTLSeconds) * time.Second)}
	st.Grants[g.ID] = grantRecord{Grant: g, TokenHash: hash(raw)}
	return g, raw
}
func (s *Service) createGrant(w http.ResponseWriter, r *http.Request) {
	var in grantInput
	if err := decode(w, r, &in); err != nil {
		fail(w, err)
		return
	}
	if err := in.validate(); err != nil {
		fail(w, err)
		return
	}
	var g Grant
	var raw string
	err := s.store.Update(func(st *State) error {
		route, err := routeOwned(st, clientID(r), r.PathValue("id"))
		if err != nil {
			return err
		}
		g, raw = addGrant(st, route.ID, in)
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 201, map[string]any{"grant": g, "token": raw})
}
func (s *Service) revokeGrant(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	err := s.store.Update(func(st *State) error {
		g, ok := st.Grants[r.PathValue("id")]
		if !ok {
			return apiError("NOT_FOUND", "Grant not found")
		}
		if _, err := routeOwned(st, clientID(r), g.Grant.RouteID); err != nil {
			return err
		}
		g.Grant.Revoked = true
		st.Grants[g.Grant.ID] = g
		return nil
	})
	if err == nil {
		for _, stream := range s.streams {
			if stream.grantID == r.PathValue("id") {
				stream.cancel()
			}
		}
	}
	s.mu.Unlock()
	if err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}

// A product backend can keep an already-authorized service session alive while
// its operation is active. Renewal cannot resurrect an expired/revoked grant.
func (s *Service) renewGrant(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TTLSeconds int `json:"ttlSeconds"`
	}
	if err := decode(w, r, &in); err != nil {
		fail(w, err)
		return
	}
	if in.TTLSeconds < 1 || in.TTLSeconds > 3600 {
		fail(w, apiError("INVALID_REQUEST", "ttlSeconds must be 1..3600"))
		return
	}
	var out Grant
	err := s.store.Update(func(st *State) error {
		g, ok := st.Grants[r.PathValue("id")]
		if !ok {
			return apiError("NOT_FOUND", "Grant not found")
		}
		if _, err := routeOwned(st, clientID(r), g.Grant.RouteID); err != nil {
			return err
		}
		if g.Grant.Revoked || !g.Grant.ExpiresAt.After(time.Now()) {
			return apiError("CONFLICT", "Expired or revoked grants cannot be renewed")
		}
		// Renewal only extends validity. Existing stream timers observe the new
		// expiry at their current deadline; shortening it would require waking
		// every active stream immediately.
		next := time.Now().UTC().Add(time.Duration(in.TTLSeconds) * time.Second)
		if next.After(g.Grant.ExpiresAt) {
			g.Grant.ExpiresAt = next
		}
		st.Grants[g.Grant.ID] = g
		out = g.Grant
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, out)
}
func (s *Service) getAccess(w http.ResponseWriter, r *http.Request) {
	var out AccessRequest
	err := s.store.View(func(st State) error {
		a, ok := st.Access[r.PathValue("id")]
		if !ok {
			return apiError("NOT_FOUND", "Access request not found")
		}
		if _, err := routeOwned(&st, clientID(r), a.Request.RouteID); err != nil {
			return err
		}
		out = a.Request
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, out)
}
func (s *Service) approveAccess(w http.ResponseWriter, r *http.Request) {
	var in grantInput
	if err := decode(w, r, &in); err != nil {
		fail(w, err)
		return
	}
	if err := in.validate(); err != nil {
		fail(w, err)
		return
	}
	code := token()
	var redirect string
	err := s.store.Update(func(st *State) error {
		a, ok := st.Access[r.PathValue("id")]
		if !ok {
			return apiError("NOT_FOUND", "Access request not found")
		}
		if _, err := routeOwned(st, clientID(r), a.Request.RouteID); err != nil {
			return err
		}
		if a.Request.Approved || a.Request.Consumed || !a.Request.ExpiresAt.After(time.Now()) {
			return apiError("CONFLICT", "Access request is expired or already approved")
		}
		g, _ := addGrant(st, a.Request.RouteID, in)
		a.GrantID = g.ID
		a.CodeHash = hash(code)
		a.Request.Approved = true
		st.Access[a.Request.ID] = a
		redirect = a.Request.CallbackURL + "?" + url.Values{"request_id": {a.Request.ID}, "code": {code}}.Encode()
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"redirectUrl": redirect})
}
func secureCookie(name, value string, maxAge int) *http.Cookie {
	return &http.Cookie{Name: name, Value: value, Path: "/", MaxAge: maxAge, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode}
}
func (s *Service) startAccess(w http.ResponseWriter, r *http.Request, route Route, b boxRecord, tail string) {
	client := s.client(b.ClientID)
	if s.hostRoute(r.Host) != route.ID || client.AuthorizeURL == "" || r.Method != "GET" || !strings.Contains(r.Header.Get("Accept"), "text/html") {
		fail(w, apiError("UNAUTHENTICATED", "A scoped service grant is required"))
		return
	}
	if !strings.HasPrefix(tail, "/") || strings.HasPrefix(tail, "//") || strings.ContainsAny(tail, "\\\r\n") {
		fail(w, apiError("INVALID_REQUEST", "Unsafe return path"))
		return
	}
	if r.URL.RawQuery != "" {
		tail += "?" + r.URL.RawQuery
	}
	if len(tail) > 4096 {
		fail(w, apiError("INVALID_REQUEST", "Service return URL exceeds 4096 bytes"))
		return
	}
	raw := token()
	a := accessRecord{Request: AccessRequest{ID: randomID("access-"), RouteID: route.ID, BoxID: route.BoxID, CallbackURL: strings.TrimSuffix(route.URL, "/") + "/_cellbox/callback", ExpiresAt: time.Now().UTC().Add(2 * time.Minute)}, BrowserHash: hash(raw), ReturnPath: tail}
	err := s.store.Update(func(st *State) error {
		now := time.Now()
		for id, v := range st.Access {
			if !v.Request.ExpiresAt.After(now) {
				delete(st.Access, id)
			}
		}
		for id, v := range st.Sessions {
			g, ok := st.Grants[v.GrantID]
			if !ok || g.Grant.Revoked || !g.Grant.ExpiresAt.After(now) {
				delete(st.Sessions, id)
			}
		}
		if len(st.Access) >= 256 {
			return apiError("BUSY", "Too many pending browser authorizations")
		}
		st.Access[a.Request.ID] = a
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	u, _ := url.Parse(client.AuthorizeURL)
	q := u.Query()
	q.Set("request_id", a.Request.ID)
	u.RawQuery = q.Encode()
	http.SetCookie(w, secureCookie(transactionCookie, raw, 120))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	http.Redirect(w, r, u.String(), http.StatusSeeOther)
}
func (s *Service) accessCallback(w http.ResponseWriter, r *http.Request, route Route) {
	if r.Method != "GET" {
		fail(w, apiError("INVALID_REQUEST", "Callback requires GET"))
		return
	}
	cookie, err := r.Cookie(transactionCookie)
	if err != nil {
		fail(w, apiError("UNAUTHENTICATED", "Browser transaction cookie is missing"))
		return
	}
	raw := token()
	var returnPath string
	ttl := 0
	err = s.store.Update(func(st *State) error {
		a, ok := st.Access[r.URL.Query().Get("request_id")]
		if !ok || a.Request.RouteID != route.ID || !a.Request.Approved || a.Request.Consumed || !a.Request.ExpiresAt.After(time.Now()) || subtle.ConstantTimeCompare([]byte(a.BrowserHash), []byte(hash(cookie.Value))) != 1 || subtle.ConstantTimeCompare([]byte(a.CodeHash), []byte(hash(r.URL.Query().Get("code")))) != 1 {
			return apiError("UNAUTHENTICATED", "Invalid or expired browser authorization")
		}
		g, ok := st.Grants[a.GrantID]
		if !ok || g.Grant.Revoked || !g.Grant.ExpiresAt.After(time.Now()) {
			return apiError("UNAUTHENTICATED", "Grant is expired or revoked")
		}
		a.Request.Consumed = true
		st.Access[a.Request.ID] = a
		st.Sessions[hash(raw)] = sessionRecord{TokenHash: hash(raw), GrantID: a.GrantID}
		returnPath = a.ReturnPath
		ttl = int(time.Until(g.Grant.ExpiresAt).Seconds())
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	http.SetCookie(w, secureCookie(sessionCookie, raw, ttl))
	http.SetCookie(w, secureCookie(transactionCookie, "", -1))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	http.Redirect(w, r, returnPath, http.StatusSeeOther)
}
func (s *Service) gateway(w http.ResponseWriter, r *http.Request, routeID, tail string) {
	var route Route
	var b boxRecord
	err := s.store.View(func(st State) error {
		var ok bool
		route, ok = st.Routes[routeID]
		if !ok {
			return apiError("NOT_FOUND", "Route not found")
		}
		b, ok = st.Boxes[route.BoxID]
		if !ok {
			return apiError("NOT_FOUND", "Box not found")
		}
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	hostMode := s.hostRoute(r.Host) == routeID
	if hostMode && tail == "/_cellbox/callback" {
		s.accessCallback(w, r, route)
		return
	}
	if strings.HasPrefix(tail, "/_cellbox/") {
		fail(w, apiError("NOT_FOUND", "Reserved service path"))
		return
	}
	var grant Grant
	cookieAuth := false
	err = s.store.View(func(st State) error {
		now := time.Now()
		if hostMode {
			if c, e := r.Cookie(sessionCookie); e == nil {
				if session, ok := st.Sessions[hash(c.Value)]; ok {
					g := st.Grants[session.GrantID].Grant
					if g.RouteID == routeID && !g.Revoked && g.ExpiresAt.After(now) {
						grant = g
						cookieAuth = true
						return nil
					}
				}
			}
		}
		if raw := bearer(r); raw != "" {
			digest := hash(raw)
			for _, record := range st.Grants {
				g := record.Grant
				if g.RouteID == routeID && !g.Revoked && g.ExpiresAt.After(now) && subtle.ConstantTimeCompare([]byte(digest), []byte(record.TokenHash)) == 1 {
					grant = g
					return nil
				}
			}
		}
		return apiError("UNAUTHENTICATED", "A scoped service grant is required")
	})
	if err != nil {
		s.startAccess(w, r, route, b, tail)
		return
	}
	publicRoute, _ := url.Parse(route.URL)
	if origin := r.Header.Get("Origin"); origin != "" && origin != publicRoute.Scheme+"://"+publicRoute.Host {
		fail(w, apiError("FORBIDDEN", "Cross-origin service request denied"))
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	go s.expireGrantStream(ctx, cancel, grant)
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	streamID := randomID("stream-")
	s.mu.Lock()
	err = s.store.View(func(st State) error {
		record := st.Grants[grant.ID]
		if record.Grant.Revoked || !record.Grant.ExpiresAt.After(time.Now()) {
			return apiError("UNAUTHENTICATED", "Grant expired or revoked")
		}
		b = st.Boxes[route.BoxID]
		if op, ok := st.Operations[b.Box.OperationID]; ok && op.Operation.Kind != "exec" && op.Operation.Kind != "archive" {
			if err := busy(&st, b, false); err != nil {
				return err
			}
		}
		if b.Box.State != "ready" {
			return apiError("CONFLICT", "Box is not ready; request resume explicitly")
		}
		return nil
	})
	if err == nil {
		s.streams[streamID] = activeStream{grantID: grant.ID, boxID: b.Box.ID, cancel: cancel}
	}
	s.mu.Unlock()
	if err != nil {
		fail(w, err)
		return
	}
	defer func() { s.mu.Lock(); delete(s.streams, streamID); s.mu.Unlock() }()
	conn, err := s.connection(ctx, b)
	if err != nil {
		fail(w, err)
		return
	}
	target, err := url.Parse(conn.URL)
	if err != nil {
		fail(w, err)
		return
	}
	upstreamAuth := ""
	if cookieAuth {
		upstreamAuth = r.Header.Get("Authorization")
		// A browser may send both session and gateway bearer credentials. Such
		// credentials must never become the sandbox application's Authorization.
		candidate := hash(bearer(r))
		_ = s.store.View(func(st State) error {
			for _, g := range st.Grants {
				if subtle.ConstantTimeCompare([]byte(candidate), []byte(g.TokenHash)) == 1 {
					upstreamAuth = ""
					break
				}
			}
			return nil
		})
		for _, c := range s.config.Clients {
			if subtle.ConstantTimeCompare([]byte(candidate), []byte(hash(c.Token))) == 1 {
				upstreamAuth = ""
				break
			}
		}
	}
	proxy := &httputil.ReverseProxy{Rewrite: func(p *httputil.ProxyRequest) {
		p.SetURL(target)
		p.Out.URL.Path = "/proxy/" + strconv.Itoa(route.Port) + tail
		p.Out.URL.RawPath = ""
		escapedTail := r.URL.EscapedPath()
		if !hostMode {
			escapedTail = strings.TrimPrefix(escapedTail, "/s/"+routeID)
			if escapedTail == "" {
				escapedTail = "/"
			}
		}
		p.Out.URL.RawPath = "/proxy/" + strconv.Itoa(route.Port) + escapedTail
		p.Out.Host = target.Host
		// Neither callers nor an application can inject control-plane credentials.
		for name := range p.Out.Header {
			lower := strings.ToLower(name)
			if strings.HasPrefix(lower, "x-cellbox-") || strings.HasPrefix(lower, "x-forwarded-") || lower == "forwarded" || lower == "proxy-authorization" {
				p.Out.Header.Del(name)
			}
		}
		p.Out.Header.Del("Authorization")
		p.Out.Header.Set("Authorization", "Bearer "+conn.Token)
		if upstreamAuth != "" {
			p.Out.Header.Set("X-Cellbox-Upstream-Authorization", upstreamAuth)
		}
		p.Out.Header.Del("Cookie")
		for _, c := range p.In.Cookies() {
			if !strings.HasPrefix(c.Name, "__Host-cellbox-") {
				p.Out.AddCookie(c)
			}
		}
	}, Transport: streamTransport{s.http.Transport}, ModifyResponse: func(res *http.Response) error {
		values := res.Header.Values("Set-Cookie")
		res.Header.Del("Set-Cookie")
		for _, v := range values {
			name, _, _ := strings.Cut(v, "=")
			if !strings.HasPrefix(strings.TrimSpace(name), "__Host-cellbox-") {
				res.Header.Add("Set-Cookie", v)
			}
		}
		return nil
	}, ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
		fail(w, apiError("UPSTREAM_UNAVAILABLE", "Box service is unavailable"))
	}}
	proxy.ServeHTTP(w, r.WithContext(ctx))
}

func (s *Service) expireGrantStream(ctx context.Context, cancel context.CancelFunc, grant Grant) {
	timer := time.NewTimer(time.Until(grant.ExpiresAt))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			valid := false
			_ = s.store.View(func(st State) error {
				record, ok := st.Grants[grant.ID]
				if ok && !record.Grant.Revoked && record.Grant.ExpiresAt.After(time.Now()) {
					grant = record.Grant
					valid = true
				}
				return nil
			})
			if !valid {
				cancel()
				return
			}
			timer.Reset(time.Until(grant.ExpiresAt))
		}
	}
}

// ReverseProxy's upgraded connection must close on grant expiration/revocation.
type streamTransport struct{ base http.RoundTripper }

func (t streamTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	res, err := t.base.RoundTrip(r)
	if err != nil {
		return res, err
	}
	if res.StatusCode == 101 {
		if body, ok := res.Body.(io.ReadWriteCloser); ok {
			wrapped := &cancelBody{ReadWriteCloser: body}
			wrapped.stop = context.AfterFunc(r.Context(), func() { _ = body.Close() })
			res.Body = wrapped
		}
	}
	return res, nil
}

type cancelBody struct {
	io.ReadWriteCloser
	stop func() bool
}

func (c *cancelBody) Close() error {
	if c.stop != nil {
		c.stop()
	}
	return c.ReadWriteCloser.Close()
}
