package service

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
)

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
		if b.Box.Phase == "deleted" || b.Box.Phase == "deleting" {
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
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	streamID := randomID("stream-")
	unlockAdmission := s.admissions.lock(route.BoxID)
	err = s.store.View(func(st State) error {
		b = st.Boxes[route.BoxID]
		if op, ok := st.Operations[b.Box.OperationID]; ok && op.Operation.Kind != "exec" && op.Operation.Kind != "archive" {
			if err := busy(&st, b, false); err != nil {
				return err
			}
		}
		if b.Box.Phase != "running" {
			return apiError("CONFLICT", "Box is not ready; request resume explicitly")
		}
		return nil
	})
	if err == nil {
		s.mu.Lock()
		s.streams[streamID] = activeStream{boxID: b.Box.ID, cancel: cancel}
		s.mu.Unlock()
	}
	unlockAdmission()
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
	escapedTail := r.URL.EscapedPath()
	if !hostMode {
		escapedTail = strings.TrimPrefix(escapedTail, "/s/"+routeID)
		if escapedTail == "" {
			escapedTail = "/"
		}
	}
	s.proxyService(w, r, ctx, conn, route.Port, tail, escapedTail)
}

// Internal product connections use their live stream as the lifecycle fence;
// no token, browser route, or grant is needed.
func (s *Service) internalService(w http.ResponseWriter, r *http.Request) {
	port, err := strconv.Atoi(r.PathValue("port"))
	if err != nil || port < 1 || port > 65535 || port == guestapi.Port {
		fail(w, apiError("INVALID_REQUEST", "Invalid service port"))
		return
	}
	b, release, err := s.beginIOWithPolicy(clientID(r), r.PathValue("id"), true)
	if err != nil {
		fail(w, err)
		return
	}
	defer release()
	if b.Box.Phase != "running" {
		fail(w, apiError("CONFLICT", "Box is not running"))
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	conn, err := s.connection(ctx, b)
	if err != nil {
		fail(w, err)
		return
	}
	prefix := "/v1/boxes/" + url.PathEscape(b.Box.ID) + "/services/" + r.PathValue("port")
	s.proxyService(w, r, ctx, conn, port, "/"+r.PathValue("path"), strings.TrimPrefix(r.URL.EscapedPath(), prefix))
}

func (s *Service) proxyService(w http.ResponseWriter, r *http.Request, ctx context.Context, conn boxprovider.Connection, port int, tail, escapedTail string) {
	target, err := url.Parse(conn.URL)
	if err != nil {
		fail(w, err)
		return
	}
	proxy := &httputil.ReverseProxy{Rewrite: func(p *httputil.ProxyRequest) {
		p.SetURL(target)
		p.Out.URL.Path = "/proxy/" + strconv.Itoa(port) + tail
		p.Out.URL.RawPath = "/proxy/" + strconv.Itoa(port) + escapedTail
		p.Out.Host = target.Host
		// Preserve application Authorization while removing proxy control headers.
		for name := range p.Out.Header {
			lower := strings.ToLower(name)
			if strings.HasPrefix(lower, "x-cellbox-") || strings.HasPrefix(lower, "x-forwarded-") || lower == "forwarded" || lower == "proxy-authorization" {
				p.Out.Header.Del(name)
			}
		}
	}, Transport: streamTransport{s.http.Transport}, ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
		fail(w, apiError("UPSTREAM_UNAVAILABLE", "Box service is unavailable"))
	}}
	proxy.ServeHTTP(w, r.WithContext(ctx))
}

// Close upgraded connections when their request or service context ends.
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
