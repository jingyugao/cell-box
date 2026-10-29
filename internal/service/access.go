package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"

	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/guestapi"
)

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
	s.proxyService(w, r, ctx, conn, b.GuestIdentity, port, "/"+r.PathValue("path"), strings.TrimPrefix(r.URL.EscapedPath(), prefix))
}

func (s *Service) proxyService(w http.ResponseWriter, r *http.Request, ctx context.Context, conn boxprovider.Connection, instanceID string, port int, tail, escapedTail string) {
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
		if instanceID != "" {
			p.Out.Header.Set(guestapi.InstanceIDHeader, instanceID)
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
