package guest

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"cellbox.local/cellbox/internal/guestapi"
	"golang.org/x/sys/unix"
)

type Server struct {
	cfg            guestapi.Config
	token          string
	self           string
	tools          map[string]compiledTool
	staged         bool
	mu             sync.Mutex
	active         bool
	restoring      bool
	quiesced       bool
	busy           int
	proc           *os.Process
	proxyTransport *http.Transport
}

func NewServer(c guestapi.Config, token, self string, staged bool) (*Server, error) {
	tools, err := validateConfig(c)
	if err != nil {
		return nil, err
	}
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("invalid control token")
	}
	if !filepath.IsAbs(self) {
		return nil, errors.New("guest executable path must be absolute")
	}
	transport := &http.Transport{Proxy: nil, MaxIdleConns: 64, MaxIdleConnsPerHost: 8, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: 30 * time.Second}
	return &Server{cfg: c, token: token, self: self, tools: tools, staged: staged, proxyTransport: transport}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/exec", s.execHandler)
	mux.HandleFunc("GET /v1/files", s.fileHandler)
	mux.HandleFunc("PUT /v1/files", s.fileHandler)
	mux.HandleFunc("GET /v1/archive", s.archiveHandler)
	mux.HandleFunc("POST /v1/restore", s.restoreHandler)
	mux.HandleFunc("POST /v1/activate", s.activateHandler)
	mux.HandleFunc("POST /v1/quiesce", s.quiesceHandler)
	mux.HandleFunc("POST /v1/unquiesce", s.unquiesceHandler)
	mux.HandleFunc("PUT /v1/credentials/{slot}", s.credentialHandler)
	mux.HandleFunc("POST /v1/tools/{id}", s.toolHandler)
	mux.HandleFunc("/proxy/", s.proxyHandler)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		a := r.Header.Get("Authorization")
		if !strings.HasPrefix(a, prefix) || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(a, prefix)), []byte(s.token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), 400)
		return false
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		http.Error(w, "trailing JSON", 400)
		return false
	}
	return true
}

func (s *Server) execHandler(w http.ResponseWriter, r *http.Request) {
	if !s.beginCommand() {
		http.Error(w, "guest quiesced", 409)
		return
	}
	defer s.endCommand()
	var req guestapi.ExecRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	dir, err := workspaceDir(s.cfg.Workspace, req.Cwd)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	defer dir.Close()
	result, err := run(r.Context(), s.self, s.cfg.Agent, req.Argv, dir.Name(), "/home/agent", req.Env, req.TimeoutMS)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	writeJSON(w, 200, result)
}

func (s *Server) toolHandler(w http.ResponseWriter, r *http.Request) {
	if !s.beginCommand() {
		http.Error(w, "guest quiesced", 409)
		return
	}
	defer s.endCommand()
	t, ok := s.tools[r.PathValue("id")]
	if !ok {
		http.NotFound(w, r)
		return
	}
	var req guestapi.ToolRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	result, err := s.runTool(r.Context(), t, req)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	writeJSON(w, 200, result)
}

func (s *Server) credentialHandler(w http.ResponseWriter, r *http.Request) {
	slot := r.PathValue("slot")
	if !namePattern.MatchString(slot) {
		http.Error(w, "invalid slot", 400)
		return
	}
	if err := writeCredential(debugRoot, slot, http.MaxBytesReader(w, r.Body, 1<<20), s.cfg.Debug); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) proxyHandler(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/proxy/")
	p, sub, ok := strings.Cut(rest, "/")
	if !ok {
		p = rest
		sub = ""
	}
	escapedRest := strings.TrimPrefix(r.URL.EscapedPath(), "/proxy/")
	escapedPort, escapedSub, hasEscapedSub := strings.Cut(escapedRest, "/")
	port, err := strconv.Atoi(p)
	if err != nil || port < 1 || port > 65535 || port == guestapi.Port || p != strconv.Itoa(port) || escapedPort != p || hasEscapedSub != ok {
		http.Error(w, "invalid proxy port", 400)
		return
	}
	target := &url.URL{Scheme: "http", Host: net.JoinHostPort("127.0.0.1", p)}
	proxy := httputil.NewSingleHostReverseProxy(target)
	old := proxy.Director
	proxy.Director = func(out *http.Request) {
		upstreamAuth := out.Header.Get("X-Cellbox-Upstream-Authorization")
		out.URL.Path = "/" + sub
		out.URL.RawPath = ""
		if hasEscapedSub {
			out.URL.RawPath = "/" + escapedSub
		}
		out.RequestURI = ""
		out.Header.Del("Authorization")
		out.Header.Del("Proxy-Authorization")
		out.Header.Del("X-Cellbox-Upstream-Authorization")
		out.Header.Del("Forwarded")
		out.Header.Del("X-Forwarded-For")
		out.Header.Del("X-Forwarded-Host")
		out.Header.Del("X-Forwarded-Proto")
		old(out)
		out.Host = target.Host
		if upstreamAuth != "" {
			out.Header.Set("Authorization", upstreamAuth)
		}
	}
	proxy.Transport = s.proxyTransport
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) { http.Error(w, "upstream unavailable", 502) }
	proxy.ServeHTTP(w, r)
}

func (s *Server) Start() error {
	if s.staged {
		return nil
	}
	return s.Activate()
}

func (s *Server) activateHandler(w http.ResponseWriter, r *http.Request) {
	if err := s.Activate(); err != nil {
		http.Error(w, err.Error(), 409)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) Activate() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.quiesced {
		return errors.New("guest quiesced")
	}
	if s.active || s.restoring {
		if s.active {
			return nil
		}
		return errors.New("restore in progress")
	}
	if len(s.cfg.Command) == 0 {
		s.active = true
		return nil
	}
	// StartWorkload retains the process and reports a startup failure immediately.
	p, err := startWorkload(s.self, s.cfg)
	if err != nil {
		return err
	}
	s.proc = p
	s.active = true
	return nil
}

func (s *Server) ListenAndServe() error {
	if err := s.Start(); err != nil {
		return err
	}
	if err := s.startToolSocket(); err != nil {
		return err
	}
	server := &http.Server{Addr: fmt.Sprintf("0.0.0.0:%d", guestapi.Port), Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
	return server.ListenAndServe()
}

func (s *Server) Shutdown() {
	s.proxyTransport.CloseIdleConnections()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.proc != nil {
		_ = unix.Kill(-s.proc.Pid, unix.SIGKILL)
	}
}

func (s *Server) beginCommand() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.quiesced {
		return false
	}
	s.busy++
	return true
}
func (s *Server) endCommand() { s.mu.Lock(); s.busy--; s.mu.Unlock() }
func (s *Server) quiesceHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.busy > 0 {
		http.Error(w, "commands in flight", 409)
		return
	}
	s.quiesced = true
	w.WriteHeader(http.StatusNoContent)
}
func (s *Server) unquiesceHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.quiesced = false
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}
