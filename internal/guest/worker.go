// A worker implements the Guest protocol. Its supervisor owns the workload process
// and listening addresses, so changing the worker does not restart the workload.
package guest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/jingyugao/cell-box/internal/guestapi"
)

const localToolHeader = "X-Cellbox-Worker-Tool"
const workerBusyHeader = "X-Cellbox-Worker-Busy"
const workerABI = "cellbox-worker-v1"

type pendingWorkKey struct{}

type workerStart struct {
	Config            guestapi.Config
	Staged            bool
	Directory, Socket string
	WorkloadPID       int
}

type workerState struct {
	ABI, Identity, BoxID, Revision string
	Active                         bool
}

func unixTransport(path string) *http.Transport {
	return &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", path)
	}, IdleConnTimeout: 30 * time.Second}
}

func lifecycleChange(r *http.Request) bool {
	return (r.Method == "POST" && (r.URL.Path == "/v1/identity" || r.URL.Path == "/v1/activate")) ||
		r.Method == "PUT" && r.URL.Path == "/v1/workload"
}

func ServeWorker(input io.Reader) error {
	var w workerStart
	if err := json.NewDecoder(io.LimitReader(input, 2<<20)).Decode(&w); err != nil {
		return err
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	s, err := NewServer(w.Config, self, w.Staged)
	if err != nil {
		return err
	}
	ctl := &http.Client{Transport: unixTransport(filepath.Join(w.Directory, "control")), Timeout: 5 * time.Second}
	s.launch = func(cfg guestapi.Config) (*os.Process, error) {
		body, err := json.Marshal(cfg.Command)
		if err != nil {
			return nil, err
		}
		res, err := ctl.Post("http://supervisor/process", "application/json", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		var pid int
		if res.StatusCode != 200 || json.NewDecoder(res.Body).Decode(&pid) != nil || pid <= 0 {
			return nil, errors.New("workload launch failed")
		}
		return os.FindProcess(pid)
	}
	statePath := filepath.Join(w.Directory, "state")
	var stateMu sync.Mutex
	save := func() error {
		stateMu.Lock()
		defer stateMu.Unlock()
		s.mu.Lock()
		s.identityMu.Lock()
		state := workerState{workerABI, s.identity, s.boxID, s.revision, s.active}
		s.identityMu.Unlock()
		s.mu.Unlock()
		raw, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if err = os.WriteFile(statePath+".next", raw, 0600); err != nil {
			return err
		}
		return os.Rename(statePath+".next", statePath)
	}
	raw, err := os.ReadFile(statePath)
	switch {
	case err == nil:
		var state workerState
		if err = json.Unmarshal(raw, &state); err != nil || state.ABI != workerABI {
			return errors.New("incompatible Guest handoff state")
		}
		s.identity, s.boxID, s.revision, s.active = state.Identity, state.BoxID, state.Revision, state.Active
		if w.WorkloadPID > 0 {
			s.proc, err = os.FindProcess(w.WorkloadPID)
			if err != nil {
				return err
			}
		}
	case errors.Is(err, os.ErrNotExist):
		if err = s.Start(); err != nil {
			return err
		}
		if err = save(); err != nil {
			return err
		}
	default:
		return err
	}
	handler := s.Handler()
	mux := http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
		if r.Header.Get(workerBusyHeader) == "1" {
			r = r.WithContext(context.WithValue(r.Context(), pendingWorkKey{}, true))
			r.Header.Del(workerBusyHeader)
		}
		if r.Header.Get(localToolHeader) == "1" {
			r.Header.Del(localToolHeader)
			r.Header.Set(guestapi.InstanceIDHeader, s.instanceID())
		}
		if r.Method == "GET" && r.URL.Path == "/healthz" {
			out.Header().Set("X-Cellbox-Worker-ABI", workerABI)
		}
		if !lifecycleChange(r) {
			handler.ServeHTTP(out, r)
			return
		}
		// Record the new identity/process selection before acknowledging a mutation.
		result := httptest.NewRecorder()
		handler.ServeHTTP(result, r)
		if err := save(); err != nil {
			http.Error(out, "cannot save Guest handoff state", 500)
			return
		}
		for k, values := range result.Header() {
			out.Header()[k] = values
		}
		out.WriteHeader(result.Code)
		_, _ = out.Write(result.Body.Bytes())
	})
	l, err := net.Listen("unix", w.Socket)
	if err != nil {
		return err
	}
	defer l.Close()
	return (&http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}).Serve(l)
}

func (m *guestManager) launch(w http.ResponseWriter, r *http.Request) {
	var argv []string
	if r.Method != "POST" || r.URL.Path != "/process" {
		http.NotFound(w, r)
		return
	}
	if !decodeJSON(w, r, &argv) {
		return
	}
	m.processMu.Lock()
	defer m.processMu.Unlock()
	if pid := m.workloadPIDLocked(); pid != 0 {
		if slices.Equal(m.command, argv) {
			writeJSON(w, 200, pid)
			return
		}
		http.Error(w, "workload still running", 409)
		return
	}
	cfg := m.cfg
	cfg.Command = argv
	proc, err := startWorkload(m.self, cfg)
	if err != nil {
		http.Error(w, fmt.Sprintf("start workload: %v", err), 500)
		return
	}
	m.process = proc
	m.command = argv
	writeJSON(w, 200, proc.Pid)
}

func (m *guestManager) workloadPIDLocked() int {
	if m.process != nil && m.process.Signal(syscall.Signal(0)) == nil {
		return m.process.Pid
	}
	return 0
}
