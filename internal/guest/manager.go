package guest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jingyugao/cell-box/internal/guestapi"
	"golang.org/x/sys/unix"
)

const platformReleases = "/var/lib/cellbox/shared/guest"

type generation struct {
	cmd                      *exec.Cmd
	socket, binary, revision string
	transport                *http.Transport
	proxy                    *httputil.ReverseProxy
	requests                 sync.WaitGroup
	done                     chan struct{}
}

func (g *generation) alive() bool {
	if g == nil {
		return false
	}
	select {
	case <-g.done:
		return false
	default:
		return true
	}
}

func (g *generation) retire() {
	g.requests.Wait()
	g.transport.CloseIdleConnections()
	_ = g.cmd.Process.Kill()
	<-g.done
	_ = os.Remove(g.socket)
	if g.binary != "" {
		_ = os.Remove(g.binary)
	}
}

type guestManager struct {
	cfg             guestapi.Config
	self, directory string
	key             ed25519.PublicKey
	staged          bool
	change          sync.RWMutex // only identity, activation and staged restores require a handoff barrier
	mu              sync.Mutex
	current         *generation
	busy            int
	processMu       sync.Mutex
	process         *os.Process
	command         []string
}

func ServeManaged(cfg guestapi.Config, self string, staged bool) error {
	key, err := guestapi.GuestUpdateKey(cfg.Env[guestapi.GuestUpdateKeyEnv])
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("/run/cellbox", "manager-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	m := &guestManager{cfg: cfg, self: self, directory: dir, key: key, staged: staged}
	ctl, err := net.Listen("unix", filepath.Join(dir, "control"))
	if err != nil {
		return err
	}
	defer ctl.Close()
	go (&http.Server{Handler: http.HandlerFunc(m.launch), ReadHeaderTimeout: 5 * time.Second}).Serve(ctl)
	if err := m.reconcile(); err != nil {
		slog.Warn("Guest release unavailable at startup", "error", err)
		if err = m.install(self, "embedded"); err != nil {
			return err
		}
	}
	tools := http.NewServeMux()
	tools.HandleFunc("POST /v1/tools/{id}", func(w http.ResponseWriter, r *http.Request) { m.forward(w, r, true) })
	if err := serveToolSocket(cfg.Agent, tools); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		last := ""
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			err := m.reconcile()
			if err == nil {
				if last != "" {
					ticker.Reset(2 * time.Second)
				}
				last = ""
				continue
			}
			ticker.Reset(10 * time.Second)
			if err.Error() != last {
				slog.Warn("Guest update deferred", "error", err)
				last = err.Error()
			}
			m.mu.Lock()
			healthy := m.current.alive()
			m.mu.Unlock()
			if !healthy {
				if e := m.install(self, "embedded"); e != nil {
					slog.Error("cannot recover Guest worker", "error", e)
				}
			}
		}
	}()
	external := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { m.forward(w, r, false) })
	return (&http.Server{Addr: fmt.Sprintf(":%d", guestapi.Port), Handler: external, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}).ListenAndServe()
}

func (m *guestManager) forward(w http.ResponseWriter, r *http.Request, tool bool) {
	change := lifecycleChange(r) || r.Method == "POST" && r.URL.Path == "/v1/restore"
	if change {
		m.change.RLock()
		defer m.change.RUnlock()
	}
	busy := tool || r.Method == "POST" && (r.URL.Path == "/v1/exec" || strings.HasPrefix(r.URL.Path, "/v1/tools/")) ||
		r.Method == "PUT" && (r.URL.Path == "/v1/files" || strings.HasPrefix(r.URL.Path, "/v1/credentials"))
	m.mu.Lock()
	g := m.current
	if !g.alive() {
		m.mu.Unlock()
		http.Error(w, "Guest worker restarting", 503)
		return
	}
	// The authenticated worker checks the coordinator's count across draining generations.
	n := m.busy
	if busy {
		m.busy++
	}
	g.requests.Add(1)
	m.mu.Unlock()
	defer g.requests.Done()
	if busy {
		defer func() { m.mu.Lock(); m.busy--; m.mu.Unlock() }()
	}
	r.Header.Del(localToolHeader)
	r.Header.Del(workerBusyHeader)
	if n > 0 && r.Method == "POST" && r.URL.Path == "/v1/quiesce" {
		r.Header.Set(workerBusyHeader, "1")
	}
	if tool {
		r.Header.Set(localToolHeader, "1")
	}
	if r.URL.Path == "/healthz" {
		w.Header().Set("X-Cellbox-Guest-Revision", g.revision)
	}
	g.proxy.ServeHTTP(w, r)
}

// Only the operator public key may authorize executable code in the management worker.
func (m *guestManager) reconcile() error {
	// The shared mount may be written by the product publisher. Use bounded,
	// nonblocking regular-file reads and execute only the private verified copy.
	file, err := openReleaseFile(filepath.Join(platformReleases, "current.json"))
	if errors.Is(err, os.ErrNotExist) {
		m.mu.Lock()
		alive := m.current.alive()
		m.mu.Unlock()
		if alive {
			return nil
		}
		return m.install(m.self, "embedded")
	}
	if err != nil {
		return err
	}
	var manifest struct {
		SHA256    string
		Signature []byte
	}
	raw, err := io.ReadAll(io.LimitReader(file, 4097))
	file.Close()
	if err != nil || len(raw) > 4096 || json.Unmarshal(raw, &manifest) != nil || !revisionPattern.MatchString(manifest.SHA256) ||
		!ed25519.Verify(m.key, []byte(workerABI+":"+manifest.SHA256), manifest.Signature) {
		return errors.New("invalid signed Guest release")
	}
	m.mu.Lock()
	current := m.current
	uptodate := current.alive() && current.revision == manifest.SHA256
	m.mu.Unlock()
	if uptodate {
		return nil
	}
	source, err := openReleaseFile(filepath.Join(platformReleases, "versions", manifest.SHA256))
	if err != nil {
		return err
	}
	defer source.Close()
	binary, err := os.CreateTemp("/run/cellbox", "worker-")
	if err != nil {
		return err
	}
	dest := binary.Name()
	defer func() {
		if err != nil {
			os.Remove(dest)
		}
	}()
	hash := sha256.New()
	const limit = 128 << 20
	n, err := io.Copy(io.MultiWriter(binary, hash), io.LimitReader(source, limit+1))
	if err == nil && (n > limit || hex.EncodeToString(hash.Sum(nil)) != manifest.SHA256) {
		err = errors.New("Guest release checksum mismatch")
	}
	if err == nil {
		err = binary.Chmod(0555)
	}
	closeErr := binary.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = m.install(dest, manifest.SHA256)
	}
	return err
}

func openReleaseFile(path string) (*os.File, error) {
	fd, err := unix.Openat2(unix.AT_FDCWD, path, &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NONBLOCK, Resolve: unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS})
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		file.Close()
		return nil, errors.New("Guest release entry must be a regular file")
	}
	return file, nil
}

func (m *guestManager) install(binary, revision string) error {
	m.change.Lock()
	defer m.change.Unlock()
	socketFile, err := os.CreateTemp(m.directory, "w-")
	if err != nil {
		return err
	}
	socket := socketFile.Name()
	socketFile.Close()
	os.Remove(socket)
	m.processMu.Lock()
	pid := m.workloadPIDLocked()
	m.processMu.Unlock()
	input, err := json.Marshal(workerStart{m.cfg, m.staged, m.directory, socket, pid})
	if err != nil {
		return err
	}
	cmd := exec.Command(binary, "__worker")
	cmd.Stdin = bytes.NewReader(input)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err = cmd.Start(); err != nil {
		return err
	}
	g := &generation{cmd: cmd, socket: socket, revision: revision, transport: unixTransport(socket), done: make(chan struct{})}
	if binary != m.self {
		g.binary = binary
	}
	go func() { _ = cmd.Wait(); close(g.done) }()
	g.proxy = &httputil.ReverseProxy{Director: func(r *http.Request) { r.URL.Scheme = "http"; r.URL.Host = "worker" }, Transport: g.transport}
	client := &http.Client{Transport: g.transport, Timeout: 300 * time.Millisecond}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && g.alive() {
		res, e := client.Get("http://worker/healthz")
		if e == nil {
			io.Copy(io.Discard, io.LimitReader(res.Body, 1024))
			res.Body.Close()
			if res.StatusCode == 200 && res.Header.Get("X-Cellbox-Worker-ABI") == workerABI {
				m.mu.Lock()
				old := m.current
				m.current = g
				m.mu.Unlock()
				if old != nil {
					go old.retire()
				}
				slog.Info("Guest worker ready", "revision", revision)
				return nil
			}
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	g.retire()
	return errors.New("candidate Guest worker did not pass its protocol handshake")
}
