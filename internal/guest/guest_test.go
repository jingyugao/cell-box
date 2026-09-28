package guest

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/guestapi"
)

func testConfig(t *testing.T) guestapi.Config {
	t.Helper()
	c := guestapi.DefaultConfig()
	c.Workspace = t.TempDir()
	c.Agent = guestapi.Identity{UID: uint32(os.Getuid()), GID: uint32(os.Getgid())}
	c.Debug = guestapi.Identity{UID: c.Agent.UID + 1, GID: c.Agent.GID + 1}
	return c
}

func request(h http.Handler, method, path, token string, body io.Reader) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, body)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestConfigAndPaths(t *testing.T) {
	c := testConfig(t)
	c.Debug.UID = c.Agent.UID
	if _, err := validateConfig(c); err == nil {
		t.Fatal("duplicate UID accepted")
	}
	c.Debug.UID++
	c.Workspace = "/var/lib/cellbox"
	if _, err := validateConfig(c); err == nil {
		t.Fatal("workspace could include debug credentials")
	}
	c.Workspace = t.TempDir()
	c.Tools = []guestapi.Tool{{ID: "git", Executable: "/bin/sh"}}
	if _, err := validateConfig(c); err == nil {
		t.Fatal("tool outside protected directory accepted")
	}
	for _, p := range []string{"../secret", "a/../b", "/etc/passwd", "a//b", "a/./b"} {
		if _, err := cleanRelative(p, false); err == nil {
			t.Errorf("accepted %q", p)
		}
	}
	root := t.TempDir()
	if err := os.Symlink("/etc", filepath.Join(root, "outside")); err != nil {
		t.Fatal(err)
	}
	if _, err := openWorkspace(root, "outside/passwd", os.O_RDONLY, 0); err == nil {
		t.Fatal("symlink traversal accepted")
	}
	if err := ensureDir(filepath.Join(root, "outside", "new"), os.Getuid(), os.Getgid(), 0700); err == nil {
		t.Fatal("symlinked directory accepted")
	}
}

func TestAccountConflicts(t *testing.T) {
	id := guestapi.Identity{UID: 11000, GID: 11000}
	for _, passwd := range []string{
		"other:x:11000:11000::/home/other:/sbin/nologin\n",
		"cellbox-agent:x:11001:11000::/home/agent:/sbin/nologin\n",
		"cellbox-agent:x:11000:11000::/home/agent:/bin/sh\n",
	} {
		if _, err := checkPasswd(strings.NewReader(passwd), "cellbox-agent", id, "/home/agent"); err == nil {
			t.Errorf("accepted conflicting account %q", passwd)
		}
	}
	if _, err := checkGroup(strings.NewReader("other:x:11000:\n"), "cellbox-agent", 11000); err == nil {
		t.Fatal("accepted conflicting group ID")
	}
	if found, err := checkPasswd(strings.NewReader("cellbox-agent:x:11000:11000::/home/agent:/sbin/nologin\n"), "cellbox-agent", id, "/home/agent"); err != nil || !found {
		t.Fatalf("idempotent account failed: %v", err)
	}
}

func TestScratchAccountFileAndDirectories(t *testing.T) {
	root := t.TempDir()
	uid := uint32(os.Getuid())
	etc := filepath.Join(root, "etc")
	if err := ensureDir(etc, os.Getuid(), os.Getgid(), 0755); err != nil {
		t.Fatal(err)
	}
	f, err := openAccountFile(filepath.Join(etc, "passwd"), uid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("guest:x:11000:11000::/home/guest:/sbin/nologin\n"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(filepath.Join(etc, "passwd"))
	if err != nil || fi.Mode().Perm() != 0644 {
		t.Fatalf("created account file: %v %v", fi, err)
	}
	if err := os.Symlink("passwd", filepath.Join(etc, "group")); err != nil {
		t.Fatal(err)
	}
	if _, err := openAccountFile(filepath.Join(etc, "group"), uid); err == nil {
		t.Fatal("symlinked account file accepted")
	}
	home := filepath.Join(root, "home", "agent")
	if err := ensureDir(home, os.Getuid(), os.Getgid(), 0750); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Stat(filepath.Dir(home))
	if err != nil || parent.Mode().Perm() != 0755 {
		t.Fatalf("new parent not traversable: %v %v", parent, err)
	}
}

func TestCredentialWriteIsPrivateAndAtomic(t *testing.T) {
	root := t.TempDir()
	id := guestapi.Identity{UID: uint32(os.Getuid()), GID: uint32(os.Getgid())}
	if err := writeCredential(root, "key", strings.NewReader("first"), id); err != nil {
		t.Fatal(err)
	}
	if err := writeCredential(root, "key", strings.NewReader("second"), id); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(filepath.Join(root, "key"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0600 {
		t.Fatalf("credential mode %o", fi.Mode().Perm())
	}
	b, err := os.ReadFile(filepath.Join(root, "key"))
	if err != nil || string(b) != "second" {
		t.Fatalf("credential content %q %v", b, err)
	}
	if err := writeCredential(root, "../key", strings.NewReader("x"), id); err == nil {
		t.Fatal("unsafe slot accepted")
	}
}

func TestAuthFilesArchiveRestore(t *testing.T) {
	c := testConfig(t)
	s, err := NewServer(c, "secret", "/bin/true", true)
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	if got := request(h, "GET", "/healthz", "", nil).Code; got != 401 {
		t.Fatalf("unauthenticated health: %d", got)
	}
	if got := request(h, "GET", "/healthz", "secret", nil).Code; got != 200 {
		t.Fatalf("health: %d", got)
	}
	if got := request(h, "PUT", "/v1/files?path=../bad", "secret", strings.NewReader("bad")).Code; got != 400 {
		t.Fatalf("traversal: %d", got)
	}
	if got := request(h, "PUT", "/v1/files?path=hello.txt", "secret", strings.NewReader("hello")).Code; got != 204 {
		t.Fatalf("put: %d", got)
	}
	if got := request(h, "GET", "/v1/files?path=hello.txt", "secret", nil); got.Code != 200 || got.Body.String() != "hello" {
		t.Fatalf("get: %d %q", got.Code, got.Body.String())
	}
	a := request(h, "GET", "/v1/archive", "secret", nil)
	if a.Code != 200 {
		t.Fatalf("archive: %d %s", a.Code, a.Body.String())
	}
	other := testConfig(t)
	restore, err := NewServer(other, "secret", "/bin/true", true)
	if err != nil {
		t.Fatal(err)
	}
	if got := request(restore.Handler(), "POST", "/v1/restore", "secret", bytes.NewReader(a.Body.Bytes())).Code; got != 204 {
		t.Fatalf("restore: %d", got)
	}
	b, err := os.ReadFile(filepath.Join(other.Workspace, "hello.txt"))
	if err != nil || string(b) != "hello" {
		t.Fatalf("restored: %q %v", b, err)
	}
	if got := request(restore.Handler(), "POST", "/v1/restore", "secret", bytes.NewReader(a.Body.Bytes())).Code; got != 409 {
		t.Fatalf("nonempty restore: %d", got)
	}
}

func TestRestoreRejectsTraversal(t *testing.T) {
	c := testConfig(t)
	s, _ := NewServer(c, "secret", "/bin/true", true)
	var body bytes.Buffer
	gz := gzip.NewWriter(&body)
	tw := tar.NewWriter(gz)
	_ = tw.WriteHeader(&tar.Header{Name: "../outside", Mode: 0644, Typeflag: tar.TypeReg, Size: 1})
	_, _ = tw.Write([]byte("x"))
	_ = tw.Close()
	_ = gz.Close()
	if got := request(s.Handler(), "POST", "/v1/restore", "secret", &body); got.Code != 400 {
		t.Fatalf("traversal restore: %d %s", got.Code, got.Body.String())
	}
}

func TestQuiesceAndToolArguments(t *testing.T) {
	c := testConfig(t)
	c.Tools = []guestapi.Tool{{ID: "safe", Executable: "/opt/cellbox/tools/safe", InputPatterns: []string{`[a-z]+`}}}
	s, err := NewServer(c, "secret", "/bin/true", true)
	if err != nil {
		t.Fatal(err)
	}
	tool := s.tools["safe"]
	if _, err := s.runTool(context.Background(), tool, guestapi.ToolRequest{Args: []string{"bad;echo"}}); err == nil {
		t.Fatal("unsafe argument accepted")
	}
	s.busy = 1
	if got := request(s.Handler(), "POST", "/v1/quiesce", "secret", nil).Code; got != 409 {
		t.Fatalf("busy quiesce: %d", got)
	}
	s.busy = 0
	if got := request(s.Handler(), "POST", "/v1/quiesce", "secret", nil).Code; got != 204 {
		t.Fatalf("quiesce: %d", got)
	}
	if got := request(s.Handler(), "POST", "/v1/exec", "secret", strings.NewReader(`{"argv":["/bin/true"]}`)).Code; got != 409 {
		t.Fatalf("quiesced exec: %d", got)
	}
	if got := request(s.Handler(), "POST", "/v1/unquiesce", "secret", nil).Code; got != 204 {
		t.Fatalf("unquiesce: %d", got)
	}
}

func TestIdleGuestActivation(t *testing.T) {
	c := testConfig(t)
	s, err := NewServer(c, "secret", "/bin/true", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	if err := s.Activate(); err != nil {
		t.Fatal("activation must be idempotent:", err)
	}
	if !s.active {
		t.Fatal("idle guest not active")
	}
}

func TestProxyUpstreamAuthorization(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"authorization": r.Header.Get("Authorization"), "path": r.URL.Path, "escapedPath": r.URL.EscapedPath(), "method": r.Method, "remoteAddr": r.RemoteAddr})
	}))
	defer up.Close()
	_, p, _ := net.SplitHostPort(strings.TrimPrefix(up.URL, "http://"))
	c := testConfig(t)
	s, _ := NewServer(c, "control", "/bin/true", true)
	r := httptest.NewRequest("POST", "/proxy/"+p+"/api/items?x=1", strings.NewReader("body"))
	r.Header.Set("Authorization", "Bearer control")
	r.Header.Set("X-Cellbox-Upstream-Authorization", "Bearer app")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("proxy: %d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"authorization":"Bearer app"`) || !strings.Contains(w.Body.String(), `"path":"/api/items"`) {
		t.Fatalf("proxy did not preserve target request: %s", w.Body.String())
	}
	escaped := request(s.Handler(), "GET", "/proxy/"+p+"/api%2Fitems", "control", nil)
	if escaped.Code != 200 || !strings.Contains(escaped.Body.String(), `"escapedPath":"/api%2Fitems"`) {
		t.Fatalf("escaped path changed: %d %s", escaped.Code, escaped.Body.String())
	}
	if got := request(s.Handler(), "GET", "/proxy/40000/", "control", nil).Code; got != 400 {
		t.Fatalf("control port forwarded: %d", got)
	}
	first := request(s.Handler(), "GET", "/proxy/"+p+"/reuse-one", "control", nil)
	second := request(s.Handler(), "GET", "/proxy/"+p+"/reuse-two", "control", nil)
	var a, b map[string]string
	if first.Code != 200 || second.Code != 200 || json.Unmarshal(first.Body.Bytes(), &a) != nil || json.Unmarshal(second.Body.Bytes(), &b) != nil || a["remoteAddr"] == "" || a["remoteAddr"] != b["remoteAddr"] {
		t.Fatalf("proxy did not reuse upstream connection: first=%s second=%s", first.Body.String(), second.Body.String())
	}
	s.Shutdown()
}

func TestProxyWebSocketUpgrade(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") != "websocket" {
			http.Error(w, "missing upgrade", 400)
			return
		}
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "hijack unavailable", 500)
			return
		}
		c, b, err := hj.Hijack()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = b.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		_ = b.Flush()
		buf := make([]byte, 4)
		if _, err := io.ReadFull(c, buf); err == nil {
			_, _ = c.Write(buf)
		}
	}))
	defer up.Close()
	_, p, _ := net.SplitHostPort(strings.TrimPrefix(up.URL, "http://"))
	cfg := testConfig(t)
	s, _ := NewServer(cfg, "secret", "/bin/true", true)
	guest := httptest.NewServer(s.Handler())
	defer guest.Close()
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(guest.URL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, err = fmt.Fprintf(conn, "GET /proxy/%s/ws HTTP/1.1\r\nHost: guest\r\nAuthorization: Bearer secret\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n", p)
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 101 {
		t.Fatalf("upgrade status %d", resp.StatusCode)
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 4)
	if _, err := io.ReadFull(br, b); err != nil || string(b) != "ping" {
		t.Fatalf("upgrade payload %q %v", b, err)
	}
}

func TestRunTimeoutAndOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("builds guest executable")
	}
	bin := filepath.Join(t.TempDir(), "cellbox-guest")
	b := exec.Command("go", "build", "-o", bin, "cellbox.local/cellbox/cmd/cellbox-guest")
	if out, err := b.CombinedOutput(); err != nil {
		t.Fatalf("build guest: %v: %s", err, out)
	}
	id := guestapi.Identity{UID: uint32(os.Getuid()), GID: uint32(os.Getgid())}
	ctx := context.Background()
	dir := t.TempDir()
	d, err := workspaceDir(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	r, err := run(ctx, bin, id, []string{"/bin/echo", "ok"}, d.Name(), dir, nil, 1000)
	if err != nil || r.Stdout != "ok\n" || r.ExitCode != 0 {
		t.Fatalf("exec: %+v %v", r, err)
	}
	r, err = run(ctx, bin, id, []string{"/bin/cat", "/proc/self/status"}, dir, dir, nil, 1000)
	if err != nil || !strings.Contains(r.Stdout, "NoNewPrivs:\t1") {
		t.Fatalf("no-new-privileges not set: %v", err)
	}
	start := time.Now()
	r, err = run(ctx, bin, id, []string{"/bin/sh", "-c", "sleep 5"}, dir, dir, nil, 100)
	if err != nil || r.ExitCode != 124 || time.Since(start) > 2*time.Second {
		t.Fatalf("timeout: %+v %v", r, err)
	}
	r, err = run(ctx, bin, id, []string{"/bin/sh", "-c", "head -c 1100000 /dev/zero"}, dir, dir, nil, 3000)
	if err != nil || !r.Truncated || len(r.Stdout) != maxOutput {
		t.Fatalf("bounded output: len=%d trunc=%v err=%v", len(r.Stdout), r.Truncated, err)
	}
	if _, err := exec.LookPath("setsid"); err == nil {
		start = time.Now()
		r, err = run(ctx, bin, id, []string{"/bin/sh", "-c", "setsid /bin/sh -c 'sleep 2' & exit 0"}, dir, dir, nil, 5000)
		if err != nil || r.ExitCode != 0 || !r.Truncated || time.Since(start) > 2500*time.Millisecond {
			t.Fatalf("inherited output pipes were not bounded: %+v %v elapsed=%s", r, err, time.Since(start))
		}
		start = time.Now()
		r, err = run(ctx, bin, id, []string{"/bin/sh", "-c", "setsid /bin/sh -c 'sleep 3' & sleep 5"}, dir, dir, nil, 100)
		if err != nil || r.ExitCode != 124 || !r.Truncated || time.Since(start) > 2500*time.Millisecond {
			t.Fatalf("escaped process held timed-out command open: %+v %v elapsed=%s", r, err, time.Since(start))
		}
	}
}
