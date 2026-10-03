package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/guest"
	"cellbox.local/cellbox/internal/guestapi"
)

// Exercise the public API against a real Guest, including its secure file opener.
func TestFileHTTPRangesAndHeaders(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "file-http")
	cfg := guestapi.DefaultConfig()
	cfg.Workspace = t.TempDir()
	cfg.Agent = guestapi.Identity{UID: uint32(os.Getuid()), GID: uint32(os.Getgid())}
	cfg.Debug = guestapi.Identity{UID: cfg.Agent.UID + 1, GID: cfg.Agent.GID + 1}
	if err := os.WriteFile(filepath.Join(cfg.Workspace, "hello.txt"), []byte("hello world"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.Workspace, "empty.txt"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(filepath.Join(cfg.Workspace, "large.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(17 << 20); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if err := os.Symlink("/etc/passwd", filepath.Join(cfg.Workspace, "escape.txt")); err != nil {
		t.Fatal(err)
	}
	g, err := guest.NewServer(cfg, "/bin/true", true)
	if err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(g.Handler())
	defer upstream.Close()
	f.provider.mu.Lock()
	f.provider.guestURL = upstream.URL
	f.provider.mu.Unlock()
	call := func(method, path string, headers map[string]string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/v1/boxes/"+box.ID+"/files?path="+path, nil)
		r.Header.Set("Authorization", "Bearer "+testClientToken)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		f.service.Handler().ServeHTTP(w, r)
		return w
	}
	head := call("HEAD", "hello.txt", map[string]string{"Range": "bytes=0-1"})
	if head.Code != 200 || head.Body.Len() != 0 || head.Header().Get("Content-Length") != "11" || head.Header().Get("Accept-Ranges") != "bytes" {
		t.Fatalf("HEAD: %d %v %q", head.Code, head.Header(), head.Body.String())
	}
	modified := head.Header().Get("Last-Modified")
	if modified == "" {
		t.Fatal("missing Last-Modified")
	}
	for _, tc := range []struct {
		rangeValue         string
		status             int
		body, contentRange string
	}{
		{"bytes=0-4", 206, "hello", "bytes 0-4/11"},
		{"bytes=6-", 206, "world", "bytes 6-10/11"},
		{"bytes=-5", 206, "world", "bytes 6-10/11"},
		{"bytes=100-", 416, "invalid range: failed to overlap\n", "bytes */11"},
	} {
		w := call("GET", "hello.txt", map[string]string{"Range": tc.rangeValue})
		if w.Code != tc.status || w.Body.String() != tc.body || w.Header().Get("Content-Range") != tc.contentRange {
			t.Fatalf("%s: %d %v %q", tc.rangeValue, w.Code, w.Header(), w.Body.String())
		}
	}
	if w := call("GET", "hello.txt", map[string]string{"If-Modified-Since": modified}); w.Code != 304 || w.Body.Len() != 0 {
		t.Fatalf("conditional: %d %q", w.Code, w.Body.String())
	}
	if w := call("GET", "hello.txt", map[string]string{"Range": "bytes=0-4", "If-Range": modified}); w.Code != 206 {
		t.Fatalf("If-Range match: %d", w.Code)
	}
	if w := call("GET", "hello.txt", map[string]string{"Range": "bytes=0-4", "If-Range": "Thu, 01 Jan 1970 00:00:01 GMT"}); w.Code != 200 || w.Body.String() != "hello world" {
		t.Fatalf("If-Range mismatch: %d %q", w.Code, w.Body.String())
	}
	if w := call("GET", "hello.txt", map[string]string{"If-Match": "\"unknown-version\""}); w.Code != 412 {
		t.Fatalf("precondition: %d", w.Code)
	}
	if w := call("GET", "empty.txt", nil); w.Code != 200 || w.Body.Len() != 0 {
		t.Fatalf("empty: %d", w.Code)
	}
	if w := call("GET", "large.bin", map[string]string{"Range": "bytes=16777216-16777220"}); w.Code != 206 || w.Body.Len() != 5 || w.Header().Get("Content-Range") != "bytes 16777216-16777220/17825792" {
		t.Fatalf("large range: %d %v", w.Code, w.Header())
	}
	if w := call("GET", "large.bin", nil); w.Code != 200 || w.Body.Len() != 17<<20 {
		t.Fatalf("large download: %d %d", w.Code, w.Body.Len())
	}
	for _, path := range []string{"escape.txt", "../private"} {
		if w := call("GET", path, nil); w.Code < 400 {
			t.Fatalf("unsafe path %s: %d", path, w.Code)
		}
	}
}

func TestFileStreamKeepsLeaseAndCancelsUpstream(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "file-stream")
	stopped := make(chan struct{})
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("Guest authorization was not preserved")
		}
		w.WriteHeader(200)
		_, _ = io.WriteString(w, strings.Repeat("x", 32<<10))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(stopped)
	}))
	defer source.Close()
	f.provider.mu.Lock()
	f.provider.guestURL = source.URL
	f.provider.mu.Unlock()
	api := httptest.NewServer(f.service.Handler())
	defer api.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, "GET", api.URL+"/v1/boxes/"+box.ID+"/files?path=slow.txt", nil)
	r.Header.Set("Authorization", "Bearer "+testClientToken)
	response, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if _, err := io.ReadFull(response.Body, make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	f.service.mu.Lock()
	active := len(f.service.streams)
	f.service.mu.Unlock()
	if active != 1 {
		t.Fatalf("stream lease count: %d", active)
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream was not cancelled")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		f.service.mu.Lock()
		active = len(f.service.streams)
		f.service.mu.Unlock()
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stream lease was not released")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
