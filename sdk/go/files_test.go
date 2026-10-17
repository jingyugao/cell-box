package cellbox

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestFilesPreserveBinaryDataAndEncodeWorkspacePaths(t *testing.T) {
	name := "folder/工具 #?&.bin"
	data := []byte{0, 255, 128, '\n', '"'}
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertHeaders(t, r)
		if r.URL.Path != "/gateway/v1/boxes/box-1/files" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if r.URL.Query().Get("list") == "1" {
			if r.URL.Query().Get("path") != "." {
				t.Errorf("root list path = %q", r.URL.Query().Get("path"))
			}
			writeJSONTest(w, []FileEntry{{Name: "folder", Directory: true}})
			return
		}
		if r.URL.Query().Get("path") != name || len(r.URL.Query()) != 1 {
			t.Errorf("encoded query = %s", r.URL.RawQuery)
		}
		if r.Method == http.MethodPut {
			writes.Add(1)
			body, _ := io.ReadAll(r.Body)
			if !bytes.Equal(body, data) || r.Header.Get("Content-Type") != "application/octet-stream" {
				t.Errorf("write was not raw binary: %q", body)
			}
			w.WriteHeader(204)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	}))
	defer server.Close()
	f := testClient(t, server, "/gateway").sandbox(SandboxInfo{ID: "box-1", Generation: 1}).Files
	ctx := context.Background()
	if err := f.Write(ctx, name, data); err != nil {
		t.Fatal(err)
	}
	read, err := f.Read(ctx, name)
	if err != nil || !bytes.Equal(read, data) || writes.Load() != 1 {
		t.Fatalf("read=%v err=%v writes=%d", read, err, writes.Load())
	}
	entries, err := f.List(ctx, "")
	if err != nil || len(entries) != 1 || !entries[0].Directory {
		t.Fatalf("list=%+v err=%v", entries, err)
	}
}

func TestFilesRejectUnsafeAndOversizedWritesBeforeRequest(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1) }))
	defer server.Close()
	f := testClient(t, server, "").sandbox(SandboxInfo{ID: "box-1", Generation: 1}).Files
	for _, name := range []string{"", ".", "../secret", "a/../b", "/etc/passwd", "a//b", "a/./b", "a\x00b"} {
		if err := f.Write(context.Background(), name, nil); err == nil {
			t.Errorf("unsafe path accepted: %q", name)
		}
	}
	if err := f.Write(context.Background(), "large", make([]byte, MaxBufferedFileSize+1)); err == nil {
		t.Error("oversized write accepted")
	}
	if requests.Load() != 0 {
		t.Fatalf("sent %d invalid requests", requests.Load())
	}
}

func TestFileReadIsBoundedWhileOpenStreamsAndErrorsRemainTyped(t *testing.T) {
	data := bytes.Repeat([]byte{42}, MaxBufferedFileSize+1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("path") == "missing" {
			w.WriteHeader(404)
			writeJSONTest(w, map[string]any{"error": APIError{Code: "NOT_FOUND", Message: "missing"}})
			return
		}
		_, _ = w.Write(data)
	}))
	defer server.Close()
	f := testClient(t, server, "").sandbox(SandboxInfo{ID: "box-1"}).Files
	if b, err := f.Read(context.Background(), "large"); err == nil || b != nil {
		t.Fatalf("oversized read returned bytes=%d err=%v", len(b), err)
	}
	body, err := f.Open(context.Background(), "large")
	if err != nil {
		t.Fatal(err)
	}
	n, err := io.Copy(io.Discard, body)
	body.Close()
	if err != nil || n != int64(len(data)) {
		t.Fatalf("streamed %d bytes: %v", n, err)
	}
	_, err = f.Open(context.Background(), "missing")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 404 || apiErr.Code != "NOT_FOUND" {
		t.Fatalf("file error lost API details: %v", err)
	}
}
