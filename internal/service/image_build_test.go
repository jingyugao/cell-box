package service

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestExtractProductRejectsTraversalAndLinks(t *testing.T) {
	for _, entry := range []tar.Header{
		{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0644, Size: 1},
		{Name: "/escape", Typeflag: tar.TypeReg, Mode: 0644, Size: 1},
		{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"},
	} {
		var buf bytes.Buffer
		tw := tar.NewWriter(&buf)
		if err := tw.WriteHeader(&entry); err != nil {
			t.Fatal(err)
		}
		if entry.Size != 0 {
			if _, err := tw.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		if err := tw.Close(); err != nil {
			t.Fatal(err)
		}
		if err := extractProduct(tar.NewReader(&buf), t.TempDir()); err == nil {
			t.Fatalf("accepted unsafe tar entry: %q", entry.Name)
		}
	}
}

func TestExtractProductKeepsRegularFile(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "app/run.sh", Typeflag: tar.TypeReg, Mode: 0755, Size: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := extractProduct(tar.NewReader(&buf), dir); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "app", "run.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0755 {
		t.Fatalf("wrong file mode: %o", info.Mode().Perm())
	}
}

func TestBuildImageOperationReturnsRegistryReference(t *testing.T) {
	dir := t.TempDir()
	guestSource := filepath.Join(dir, "guest.go")
	guest := filepath.Join(dir, "guest")
	if err := os.WriteFile(guestSource, []byte("package main\nfunc main() {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	compile := exec.Command("go", "build", "-o", guest, guestSource)
	compile.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux")
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("guest build: %v: %s", err, output)
	}
	script := filepath.Join(dir, "buildctl")
	contents := "#!/bin/sh\nset -eu\nwhile [ \"$#\" -gt 0 ]; do\n  if [ \"$1\" = '--metadata-file' ]; then shift; metadata=$1; fi\n  shift\ndone\nprintf '%s' '{\"containerimage.digest\":\"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}' > \"$metadata\"\n"
	if err := os.WriteFile(script, []byte(contents), 0700); err != nil {
		t.Fatal(err)
	}
	const authToken = "0123456789abcdef0123456789abcdef"
	app, err := New(Config{DataDir: filepath.Join(dir, "data"), ClientID: "test", ImageBuild: ImageBuildConfig{Address: "unix:///tmp/buildkitd.sock", Repository: "example.com/cellbox", GuestBinary: guest, BuildctlBinary: script}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	base := "example.com/base@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err := form.WriteField("baseImage", base); err != nil {
		t.Fatal(err)
	}
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/images", &body)
	req.Header.Set("Content-Type", form.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+authToken)
	req.Header.Set("Idempotency-Key", "image-1")
	res := httptest.NewRecorder()
	app.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusAccepted {
		t.Fatalf("build request failed: %d %s", res.Code, res.Body.String())
	}
	var op Operation
	if err := json.Unmarshal(res.Body.Bytes(), &op); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		get := httptest.NewRequest(http.MethodGet, "/v1/operations/"+op.ID, nil)
		get.Header.Set("Authorization", "Bearer "+authToken)
		response := httptest.NewRecorder()
		app.Handler().ServeHTTP(response, get)
		if response.Code != http.StatusOK {
			t.Fatalf("operation status: %d", response.Code)
		}
		if err := json.NewDecoder(response.Body).Decode(&op); err != nil {
			t.Fatal(err)
		}
		if op.Status == "succeeded" {
			if op.Result["image"] != "example.com/cellbox@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
				t.Fatalf("wrong image result: %+v", op.Result)
			}
			return
		}
		if op.Status == "failed" {
			t.Fatalf("image build failed: %+v", op.Error)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("image build did not finish")
}
