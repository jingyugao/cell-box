package image

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareBuildKitReturnsPublishedDigest(t *testing.T) {
	guest := staticGuest(t)
	dir := t.TempDir()
	product := filepath.Join(dir, "product")
	if err := os.Mkdir(product, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(product, "app.txt"), []byte("product"), 0644); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(dir, "Dockerfile")
	script := filepath.Join(dir, "buildctl")
	contents := "#!/bin/sh\nset -eu\nwhile [ \"$#\" -gt 0 ]; do\n  case \"$1\" in\n    context=*) cp \"${1#context=}/Dockerfile\" \"$CAPTURE\" ;;\n    --metadata-file) shift; metadata=$1 ;;\n  esac\n  shift\ndone\nprintf '%s' '{\"containerimage.digest\":\"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}' > \"$metadata\"\n"
	if err := os.WriteFile(script, []byte(contents), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CAPTURE", capture)
	base := "example.com/base@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	result, err := PrepareBuildKit(context.Background(), BuildKitOptions{Address: "unix:///tmp/buildkitd.sock", Binary: script, Repository: "example.com/cellbox", Base: base, GuestBinary: guest, ProductDir: product, Platform: "linux/amd64"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Image != "example.com/cellbox@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" || !strings.HasPrefix(result.Tag, "example.com/cellbox:cellbox-") {
		t.Fatalf("wrong published image: %+v", result)
	}
	dockerfile, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(dockerfile), "FROM "+base+"\n") || !strings.Contains(string(dockerfile), "COPY product/ /opt/product/") {
		t.Fatalf("wrong fixed Dockerfile: %s", dockerfile)
	}
}

func TestPrepareBuildKitRejectsMutableBase(t *testing.T) {
	_, err := PrepareBuildKit(context.Background(), BuildKitOptions{Address: "unix:///tmp/buildkitd.sock", Repository: "example.com/cellbox", Base: "ubuntu:latest"})
	if err == nil {
		t.Fatal("mutable base was accepted")
	}
}
