package image

import (
	"context"
	"encoding/json"
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
	opts := BuildKitOptions{Address: "unix:///tmp/buildkitd.sock", Binary: script, Repository: "example.com/cellbox", Base: base, GuestBinary: guest, ProductDir: product, Platform: "linux/amd64"}
	result, err := PrepareBuildKit(context.Background(), opts)
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
	canary := filepath.Join(dir, "host-canary")
	opts.BuildCommand = "touch " + canary + "\nprintf 'packed\\n' > /opt/product/platform.txt\n# FROM scratch\n# COPY guest /attacker"
	built, err := PrepareBuildKit(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if built.Key == result.Key || built.Tag == result.Tag {
		t.Fatal("build command did not change cache identity")
	}
	if _, err := os.Stat(canary); !os.IsNotExist(err) {
		t.Fatal("build command ran on the API host")
	}
	dockerfile, err = os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(dockerfile)), "\n")
	if len(lines) != 6 || lines[1] != "COPY product/ /opt/product/" || lines[2] != "USER 0:0" || !strings.HasPrefix(lines[4], "LABEL ") || lines[5] != "COPY --chmod=0755 guest /opt/cellbox/bin/cellbox-container-agent" {
		t.Fatalf("build command escaped its stage or overwrote guest injection: %s", dockerfile)
	}
	var argv []string
	if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[3], "RUN ")), &argv); err != nil || len(argv) != 3 || argv[0] != "/bin/sh" || argv[1] != "-c" || argv[2] != opts.BuildCommand {
		t.Fatalf("build shell argument was altered: %v %v", argv, err)
	}
	repeat, err := PrepareBuildKit(context.Background(), opts)
	if err != nil || repeat.Key != built.Key {
		t.Fatalf("same build command changed cache identity: %+v %v", repeat, err)
	}
	opts.BuildCommand += "\ntrue"
	different, err := PrepareBuildKit(context.Background(), opts)
	if err != nil || different.Key == built.Key {
		t.Fatalf("different build commands shared cache identity: %+v %v", different, err)
	}
	for _, command := range []string{"\x00", " \n\t", strings.Repeat("x", (64<<10)+1)} {
		opts.BuildCommand = command
		if _, err := PrepareBuildKit(context.Background(), opts); err == nil {
			t.Fatal("invalid build command accepted")
		}
	}
}

func TestPrepareBuildKitRejectsMutableBase(t *testing.T) {
	_, err := PrepareBuildKit(context.Background(), BuildKitOptions{Address: "unix:///tmp/buildkitd.sock", Repository: "example.com/cellbox", Base: "ubuntu:latest"})
	if err == nil {
		t.Fatal("mutable base was accepted")
	}
}
