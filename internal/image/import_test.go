package image

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

func TestRunCommandIsDataAndPreservesLiteralArguments(t *testing.T) {
	out, err := ParseRunCommand(`docker run --rm -d -e 'MESSAGE=hello world' -w /app -p 127.0.0.1:8089:8080 --entrypoint /bin/sh example.com/app:v1 -c 'printf "$MESSAGE"'`)
	if err != nil {
		t.Fatal(err)
	}
	if out.Image != "example.com/app:v1" || out.Env["MESSAGE"] != "hello world" || out.WorkingDir != "/app" || len(out.Command) != 2 || out.Command[1] != `printf "$MESSAGE"` || out.Ports[0] != 8080 {
		t.Fatalf("unexpected conversion: %+v", out)
	}
	for _, command := range []string{
		`docker run example.com/app:v1; touch /tmp/escaped`,
		`docker run -e PASSWORD=$(cat /etc/passwd) example.com/app:v1`,
		"docker run -e PASSWORD=`id` example.com/app:v1",
		`docker run -e VALUE=$HOME example.com/app:v1`,
		`docker run example.com/app:v1 > /tmp/escaped`,
		`docker run --privileged example.com/app:v1`,
		`docker run -v /:/host example.com/app:v1`,
		`docker run --network host example.com/app:v1`,
		`docker run -e FROM_SERVER_ENV example.com/app:v1`,
	} {
		if _, err := ParseRunCommand(command); err == nil {
			t.Fatalf("accepted unsupported command %q", command)
		}
	}
	defaults, err := ParseRunCommand(`docker run example.com/app:v1`)
	if err != nil || defaults.Command != nil {
		t.Fatalf("image CMD was cleared: %+v, %v", defaults, err)
	}
	compact, err := ParseRunCommand(`docker run -eMODE=demo -w/app -p8080:80 example.com/app:v1`)
	if err != nil || compact.Env["MODE"] != "demo" || compact.WorkingDir != "/app" || compact.Ports[0] != 80 {
		t.Fatalf("compact options failed: %+v %v", compact, err)
	}
}

func TestMirrorKeepsSourceAndPlatformCredentialsSeparate(t *testing.T) {
	backend := registry.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || password != "test-password" {
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if strings.Contains(r.URL.Path, "/source/") && user != "source" || strings.Contains(r.URL.Path, "/prepared/") && user != "platform" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		backend.ServeHTTP(w, r)
	}))
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	ref, err := name.ParseReference(host+"/source:v1", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	base, err := random.Image(1024, 1)
	if err != nil {
		t.Fatal(err)
	}
	configFile, err := base.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	configFile.OS, configFile.Architecture = "linux", "amd64"
	base, err = mutate.ConfigFile(base, configFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, base, remote.WithAuth(&authn.Basic{Username: "source", Password: "test-password"})); err != nil {
		t.Fatal(err)
	}
	resolved, err := ResolveRegistryImage(context.Background(), ref.Name(), "linux/amd64", host, &RegistryAuth{Username: "source", Password: "test-password"}, RunOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	config, _ := json.Marshal(map[string]any{"auths": map[string]any{host: map[string]string{"auth": base64.StdEncoding.EncodeToString([]byte("platform:test-password"))}}})
	if err := os.WriteFile(filepath.Join(dir, "config.json"), config, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOCKER_CONFIG", dir)
	mirrored, err := MirrorRegistryImage(context.Background(), resolved, host+"/prepared", host)
	if err != nil {
		t.Fatal(err)
	}
	destination, err := name.ParseReference(mirrored, name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	copy, err := remote.Image(destination, remote.WithAuthFromKeychain(authn.DefaultKeychain))
	if err != nil {
		t.Fatal(err)
	}
	originalDigest, _ := base.Digest()
	copiedDigest, err := copy.Digest()
	if err != nil || originalDigest != copiedDigest {
		t.Fatalf("mirror changed source image: %v %v %v", originalDigest, copiedDigest, err)
	}
}
