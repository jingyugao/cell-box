package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/image"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

func TestImageImportCreateIsolationAndRestart(t *testing.T) {
	f := newCoreFixture(t)
	const registryPassword = "private-registry-password-never-in-state"
	server := registry.New()
	reg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "import-user" || password != registryPassword {
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		server.ServeHTTP(w, r)
	}))
	defer reg.Close()
	ref, err := name.ParseReference(strings.TrimPrefix(reg.URL, "http://")+"/user-image:v1", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	base, err := mutate.ConfigFile(empty.Image, &v1.ConfigFile{OS: "linux", Architecture: "amd64", Config: v1.Config{Env: []string{"PATH=/usr/local/bin:/usr/bin:/bin", "BASE_ENV=original"}, Entrypoint: []string{"/bin/sh", "-c"}, Cmd: []string{"echo original"}, WorkingDir: "/app", User: "1000"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = remote.Write(ref, base, remote.WithAuth(&authn.Basic{Username: "import-user", Password: registryPassword})); err != nil {
		t.Fatal(err)
	}
	defaults, err := image.ResolveRegistryImage(context.Background(), ref.Name(), "", "", &image.RegistryAuth{Username: "import-user", Password: registryPassword}, image.RunOptions{})
	if err != nil || len(defaults.Command) != 3 || defaults.Command[2] != "echo original" || defaults.WorkingDir != "/app" || defaults.Env["BASE_ENV"] != "original" {
		t.Fatalf("image startup defaults were lost: %+v %v", defaults, err)
	}
	dir := t.TempDir()
	guest := filepath.Join(dir, "guest")
	source := filepath.Join(dir, "guest.go")
	if err = os.WriteFile(source, []byte("package main\nfunc main(){}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", guest, source)
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH=amd64")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("guest build: %v %s", err, out)
	}
	buildctl := filepath.Join(dir, "buildctl")
	capture := filepath.Join(dir, "Dockerfile")
	t.Setenv("CAPTURE", capture)
	// The registry request is real; only the image builder is replaced by a fixture.
	contents := "#!/bin/sh\nset -eu\ntest -f \"$DOCKER_CONFIG/config.json\"\nwhile [ \"$#\" -gt 0 ]; do\ncase \"$1\" in\ncontext=*) cp \"${1#context=}/Dockerfile\" \"$CAPTURE\" ;;\n--metadata-file) shift; metadata=$1 ;;\nesac\nshift\ndone\nprintf '%s' '{\"containerimage.digest\":\"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}' > \"$metadata\"\n"
	if err = os.WriteFile(buildctl, []byte(contents), 0700); err != nil {
		t.Fatal(err)
	}
	f.config.ImageBuild = ImageBuildConfig{Address: "unix:///tmp/buildkit.sock", Repository: "example.com/prepared", GuestBinary: guest, BuildctlBinary: buildctl}
	f.config.Profiles[0].Clients = append(f.config.Profiles[0].Clients, "client-b")
	f.reopen(t)
	const buildCommand = "mkdir -p /opt/product && printf platform > /opt/product/platform.txt"
	input := importImageRequest{URL: ref.Name(), BuildCommand: buildCommand, RunCommand: "docker run -e BASE_ENV=overridden -w /tmp " + ref.Name() + " 'echo imported'", RegistryAuth: &image.RegistryAuth{Username: "import-user", Password: registryPassword}}
	status, raw := f.call(t, "POST", "/v1/images:import", testClientToken, "import-one", input)
	wantStatus(t, status, 202, raw)
	op := f.waitOperation(t, decodeResponse[Operation](t, raw).ID, "succeeded")
	id := op.Result["importedImageId"]
	if id == "" {
		t.Fatal("import did not return an ID")
	}
	status, raw = f.call(t, "GET", "/v1/images/"+id, testClientToken, "", nil)
	wantStatus(t, status, 200, raw)
	imported := decodeResponse[ImportedImage](t, raw)
	dockerfile, err := os.ReadFile(capture)
	wantRun, _ := json.Marshal([]string{"/bin/sh", "-c", buildCommand})
	if err != nil || !strings.Contains(string(dockerfile), "RUN "+string(wantRun)) || imported.BuildCommand != buildCommand {
		t.Fatalf("build command did not reach builder and record: %s %+v %v", dockerfile, imported, err)
	}
	if !strings.Contains(imported.ResolvedSource, "@sha256:") || imported.Env["BASE_ENV"] != "overridden" || imported.WorkingDir != "/tmp" || len(imported.Command) != 3 || imported.Command[2] != "echo imported" {
		t.Fatalf("imported startup config: %+v", imported)
	}
	status, raw = f.call(t, "POST", "/v1/images:import", testClientToken, "import-one", input)
	wantStatus(t, status, 202, raw)
	if decodeResponse[Operation](t, raw).ID != op.ID {
		t.Fatal("idempotent import started again")
	}
	input.BuildCommand = "echo different"
	status, raw = f.call(t, "POST", "/v1/images:import", testClientToken, "import-one", input)
	wantStatus(t, status, 409, raw)
	input.BuildCommand = "\x00"
	status, raw = f.call(t, "POST", "/v1/images:import", testClientToken, "invalid-build", input)
	wantStatus(t, status, 400, raw)
	input.BuildCommand = buildCommand
	status, raw = f.call(t, "GET", "/v1/images/"+id, otherClientToken, "", nil)
	wantStatus(t, status, 404, raw)
	status, raw = f.call(t, "POST", "/v1/boxes", otherClientToken, "foreign-image", createRequest{ProfileID: "profile-a", OwnerKey: "foreign", ImportedImageID: id})
	wantStatus(t, status, 404, raw)
	input.RunCommand = "docker run --privileged " + ref.Name()
	status, raw = f.call(t, "POST", "/v1/images:import", testClientToken, "unsafe", input)
	wantStatus(t, status, 400, raw)
	status, raw = f.call(t, "POST", "/v1/boxes", testClientToken, "create-imported", createRequest{ProfileID: "profile-a", OwnerKey: "import-demo", ImportedImageID: id})
	wantStatus(t, status, 202, raw)
	created := f.waitOperation(t, decodeResponse[Operation](t, raw).ID, "succeeded")
	b, err := f.service.rawBox(created.TargetID)
	if err != nil {
		t.Fatal(err)
	}
	if b.Box.Image != imported.Image || b.Box.ImportedImageID != id || b.Profile.Guest.CommandDir != "/tmp" || b.Profile.Guest.Env["BASE_ENV"] != "overridden" {
		t.Fatalf("box did not use imported config: %+v", b)
	}
	if f.service.config.Profiles[0].Image == imported.Image || f.service.config.Profiles[0].Guest.Env["BASE_ENV"] != "" {
		t.Fatal("import mutated shared profile")
	}
	privilegedProfile := f.config.Profiles[0]
	privilegedProfile.DebugReadWriteHostPath = "/host/private"
	privilegedProfile.Guest.Tools = []guestapi.Tool{{ID: "private-tool", Executable: "/opt/cellbox/tools/private-tool"}}
	isolated, err := f.service.importedProfile("client-a", id, privilegedProfile)
	if err != nil || len(isolated.Guest.Tools) != 0 || isolated.DebugReadWriteHostPath != "" {
		t.Fatalf("import retained trusted tool or host access: %+v %v", isolated, err)
	}
	f.reopen(t)
	status, raw = f.call(t, "GET", "/v1/images/"+id, testClientToken, "", nil)
	wantStatus(t, status, 200, raw)
	if decodeResponse[ImportedImage](t, raw).BuildCommand != buildCommand {
		t.Fatal("buildCommand lost after API restart")
	}
	state, err := os.ReadFile(filepath.Join(f.config.DataDir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(state), registryPassword) {
		t.Fatal("registry password persisted")
	}
	if err := f.service.store.View(func(st State) error {
		if len(st.ImportedImages) != 1 {
			t.Fatalf("image records: %d", len(st.ImportedImages))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Keep the digest pinned even if the original mutable tag now points elsewhere.
	changed, err := mutate.Config(base, v1.Config{Cmd: []string{"/different"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = remote.Write(ref, changed, remote.WithContext(context.Background()), remote.WithAuth(&authn.Basic{Username: "import-user", Password: registryPassword})); err != nil {
		t.Fatal(err)
	}
	status, raw = f.call(t, "GET", "/v1/images/"+id, testClientToken, "", nil)
	wantStatus(t, status, 200, raw)
	if decodeResponse[ImportedImage](t, raw).ResolvedSource != imported.ResolvedSource {
		t.Fatal("source tag changed imported image identity")
	}
	var stateObject map[string]any
	if err := json.Unmarshal(state, &stateObject); err != nil {
		t.Fatal(err)
	}
}
