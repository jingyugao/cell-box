package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
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
	f.reopen(t)
	const buildCommand = "mkdir -p /opt/product && printf platform > /opt/product/platform.txt"
	cacheStarted, cacheRelease := make(chan string, 1), make(chan struct{})
	f.provider.cacheImage = func(ctx context.Context, image, _, _ string) error {
		cacheStarted <- image
		select {
		case <-cacheRelease:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	input := importImageRequest{URL: ref.Name(), BuildCommand: buildCommand, RunCommand: "docker run -e BASE_ENV=overridden -w /tmp " + ref.Name() + " 'echo imported'", RegistryAuth: &image.RegistryAuth{Username: "import-user", Password: registryPassword}}
	status, raw := f.call(t, "POST", "/v1/images:import", testClientToken, "import-one", input)
	wantStatus(t, status, 202, raw)
	importOp := decodeResponse[Operation](t, raw)
	select {
	case cached := <-cacheStarted:
		if !strings.Contains(cached, "example.com/prepared@sha256:") {
			t.Fatalf("cached source instead of prepared digest: %s", cached)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("import did not request runtime caching")
	}
	status, pending := f.call(t, "GET", "/v1/operations/"+importOp.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, pending)
	if decodeResponse[Operation](t, pending).Status != "running" {
		t.Fatal("import completed before image was cached")
	}
	status, missing := f.call(t, "GET", "/v1/images/"+importOp.TargetID, testClientToken, "", nil)
	wantStatus(t, status, 404, missing)
	close(cacheRelease)
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
	privilegedProfile.SharedReadOnlyHostPath = "/host/shared"
	privilegedProfile.Guest.Tools = []guestapi.Tool{{ID: "private-tool", Executable: "/opt/cellbox/tools/private-tool"}}
	privilegedProfile.Guest.Env = map[string]string{"SHARED_STARTUP_DIRECTORY": api.SharedMountPath,
		"SHARED_CONFIG_PATH": api.SharedMountPath + "/runtime/config.json", "SAFE_PROFILE_ENV": "preserved"}
	isolated, err := f.service.importedProfile("client-a", id, privilegedProfile)
	if err != nil || len(isolated.Guest.Tools) != 0 || isolated.DebugReadWriteHostPath != "" {
		t.Fatalf("import retained trusted tool or host access: %+v %v", isolated, err)
	}
	if isolated.Guest.Env["SHARED_STARTUP_DIRECTORY"] != "" || isolated.Guest.Env["SHARED_CONFIG_PATH"] != "" || isolated.Guest.Env["SAFE_PROFILE_ENV"] != "preserved" {
		t.Fatal("import inherited absent shared mount paths or lost safe environment")
	}
	if err := f.service.store.View(func(st State) error {
		if st.ImportedImages[id].ImportedImage.Env["SAFE_PROFILE_ENV"] != "" {
			t.Fatal("profile preparation mutated imported image metadata")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	privilegedProfile.TrustedToolImages = []string{imported.Image}
	privilegedProfile.MountedToolRuntimeImages = []string{imported.Image}
	approved, err := f.service.importedProfile("client-a", id, privilegedProfile)
	if err != nil || len(approved.Guest.Tools) != 1 || approved.DebugReadWriteHostPath != "" || approved.SharedReadOnlyHostPath != "/host/shared" {
		t.Fatalf("approved image lost shared mount or gained writable host access: %+v %v", approved, err)
	}
	if approved.Guest.Env["SHARED_STARTUP_DIRECTORY"] != api.SharedMountPath || approved.Guest.Env["SHARED_CONFIG_PATH"] != api.SharedMountPath+"/runtime/config.json" || !profileCapabilities(approved).SharedDirectory {
		t.Fatal("approved image lost mounted startup configuration")
	}
	approved.Guest.Debug = guestapi.Identity{}
	if !profileCapabilities(approved).MountedToolRuntime {
		t.Fatal("admitted mounted-runtime image did not advertise its capability")
	}
	if profileCapabilities(approved).MountedDebugHome {
		t.Fatal("old image advertised a HOME mount")
	}
	privilegedProfile.Guest.Debug = guestapi.Identity{}
	privilegedProfile.DebugHomeImages = []string{imported.Image}
	privilegedProfile.DebugReadWriteHostPath = "/host/shared/runtime/debug-homes"
	homeProfile, err := f.service.importedProfile("client-a", id, privilegedProfile)
	if err != nil || !profileCapabilities(homeProfile).MountedDebugHome || homeProfile.DebugReadWriteHostPath != privilegedProfile.DebugReadWriteHostPath {
		t.Fatalf("admitted HOME image lost native mount: %+v %v", homeProfile, err)
	}
	homeProfile.DebugReadWriteHostPath = "/host/private"
	if profileCapabilities(homeProfile).MountedDebugHome {
		t.Fatal("unmanaged host directory advertised mounted debug HOME")
	}
	approved.MountedToolRuntimeImages = nil
	if profileCapabilities(approved).MountedToolRuntime {
		t.Fatal("old image advertised mounted-runtime support")
	}
	privilegedProfile.TrustedToolImages = []string{imported.Image + "different"}
	unapproved, err := f.service.importedProfile("client-a", id, privilegedProfile)
	if err != nil || len(unapproved.Guest.Tools) != 0 {
		t.Fatal("a different image inherited trusted tools")
	}
	if unapproved.SharedReadOnlyHostPath != "" || profileCapabilities(unapproved).MountedToolRuntime {
		t.Fatal("unadmitted image inherited the shared mount")
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
	f.provider.cacheImage = func(context.Context, string, string, string) error { return errors.New("runtime registry unavailable") }
	input.RunCommand = "docker run " + ref.Name()
	status, raw = f.call(t, "POST", "/v1/images:import", testClientToken, "import-cache-failure", input)
	wantStatus(t, status, 202, raw)
	failed := f.waitOperation(t, decodeResponse[Operation](t, raw).ID, "failed")
	status, raw = f.call(t, "GET", "/v1/images/"+failed.TargetID, testClientToken, "", nil)
	wantStatus(t, status, 404, raw)
}

func TestImportedImageUsageBlocksPausedAndHidesForeignOwnership(t *testing.T) {
	f := newCoreFixture(t)
	id := "img-11111111111111111111111111111111"
	prepared := "registry.example/prepared@sha256:" + strings.Repeat("c", 64)
	if err := f.service.store.Update(func(st *State) error {
		st.ImportedImages[id] = importedImageRecord{ImportedImage: ImportedImage{ID: id, Image: prepared}, ClientID: "client-a"}
		st.Boxes["paused-box"] = boxRecord{Box: Box{ID: "paused-box", Phase: "suspended", Image: prepared, ImportedImageID: id}, ClientID: "client-a"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f.service.config.ImageBuild = ImageBuildConfig{Repository: "registry.example/prepared"}
	status, body := f.call(t, "GET", "/v1/images/"+id+"/usage", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	usage := decodeResponse[imageUsage](t, body)
	if usage.Deletable || len(usage.Blockers) != 1 || usage.Blockers[0] != "boxes-reference-image" {
		t.Fatalf("paused box was not reported as blocker: %+v", usage)
	}
	status, body = f.call(t, "GET", "/v1/images/"+id+"/usage", otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "DELETE", "/v1/images/"+id, otherClientToken, "foreign-delete", nil)
	wantStatus(t, status, 404, body)
}

func TestImportedImageUsageCountsCrossClientBoxesAndArchives(t *testing.T) {
	f := newCoreFixture(t)
	f.service.config.ImageBuild = ImageBuildConfig{Repository: "registry.example/prepared"}
	id := "img-33333333333333333333333333333333"
	digest := "sha256:" + strings.Repeat("d", 64)
	prepared := "registry.example/prepared@" + digest
	if err := f.service.store.Update(func(st *State) error {
		st.ImportedImages[id] = importedImageRecord{ImportedImage: ImportedImage{ID: id, Image: prepared}, ClientID: "client-a"}
		st.Boxes["foreign-box"] = boxRecord{Box: Box{ID: "foreign-box", Phase: "failed", ImageID: digest}, Profile: Profile{Image: "registry.example/prepared:alias@" + digest}, ClientID: "client-b"}
		st.Archives["arc-33333333333333333333333333333333"] = archiveRecord{Archive: Archive{ID: "arc-33333333333333333333333333333333", ImageID: digest, PreparedImage: prepared, ManifestVersion: 1}, ClientID: "client-b"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	status, body := f.call(t, "GET", "/v1/images/"+id+"/usage", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	usage := decodeResponse[imageUsage](t, body)
	if usage.Deletable || len(usage.Blockers) != 2 || usage.Blockers[0] != "boxes-reference-image" || usage.Blockers[1] != "archives-reference-image" {
		t.Fatalf("cross-client image references were not counted generically: %+v", usage)
	}
	if err := f.service.store.Update(func(st *State) error {
		box := st.Boxes["foreign-box"]
		box.Box.Phase = "deleted"
		st.Boxes["foreign-box"] = box
		delete(st.Archives, "arc-33333333333333333333333333333333")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	status, body = f.call(t, "GET", "/v1/images/"+id+"/usage", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if usage := decodeResponse[imageUsage](t, body); !usage.Deletable {
		t.Fatalf("deleted box or removed archive blocked deletion: %+v", usage)
	}
	f.service.config.Profiles[0].Image = "registry.example/prepared:profile-alias@" + digest
	status, body = f.call(t, "GET", "/v1/images/"+id+"/usage", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if usage := decodeResponse[imageUsage](t, body); usage.Deletable || len(usage.Blockers) != 1 || usage.Blockers[0] != "profiles-reference-image" {
		t.Fatalf("canonical profile digest reference was not counted: %+v", usage)
	}
}

func TestArchiveBlocksImageDeletionWithoutSourceBox(t *testing.T) {
	for _, useAlias := range []bool{false, true} {
		name := "same-imported-id"
		if useAlias {
			name = "shared-manifest-alias"
		}
		t.Run(name, func(t *testing.T) {
			f := newCoreFixture(t)
			f.service.config.ImageBuild = ImageBuildConfig{Repository: "registry.example/prepared"}
			targetID := "img-11111111111111111111111111111111"
			aliasID := "img-22222222222222222222222222222222"
			digest := "sha256:" + strings.Repeat("e", 64)
			prepared := "registry.example/prepared@" + digest
			sourceImportedID := targetID
			if useAlias {
				sourceImportedID = aliasID
			}
			if err := f.service.store.Update(func(st *State) error {
				st.ImportedImages[targetID] = importedImageRecord{ImportedImage: ImportedImage{ID: targetID, Image: prepared}, ClientID: "client-a"}
				if useAlias {
					st.ImportedImages[aliasID] = importedImageRecord{ImportedImage: ImportedImage{
						ID: aliasID, Image: "registry.example/prepared:source-alias@" + digest,
					}, ClientID: "client-b"}
				}
				const sourceBoxID = "box-deleted-docker-source"
				st.Archives["arc-44444444444444444444444444444444"] = archiveRecord{Archive: Archive{
					ID: "arc-44444444444444444444444444444444", SourceBoxID: sourceBoxID,
					ManifestVersion: 1, ImportedImageID: sourceImportedID, PreparedImage: prepared, Portable: true,
					ImageID: "sha256:" + strings.Repeat("f", 64),
				}, ClientID: "client-b"}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			status, body := f.call(t, "GET", "/v1/images/"+targetID+"/usage", testClientToken, "", nil)
			wantStatus(t, status, 200, body)
			usage := decodeResponse[imageUsage](t, body)
			if usage.Deletable || !slices.Contains(usage.Blockers, "archives-reference-image") {
				t.Fatalf("archive did not block image deletion without its source box: %+v", usage)
			}
			if useAlias && !usage.ManifestShared {
				t.Fatalf("alias record did not share the prepared manifest: %+v", usage)
			}

			if err := f.service.store.Update(func(st *State) error {
				delete(st.Archives, "arc-44444444444444444444444444444444")
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			status, body = f.call(t, "GET", "/v1/images/"+targetID+"/usage", testClientToken, "", nil)
			wantStatus(t, status, 200, body)
			if usage := decodeResponse[imageUsage](t, body); !usage.Deletable {
				t.Fatalf("removed archive blocked image deletion: %+v", usage)
			}
		})
	}
}

func TestImportedImageDeleteSharedDigestRemovesOnlyMetadata(t *testing.T) {
	f := newCoreFixture(t)
	f.service.config.ImageBuild = ImageBuildConfig{Repository: "registry.example/prepared"}
	imageRef := "registry.example/prepared@sha256:" + strings.Repeat("b", 64)
	for _, id := range []string{"img-11111111111111111111111111111111", "img-22222222222222222222222222222222"} {
		if err := f.service.store.Update(func(st *State) error {
			st.ImportedImages[id] = importedImageRecord{ImportedImage: ImportedImage{ID: id, Image: imageRef}, ClientID: "client-a"}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	status, body := f.call(t, "GET", "/v1/images/img-11111111111111111111111111111111/usage", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if usage := decodeResponse[imageUsage](t, body); !usage.Deletable || !usage.ManifestShared {
		t.Fatalf("shared manifest usage: %+v", usage)
	}
	status, body = f.call(t, "DELETE", "/v1/images/img-11111111111111111111111111111111", testClientToken, "delete-shared", nil)
	wantStatus(t, status, 202, body)
	done := f.waitOperation(t, decodeResponse[Operation](t, body).ID, "succeeded")
	status, body = f.call(t, "DELETE", "/v1/images/img-11111111111111111111111111111111", testClientToken, "delete-shared", nil)
	wantStatus(t, status, 202, body)
	if decodeResponse[Operation](t, body).ID != done.ID {
		t.Fatal("metadata-only delete idempotency replay changed operation")
	}
	if done.Result["metadataOnly"] != "true" {
		t.Fatalf("shared digest deletion did not report metadata-only result: %+v", done.Result)
	}
	if err := f.service.store.View(func(st State) error {
		if _, ok := st.ImportedImages["img-11111111111111111111111111111111"]; ok {
			t.Fatal("target imported image metadata remains")
		}
		if _, ok := st.ImportedImages["img-22222222222222222222222222222222"]; !ok {
			t.Fatal("shared imported image metadata was removed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestImportedImageDeleteRetryAfterRestartAndBlocksCreates(t *testing.T) {
	f := newCoreFixture(t)
	registryStore := registry.New()
	var deleteCount atomic.Int32
	allowDelete := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/manifests/") {
			switch deleteCount.Add(1) {
			case 1:
				http.Error(w, "temporary registry failure", http.StatusForbidden)
				return
			case 2:
				<-allowDelete
			}
		}
		registryStore.ServeHTTP(w, r)
	}))
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	ref, err := name.ParseReference(host+"/prepared:initial", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, empty.Image); err != nil {
		t.Fatal(err)
	}
	digest, err := empty.Image.Digest()
	if err != nil {
		t.Fatal(err)
	}
	imageRef := host + "/prepared@" + digest.String()
	id := "img-11111111111111111111111111111111"
	f.config.ImageBuild = ImageBuildConfig{Address: "unix:///tmp/buildkit.sock", Repository: host + "/prepared", GuestBinary: "/bin/true", InsecureRegistry: host}
	f.service.config.ImageBuild = f.config.ImageBuild
	if err := f.service.store.Update(func(st *State) error {
		st.ImportedImages[id] = importedImageRecord{ImportedImage: ImportedImage{ID: id, Image: imageRef}, ClientID: "client-a"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	status, body := f.call(t, "DELETE", "/v1/images/"+id, testClientToken, "delete-first", nil)
	wantStatus(t, status, 202, body)
	failed := f.waitOperation(t, decodeResponse[Operation](t, body).ID, "failed")
	if failed.Error == nil || failed.Error.Code != "IMAGE_DELETE_FAILED" {
		t.Fatalf("registry failure was not surfaced: %+v", failed.Error)
	}
	if err := f.service.store.View(func(st State) error {
		if !st.ImportedImages[id].ImportedImage.Deleting {
			t.Fatal("failed registry deletion made imported image available")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	status, body = f.call(t, "GET", "/v1/images/"+id+"/usage", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if usage := decodeResponse[imageUsage](t, body); usage.Deletable || len(usage.Blockers) != 1 || usage.Blockers[0] != "deletion-pending" {
		t.Fatalf("failed deletion did not remain unavailable: %+v", usage)
	}
	status, body = f.call(t, "DELETE", "/v1/images/"+id, testClientToken, "delete-first", nil)
	wantStatus(t, status, 202, body)
	if decodeResponse[Operation](t, body).ID != failed.ID {
		t.Fatal("same idempotency key did not return original failed operation")
	}
	interrupted, _, err := f.service.prepareOperation("client-a", "interrupted-delete", "image-delete", id, nil, func(*State, *Operation) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	// Startup interrupts operations durably while retaining Deleting so a new
	// request can safely retry the digest deletion.
	f.reopen(t)
	status, body = f.call(t, "GET", "/v1/operations/"+interrupted.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if got := decodeResponse[Operation](t, body); got.Status != "failed" {
		t.Fatalf("interrupted deletion operation status: %+v", got)
	}
	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "blocked-create", createRequest{ProfileID: "profile-a", OwnerKey: "blocked", ImportedImageID: id})
	wantStatus(t, status, 409, body)
	status, body = f.call(t, "DELETE", "/v1/images/"+id, testClientToken, "delete-retry", nil)
	wantStatus(t, status, 202, body)
	retry := decodeResponse[Operation](t, body)
	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "blocked-during-delete", createRequest{ProfileID: "profile-a", OwnerKey: "blocked-during-delete", ImportedImageID: id})
	wantStatus(t, status, 409, body)
	close(allowDelete)
	done := f.waitOperation(t, retry.ID, "succeeded")
	if done.Error != nil {
		t.Fatalf("retry after restart failed: %+v", done.Error)
	}
	if err := f.service.store.View(func(st State) error {
		if _, ok := st.ImportedImages[id]; ok {
			t.Fatal("successful retry retained imported image metadata")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
