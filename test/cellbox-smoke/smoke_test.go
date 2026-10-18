package smoke

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/catalog"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/image"
	"cellbox.local/cellbox/internal/providers/docker"
	"cellbox.local/cellbox/internal/service"
)

type localImageBuilder struct {
	image      string
	command    []string
	env        map[string]string
	workingDir string
}

func (b localImageBuilder) Build(_ context.Context, in catalog.BuildInput, log io.Writer, stage func(string) error) (catalog.BuildResult, error) {
	if err := stage("resolving"); err != nil {
		return catalog.BuildResult{}, err
	}
	if _, err := fmt.Fprintf(log, "Using local Docker image %s\n", b.image); err != nil {
		return catalog.BuildResult{}, err
	}
	if err := stage("building"); err != nil {
		return catalog.BuildResult{}, err
	}
	return catalog.BuildResult{
		ResolvedBaseImage: in.BaseImage,
		Image:             b.image,
		Command:           append([]string(nil), b.command...),
		Env:               b.env,
		WorkingDir:        b.workingDir,
	}, nil
}

func TestDockerREST(t *testing.T) {
	if os.Getenv("CELLBOX_DOCKER_SMOKE") != "1" {
		t.Skip("set CELLBOX_DOCKER_SMOKE=1 for the local Docker integration test")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	run := func(argv ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s failed: %v: %s", argv[0], err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("go", "build", "-o", filepath.Join(dir, "guest"), "./cmd/cellbox-container-agent")
	run("go", "build", "-o", filepath.Join(dir, "workload"), "./test/smoke-workload")
	dockerfile := "FROM scratch\nCOPY workload /workload\n"
	if err = os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(dockerfile), 0600); err != nil {
		t.Fatal(err)
	}
	tag := fmt.Sprintf("cellbox-smoke:%d", time.Now().UnixNano())
	run("docker", "build", "--quiet", "--label", tag, "--tag", tag, dir)
	t.Cleanup(func() { _ = exec.Command("docker", "image", "rm", tag).Run() })
	prepared, err := image.Prepare(context.Background(), image.Options{Base: tag, GuestBinary: filepath.Join(dir, "guest")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "image", "rm", prepared.Tag).Run() })
	cfg := guestapi.DefaultConfig()
	cfg.Command = []string{"/workload", "serve"}
	s, err := service.New(service.Config{
		DataDir: filepath.Join(dir, "state"), StartupTimeoutSeconds: 20,
		ClientID: "smoke", CatalogBuilder: localImageBuilder{image: prepared.ImageID, command: cfg.Command, env: cfg.Env, workingDir: cfg.Workspace},
		Profiles: []service.Profile{{ID: "smoke", Provider: "docker", Image: prepared.ImageID, AutoTrustImportedImages: true, Guest: cfg}},
	}, map[string]boxprovider.Provider{"docker": docker.New("docker")})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	call := func(method, path string, body any, want int) []byte {
		t.Helper()
		var data []byte
		switch v := body.(type) {
		case nil:
		case string:
			data = []byte(v)
		default:
			data, err = json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
		}
		req, _ := http.NewRequest(method, server.URL+path, bytes.NewReader(data))
		res, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		out, _ := io.ReadAll(res.Body)
		if res.StatusCode != want {
			t.Fatalf("%s %s: %d want %d: %s", method, path, res.StatusCode, want, out)
		}
		return out
	}
	wait := func(raw []byte) service.Operation {
		t.Helper()
		var op service.Operation
		if err := json.Unmarshal(raw, &op); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for op.Status == "running" || op.Status == "queued" {
			if time.Now().After(deadline) {
				t.Fatal("operation timeout")
			}
			time.Sleep(50 * time.Millisecond)
			json.Unmarshal(call("GET", "/v1/operations/"+op.ID, nil, 200), &op)
		}
		if op.Status != "succeeded" {
			t.Fatalf("%s failed: %+v", op.Kind, op.Error)
		}
		return op
	}
	var template catalog.Template
	if err := json.Unmarshal(call("POST", "/v1/templates", map[string]any{"name": "smoke", "description": "local Docker smoke"}, 201), &template); err != nil {
		t.Fatal(err)
	}
	baseImage := "registry.example.test/smoke-base@sha256:" + strings.Repeat("a", 64)
	buildRaw := call("POST", "/v1/templates/"+template.ID+"/revisions", map[string]any{
		"userVersion": "local", "baseImage": baseImage, "profileId": "smoke",
	}, 202)
	var buildOp catalog.Operation
	if err := json.Unmarshal(buildRaw, &buildOp); err != nil {
		t.Fatal(err)
	}
	buildDeadline := time.Now().Add(30 * time.Second)
	for buildOp.Status == "running" || buildOp.Status == "queued" {
		if time.Now().After(buildDeadline) {
			t.Fatal("template build operation timeout")
		}
		time.Sleep(50 * time.Millisecond)
		if err := json.Unmarshal(call("GET", "/v1/operations/"+buildOp.ID, nil, 200), &buildOp); err != nil {
			t.Fatal(err)
		}
	}
	if buildOp.Status != "succeeded" {
		t.Fatalf("template build failed: %+v", buildOp.Error)
	}
	revisionID := buildOp.Result["revisionId"]
	if revisionID == "" {
		t.Fatalf("build operation omitted revisionId: %+v", buildOp)
	}
	createRaw := call("POST", "/v1/boxes", map[string]string{"revisionId": revisionID, "ownerKey": "source"}, 202)
	var creating service.Operation
	json.Unmarshal(createRaw, &creating)
	t.Cleanup(func() {
		if t.Failed() {
			out, _ := exec.Command("docker", "logs", "cellbox-"+creating.TargetID).CombinedOutput()
			t.Logf("source guest: %s", out)
		}
		_ = exec.Command("docker", "rm", "-f", "cellbox-"+creating.TargetID).Run()
	})
	source := wait(createRaw).TargetID
	getBox := func(id string) service.Box {
		var b service.Box
		json.Unmarshal(call("GET", "/v1/boxes/"+id, nil, 200), &b)
		return b
	}
	b := getBox(source)
	if b.Phase != "running" || b.Generation != 1 || b.TemplateID != template.ID || b.RevisionID != revisionID {
		t.Fatalf("unexpected box %+v", b)
	}
	expectUnsupported := func(method, path string, body any) {
		t.Helper()
		var envelope struct {
			Error service.APIError `json:"error"`
		}
		if err := json.Unmarshal(call(method, path, body, http.StatusUnprocessableEntity), &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.Error.Code != "UNSUPPORTED_CAPABILITY" {
			t.Fatalf("Docker operation %s %s returned %q", method, path, envelope.Error.Code)
		}
	}
	expectUnsupported("POST", "/v1/boxes", map[string]any{
		"revisionId": revisionID, "ownerKey": "unsupported-docker-staged", "staged": true,
	})
	for _, path := range []string{
		"/v1/boxes/" + source + ":suspend",
		"/v1/boxes/" + source + ":resume",
		"/v1/boxes/" + source + ":activate",
	} {
		expectUnsupported("POST", path, map[string]any{"expectedGeneration": b.Generation})
	}
	expectUnsupported("POST", "/v1/boxes:restore", map[string]any{
		"revisionId": revisionID, "ownerKey": "unsupported-docker-restore", "archiveId": "unused",
	})
	expectUnsupported("POST", "/v1/boxes/"+source+"/archives", nil)
	expectUnsupported("POST", "/v1/boxes/"+source+"/tools/protected", map[string]any{
		"args": []string{}, "timeoutMs": 1000,
	})
	expectUnsupported("PUT", "/v1/boxes/"+source+"/credentials/api-token", "valid-credential")
	expectUnsupported("PUT", "/v1/boxes/"+source+"/credentials", map[string]any{
		"expectedGeneration": b.Generation,
		"slots":              map[string]string{"API_TOKEN": "dmFsaWQtY3JlZGVudGlhbA=="},
	})
	execCheck := func(argv []string, want string) {
		t.Helper()
		op := wait(call("POST", "/v1/boxes/"+source+"/execs", map[string]any{"argv": argv, "expectedGeneration": b.Generation}, 202))
		var e service.Execution
		json.Unmarshal(call("GET", "/v1/execs/"+op.Result["execId"], nil, 200), &e)
		if e.Result == nil || e.Result.ExitCode != 0 || !strings.Contains(e.Result.Stdout, want) {
			t.Fatalf("unexpected execution result %+v", e)
		}
	}
	execCheck([]string{"/workload", "identity"}, "uid=11000 gid=11000")
	call("PUT", "/v1/boxes/"+source+"/files?path=hello.txt", "archive-roundtrip", 204)
	req, _ := http.NewRequest("GET", server.URL+"/v1/boxes/"+source+"/services/8080/", nil)
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || string(data) != "loopback-ok" {
		t.Fatalf("proxy failed %d: %s", res.StatusCode, data)
	}
	wait(call("POST", "/v1/boxes/"+source+":destroy", nil, 202))
	t.Log("Docker basic create, exec, files, direct service proxy and destroy passed; template revision retained for catalog lifetime")
}
