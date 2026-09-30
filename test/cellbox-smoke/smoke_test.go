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
	"cellbox.local/cellbox/internal/image"
	"cellbox.local/cellbox/internal/providers/docker"
	"cellbox.local/cellbox/internal/service"
	"cellbox.local/cellbox/internal/guestapi"
)

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
	dockerfile := "FROM scratch\nCOPY workload /workload\nCOPY --chmod=0755 workload /opt/cellbox/tools/demo\n"
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
	cfg.Tools = []guestapi.Tool{{ID: "demo", Executable: "/opt/cellbox/tools/demo", Args: []string{"tool"}, CredentialEnv: map[string]string{"DEMO_FILE": "demo"}}}
	const clientToken = "smoke-client-abcdefghijklmnopqrstuvwxyz-0123456789"
	s, err := service.New(service.Config{DataDir: filepath.Join(dir, "state"), PublicURL: "http://127.0.0.1", StartupTimeoutSeconds: 20, Clients: []service.Client{{ID: "smoke", Token: clientToken}}, Profiles: []service.Profile{{ID: "smoke", Provider: "docker", Image: prepared.ImageID, Guest: cfg, Clients: []string{"smoke"}}}}, map[string]boxprovider.Provider{"docker": docker.New("docker")})
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
		req.Header.Set("Authorization", "Bearer "+clientToken)
		req.Header.Set("Idempotency-Key", fmt.Sprint(time.Now().UnixNano()))
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
	createRaw := call("POST", "/v1/boxes", map[string]string{"profileId": "smoke", "ownerKey": "source"}, 202)
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
	if b.State != "ready" || b.Generation != 1 {
		t.Fatalf("unexpected box %+v", b)
	}
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
	call("PUT", "/v1/boxes/"+source+"/credentials/demo", "smoke-secret", 204)
	execCheck([]string{"/workload", "secret-check"}, "secret-denied")
	execCheck([]string{guestapi.Binary, "tool", "demo"}, "uid=11001 credential-ok")
	call("PUT", "/v1/boxes/"+source+"/files?path=hello.txt", "archive-roundtrip", 204)
	var route service.Route
	json.Unmarshal(call("POST", "/v1/routes", map[string]any{"boxId": source, "port": 8080}, 201), &route)
	var grant struct {
		Grant service.Grant `json:"grant"`
		Token string        `json:"token"`
	}
	json.Unmarshal(call("POST", "/v1/routes/"+route.ID+"/grants", map[string]any{"subject": "smoke", "ttlSeconds": 30}, 201), &grant)
	req, _ := http.NewRequest("GET", server.URL+"/s/"+route.ID+"/", nil)
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 401 {
		t.Fatal("unauthorized proxy allowed")
	}
	req.Header.Set("Authorization", "Bearer "+grant.Token)
	res, err = server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || string(data) != "loopback-ok" {
		t.Fatalf("proxy failed %d: %s", res.StatusCode, data)
	}
	wait(call("POST", "/v1/boxes/"+source+":freeze", nil, 202))
	if getBox(source).State != "frozen" {
		t.Fatal("not frozen")
	}
	wait(call("POST", "/v1/boxes/"+source+":unfreeze", nil, 202))
	archive := wait(call("POST", "/v1/boxes/"+source+"/archives", nil, 202)).Result["archiveId"]
	restoreRaw := call("POST", "/v1/boxes:restore", map[string]string{"profileId": "smoke", "ownerKey": "candidate", "archiveId": archive}, 202)
	var restoring service.Operation
	json.Unmarshal(restoreRaw, &restoring)
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", "cellbox-"+restoring.TargetID).Run() })
	candidate := wait(restoreRaw).TargetID
	if getBox(candidate).State != "staged" {
		t.Fatal("restore activated too early")
	}
	if string(call("GET", "/v1/boxes/"+candidate+"/files?path=hello.txt", nil, 200)) != "archive-roundtrip" {
		t.Fatal("archive content differs")
	}
	wait(call("POST", "/v1/boxes/"+candidate+":activate", nil, 202))
	wait(call("POST", "/v1/boxes/"+source+":destroy", nil, 202))
	call("GET", "/v1/archives/"+archive, nil, 200)
	wait(call("POST", "/v1/boxes/"+candidate+":destroy", nil, 202))
	call("DELETE", "/v1/archives/"+archive, nil, 204)
	t.Log("agent/debug isolation, local tool, files, authenticated loopback forwarding, freeze, archive restore and cleanup passed")
}
