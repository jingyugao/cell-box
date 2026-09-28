package docker

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/image"
	"cellbox.local/cellbox/internal/guestapi"
)

const testImageID = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const testToken = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type fakeDocker struct {
	calls     [][]string
	exists    bool
	labels    map[string]string
	state     string
	startedAt string
	ports     map[string][]struct {
		HostIP   string `json:"HostIp"`
		HostPort string `json:"HostPort"`
	}
	imageLabels map[string]string
}

func (f *fakeDocker) Run(_ context.Context, args []string) ([]byte, error) {
	f.calls = append(f.calls, append([]string(nil), args...))
	switch strings.Join(args[:min(2, len(args))], " ") {
	case "image inspect":
		return json.Marshal([]any{map[string]any{"Id": testImageID, "Config": map[string]any{"Labels": f.imageLabels}}})
	case "container inspect":
		if !f.exists {
			return nil, errors.New("No such container")
		}
		return json.Marshal([]any{map[string]any{"Id": "container-id", "Name": "/cellbox-box1", "Image": testImageID, "Config": map[string]any{"Labels": f.labels}, "State": map[string]any{"Status": f.state, "Paused": f.state == "paused", "StartedAt": f.startedAt}, "NetworkSettings": map[string]any{"Ports": f.ports}}})
	}
	switch args[0] {
	case "create":
		f.exists = true
		f.state = "created"
		f.labels = map[string]string{}
		for i := 0; i < len(args)-1; i++ {
			if args[i] == "--label" {
				parts := strings.SplitN(args[i+1], "=", 2)
				f.labels[parts[0]] = parts[1]
			}
		}
		return []byte("container-id"), nil
	case "start":
		f.state = "running"
		f.startedAt = "2026-09-26T01:02:03.123456789Z"
		return nil, nil
	case "pause":
		f.state = "paused"
		return nil, nil
	case "unpause":
		f.state = "running"
		return nil, nil
	case "rm":
		f.exists = false
		return nil, nil
	case "exec":
		return []byte(testToken + "\n"), nil
	}
	return nil, errors.New("unexpected command")
}
func newFake() *fakeDocker {
	return &fakeDocker{imageLabels: map[string]string{image.ManagedLabel: image.ImageVersion, image.IdentityLabel: "key"}, ports: map[string][]struct {
		HostIP   string `json:"HostIp"`
		HostPort string `json:"HostPort"`
	}{"40000/tcp": {{HostIP: "127.0.0.1", HostPort: "49152"}}}}
}
func spec() boxprovider.Spec {
	return boxprovider.Spec{BoxID: "box1", Image: "cellbox-prepared:key", CPU: 1.5, MemoryMiB: 512, Config: guestapi.DefaultConfig(), Staged: true}
}

func TestCreateLifecycle(t *testing.T) {
	f := newFake()
	p := NewWithRunner(f)
	ctx := context.Background()
	h, err := p.Create(ctx, spec())
	if err != nil {
		t.Fatal(err)
	}
	if h.ID != "container-id" || h.ImageID != testImageID {
		t.Fatalf("unexpected handle: %+v", h)
	}
	var create []string
	for _, call := range f.calls {
		if call[0] == "create" {
			create = call
		}
	}
	for _, want := range []string{"127.0.0.1::40000", guestapi.Binary, "--staged", "--cap-drop", "ALL", "CHOWN", "FOWNER", "DAC_OVERRIDE", "SETUID", "SETGID", "no-new-privileges", "0:0"} {
		if !contains(create, want) {
			t.Errorf("create missing %q: %v", want, create)
		}
	}
	if _, err = p.Create(ctx, spec()); err != nil {
		t.Fatalf("retry: %v", err)
	}
	count := 0
	for _, c := range f.calls {
		if c[0] == "create" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("created %d times", count)
	}
	obs, err := p.Inspect(ctx, h)
	if err != nil || obs.State != "ready" || obs.ExecutionID != h.ID+"/2026-09-26T01:02:03.123456789Z" {
		t.Fatalf("inspect: %+v %v", obs, err)
	}
	conn, err := p.Guest(ctx, h)
	if err != nil || conn.URL != "http://127.0.0.1:49152" || conn.Token != testToken {
		t.Fatalf("guest: %+v %v", conn, err)
	}
	if err = p.Action(ctx, h, "freeze"); err != nil {
		t.Fatal(err)
	}
	obs, _ = p.Inspect(ctx, h)
	if obs.State != "frozen" {
		t.Fatal(obs)
	}
	if err = p.Action(ctx, h, "unfreeze"); err != nil {
		t.Fatal(err)
	}
	if err = p.Action(ctx, h, "suspend"); !errors.Is(err, boxprovider.ErrUnsupported) {
		t.Fatal(err)
	}
	if err = p.Destroy(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err = p.Destroy(ctx, h); err != nil {
		t.Fatal(err)
	}
	obs, err = p.Inspect(ctx, h)
	if err != nil || obs.State != "deleted" {
		t.Fatalf("deleted: %+v %v", obs, err)
	}
}
func TestExecutionIdentityChangesOnContainerRestart(t *testing.T) {
	f := newFake()
	p := NewWithRunner(f)
	ctx := context.Background()
	h, err := p.Create(ctx, spec())
	if err != nil {
		t.Fatal(err)
	}
	first, err := p.Inspect(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	f.state = "exited"
	exited, err := p.Inspect(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if exited.ExecutionID != first.ExecutionID {
		t.Fatal("exit changed execution identity")
	}
	f.state = "running"
	f.startedAt = "2026-09-26T04:05:06Z"
	second, err := p.Inspect(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if second.ExecutionID == first.ExecutionID || second.ExecutionID == "" || second.State != "ready" {
		t.Fatalf("restart did not change execution identity: first=%+v second=%+v", first, second)
	}
	f.state = "paused"
	frozen, err := p.Inspect(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if frozen.ExecutionID != second.ExecutionID {
		t.Fatal("pause changed execution identity")
	}
}
func TestCanceledContextSkipsCommands(t *testing.T) {
	f := newFake()
	p := NewWithRunner(f)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Create(ctx, spec()); !errors.Is(err, context.Canceled) {
		t.Fatalf("create: %v", err)
	}
	if _, err := p.Inspect(ctx, boxprovider.Handle{Provider: "docker", Name: "cellbox-box1", ID: "container-id"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("inspect: %v", err)
	}
	if len(f.calls) != 0 {
		t.Fatalf("canceled context ran %d Docker commands", len(f.calls))
	}
}
func TestOwnershipAndSpecCollision(t *testing.T) {
	f := newFake()
	p := NewWithRunner(f)
	ctx := context.Background()
	h, err := p.Create(ctx, spec())
	if err != nil {
		t.Fatal(err)
	}
	s := spec()
	s.MemoryMiB = 1024
	if _, err = p.Create(ctx, s); err == nil {
		t.Fatal("accepted incompatible spec")
	}
	f.labels[boxLabel] = "another"
	if err = p.Destroy(ctx, h); err == nil {
		t.Fatal("destroyed unrelated container")
	}
	f.labels[boxLabel] = "box1"
	h.ID = "wrong"
	if _, err = p.Guest(ctx, h); err == nil {
		t.Fatal("accepted stale identity")
	}
}
func TestRejectUnsafeInputs(t *testing.T) {
	for _, id := range []string{"-bad", "Bad", "a/b", ""} {
		s := spec()
		s.BoxID = id
		if err := validateSpec(s); err == nil {
			t.Errorf("accepted ID %q", id)
		}
	}
	f := newFake()
	p := NewWithRunner(f)
	s := spec()
	s.CPU = 0
	if _, err := p.Create(context.Background(), s); err == nil {
		t.Fatal("accepted zero CPU")
	}
	s.CPU = math.NaN()
	if _, err := p.Create(context.Background(), s); err == nil {
		t.Fatal("accepted NaN CPU")
	}
	f.ports["40000/tcp"][0].HostIP = "0.0.0.0"
	h, err := p.Create(context.Background(), spec())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Guest(context.Background(), h); err == nil {
		t.Fatal("accepted wildcard binding")
	}
}
func contains(xs []string, x string) bool {
	for _, v := range xs {
		if reflect.DeepEqual(v, x) {
			return true
		}
	}
	return false
}
