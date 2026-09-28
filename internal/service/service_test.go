package service

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
)

const testClientToken = "client-a-secret-abcdefghijklmnopqrstuvwxyz"
const otherClientToken = "client-b-secret-abcdefghijklmnopqrstuvwxyz"
const testGuestToken = "local-test-guest-token"

type fakeCoreProvider struct {
	mu           sync.Mutex
	guestURL     string
	created      bool
	createCalls  int
	destroyCalls int
	executionID  string
}

func (*fakeCoreProvider) Name() string { return "docker" }
func (p *fakeCoreProvider) Create(_ context.Context, s boxprovider.Spec) (boxprovider.Handle, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.createCalls++
	p.created = true
	return boxprovider.Handle{Provider: "docker", ID: "fake-" + s.BoxID, Name: "cellbox-" + s.BoxID, ImageID: s.Image}, nil
}
func (p *fakeCoreProvider) Inspect(_ context.Context, h boxprovider.Handle) (boxprovider.Observation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.created {
		return boxprovider.Observation{State: "deleted"}, nil
	}
	return boxprovider.Observation{State: "ready", ExecutionID: p.executionID}, nil
}
func (*fakeCoreProvider) Action(context.Context, boxprovider.Handle, string) error { return nil }
func (p *fakeCoreProvider) Destroy(_ context.Context, _ boxprovider.Handle) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.destroyCalls++
	p.created = false
	return nil
}
func (p *fakeCoreProvider) Guest(_ context.Context, _ boxprovider.Handle) (boxprovider.Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.created {
		return boxprovider.Connection{}, boxprovider.ErrNotFound
	}
	return boxprovider.Connection{URL: p.guestURL, Token: testGuestToken}, nil
}
func (p *fakeCoreProvider) setExecutionID(id string) { p.mu.Lock(); p.executionID = id; p.mu.Unlock() }
func (p *fakeCoreProvider) counts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.createCalls, p.destroyCalls
}

type coreFixture struct {
	service       *Service
	provider      *fakeCoreProvider
	guest         *httptest.Server
	config        Config
	execCalls     atomic.Int32
	activateCalls atomic.Int32
}

func newCoreFixture(t *testing.T) *coreFixture {
	t.Helper()
	f := &coreFixture{}
	f.guest = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testGuestToken {
			http.Error(w, "unauthorized", 401)
			return
		}
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(200)
		case "/v1/exec":
			f.execCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(guestapi.ExecResult{Stdout: "ok", ExitCode: 0})
		case "/v1/activate":
			f.activateCalls.Add(1)
			w.WriteHeader(204)
		default:
			http.NotFound(w, r)
		}
	}))
	f.provider = &fakeCoreProvider{guestURL: f.guest.URL, executionID: "fake-container/start-one"}
	f.config = Config{DataDir: t.TempDir(), StartupTimeoutSeconds: 3,
		Clients:  []Client{{ID: "client-a", Token: testClientToken}, {ID: "client-b", Token: otherClientToken}},
		Profiles: []Profile{{ID: "profile-a", Provider: "docker", Image: "sha256:" + strings.Repeat("a", 64), CPU: 1, MemoryMiB: 512, Guest: guestapi.DefaultConfig(), Clients: []string{"client-a"}}},
	}
	var err error
	f.service, err = New(f.config, map[string]boxprovider.Provider{"docker": f.provider})
	if err != nil {
		f.guest.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if f.service != nil {
			_ = f.service.Close()
		}
		f.guest.Close()
	})
	return f
}
func (f *coreFixture) reopen(t *testing.T) {
	t.Helper()
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	f.service = nil
	var err error
	f.service, err = New(f.config, map[string]boxprovider.Provider{"docker": f.provider})
	if err != nil {
		t.Fatal(err)
	}
}
func (f *coreFixture) call(t *testing.T, method, path, token, key string, body any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(b)
	}
	r := httptest.NewRequest(method, path, reader)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(w, r)
	return w.Code, w.Body.Bytes()
}
func decodeResponse[T any](t *testing.T, b []byte) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(b, &value); err != nil {
		t.Fatalf("decode response %q: %v", b, err)
	}
	return value
}
func wantStatus(t *testing.T, got, want int, body []byte) {
	t.Helper()
	if got != want {
		t.Fatalf("HTTP %d, want %d: %s", got, want, body)
	}
}
func (f *coreFixture) createBox(t *testing.T, key string) (Operation, Box) {
	t.Helper()
	status, body := f.call(t, "POST", "/v1/boxes", testClientToken, key, map[string]string{"profileId": "profile-a", "ownerKey": "owner-a"})
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	f.waitOperation(t, op.ID, "succeeded")
	status, body = f.call(t, "GET", "/v1/boxes/"+op.TargetID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	box := decodeResponse[Box](t, body)
	if box.State != "ready" || box.Generation == 0 {
		t.Fatalf("created box is not ready: %+v", box)
	}
	return op, box
}
func (f *coreFixture) waitOperation(t *testing.T, id, want string) Operation {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, body := f.call(t, "GET", "/v1/operations/"+id, testClientToken, "", nil)
		wantStatus(t, status, 200, body)
		op := decodeResponse[Operation](t, body)
		if op.Status == want {
			return op
		}
		if op.Status == "failed" || op.Status == "succeeded" {
			t.Fatalf("operation ended %s, want %s: %+v", op.Status, want, op)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("operation %s did not become %s", id, want)
	return Operation{}
}

func TestRESTAuthAndClientIsolation(t *testing.T) {
	f := newCoreFixture(t)
	op, box := f.createBox(t, "create-isolation")
	status, body := f.call(t, "GET", "/v1/boxes/"+box.ID, "", "", nil)
	wantStatus(t, status, 401, body)
	status, body = f.call(t, "GET", "/v1/boxes/"+box.ID, otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "GET", "/v1/operations/"+op.ID, otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "POST", "/v1/boxes", otherClientToken, "other-create", map[string]string{"profileId": "profile-a", "ownerKey": "owner-b"})
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "GET", "/v1/boxes", otherClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if boxes := decodeResponse[[]Box](t, body); len(boxes) != 0 {
		t.Fatalf("other client sees boxes: %+v", boxes)
	}
}

func TestRESTIdempotencyPersistsAcrossReopen(t *testing.T) {
	f := newCoreFixture(t)
	createOp, box := f.createBox(t, "create-persist")
	status, body := f.call(t, "POST", "/v1/boxes", testClientToken, "create-persist", map[string]string{"profileId": "profile-a", "ownerKey": "owner-a"})
	wantStatus(t, status, 202, body)
	if op := decodeResponse[Operation](t, body); op.ID != createOp.ID {
		t.Fatalf("create replay returned %s, want %s", op.ID, createOp.ID)
	}
	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "create-persist", map[string]string{"profileId": "profile-a", "ownerKey": "changed"})
	wantStatus(t, status, 409, body)
	input := map[string]any{"argv": []string{"echo", "ok"}, "expectedGeneration": box.Generation, "timeoutMs": 30000}
	status, body = f.call(t, "POST", "/v1/boxes/"+box.ID+"/execs", testClientToken, "exec-persist", input)
	wantStatus(t, status, 202, body)
	execOp := decodeResponse[Operation](t, body)
	completed := f.waitOperation(t, execOp.ID, "succeeded")
	if f.execCalls.Load() != 1 {
		t.Fatalf("guest exec calls = %d, want 1", f.execCalls.Load())
	}
	f.reopen(t)
	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "create-persist", map[string]string{"profileId": "profile-a", "ownerKey": "owner-a"})
	wantStatus(t, status, 202, body)
	if op := decodeResponse[Operation](t, body); op.ID != createOp.ID {
		t.Fatalf("reopened create replay returned %s", op.ID)
	}
	status, body = f.call(t, "POST", "/v1/boxes/"+box.ID+"/execs", testClientToken, "exec-persist", input)
	wantStatus(t, status, 202, body)
	if op := decodeResponse[Operation](t, body); op.ID != completed.ID || op.Status != "succeeded" {
		t.Fatalf("reopened exec replay: %+v", op)
	}
	status, body = f.call(t, "POST", "/v1/boxes/"+box.ID+"/execs", testClientToken, "exec-persist", map[string]any{"argv": []string{"echo", "changed"}, "expectedGeneration": box.Generation, "timeoutMs": 30000})
	wantStatus(t, status, 409, body)
	if creates, _ := f.provider.counts(); creates != 1 {
		t.Fatalf("provider create calls = %d, want 1", creates)
	}
	if f.execCalls.Load() != 1 {
		t.Fatalf("guest exec replayed %d times", f.execCalls.Load())
	}
}

func TestRESTRejectsStaleGenerationBeforeGuestExec(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "create-generation")
	f.provider.setExecutionID("fake-container/start-two")
	status, body := f.call(t, "GET", "/v1/boxes/"+box.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	latest := decodeResponse[Box](t, body)
	if latest.Generation <= box.Generation {
		t.Fatalf("generation did not advance: %d -> %d", box.Generation, latest.Generation)
	}
	status, body = f.call(t, "POST", "/v1/boxes/"+box.ID+"/execs", testClientToken, "stale-exec", map[string]any{"argv": []string{"echo", "unsafe"}, "expectedGeneration": box.Generation})
	wantStatus(t, status, 409, body)
	if f.execCalls.Load() != 0 {
		t.Fatalf("stale request reached guest %d times", f.execCalls.Load())
	}
}

func TestRESTLeaseBlocksDestroy(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "create-lease")
	status, body := f.call(t, "POST", "/v1/boxes/"+box.ID+"/leases", testClientToken, "", map[string]any{"purpose": "active work", "ttlSeconds": 60})
	wantStatus(t, status, 201, body)
	lease := decodeResponse[Lease](t, body)
	status, body = f.call(t, "POST", "/v1/boxes/"+box.ID+":destroy", testClientToken, "destroy-with-lease", nil)
	wantStatus(t, status, 409, body)
	if _, destroys := f.provider.counts(); destroys != 0 {
		t.Fatalf("destroy called with active lease: %d", destroys)
	}
	status, body = f.call(t, "DELETE", "/v1/leases/"+lease.ID, testClientToken, "", nil)
	wantStatus(t, status, 204, body)
	status, body = f.call(t, "POST", "/v1/boxes/"+box.ID+":destroy", testClientToken, "destroy-after-lease", nil)
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	f.waitOperation(t, op.ID, "succeeded")
	if _, destroys := f.provider.counts(); destroys != 1 {
		t.Fatalf("destroy calls = %d, want 1", destroys)
	}
}

func TestInterruptedExecIsUnknownAndNeverReplayed(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "create-interrupted")
	input := execInput{ExecRequest: guestapi.ExecRequest{Argv: []string{"echo", "once"}, TimeoutMS: 30000}, ExpectedGeneration: box.Generation}
	var eid string
	op, fresh, err := f.service.prepareOperation("client-a", "interrupted-exec", "exec", box.ID, input, func(st *State, op *Operation) error {
		eid = randomID("exec-")
		st.Executions[eid] = Execution{ID: eid, BoxID: box.ID, OperationID: op.ID, State: "running"}
		b := st.Boxes[box.ID]
		b.Box.OperationID = op.ID
		st.Boxes[box.ID] = b
		return nil
	})
	if err != nil || !fresh {
		t.Fatalf("seed operation: %+v %v", op, err)
	}
	f.reopen(t)
	status, body := f.call(t, "GET", "/v1/operations/"+op.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	after := decodeResponse[Operation](t, body)
	if after.Status != "failed" || after.Error == nil || after.Error.Code != "OPERATION_INTERRUPTED" {
		t.Fatalf("interrupted operation: %+v", after)
	}
	status, body = f.call(t, "GET", "/v1/execs/"+eid, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	execution := decodeResponse[Execution](t, body)
	if execution.State != "unknown" {
		t.Fatalf("interrupted execution: %+v", execution)
	}
	status, body = f.call(t, "POST", "/v1/boxes/"+box.ID+"/execs", testClientToken, "interrupted-exec", map[string]any{"argv": []string{"echo", "once"}, "timeoutMs": 30000, "expectedGeneration": box.Generation})
	wantStatus(t, status, 202, body)
	if replay := decodeResponse[Operation](t, body); replay.ID != op.ID || replay.Status != "failed" {
		t.Fatalf("interrupted replay: %+v", replay)
	}
	if f.execCalls.Load() != 0 {
		t.Fatalf("interrupted exec replayed %d times", f.execCalls.Load())
	}
}

func TestFailedRestoreCandidateCannotActivate(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "create-staged")
	if err := f.service.store.Update(func(st *State) error {
		b := st.Boxes[box.ID]
		b.Staged = true
		b.Box.State = "failed"
		b.Box.Error = &APIError{Code: "RESTORE_FAILED", Message: "Archive checksum mismatch"}
		st.Boxes[box.ID] = b
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	status, body := f.call(t, "POST", "/v1/boxes/"+box.ID+":activate", testClientToken, "activate-failed-restore", nil)
	wantStatus(t, status, 409, body)
	if f.activateCalls.Load() != 0 {
		t.Fatalf("failed restore activated %d times", f.activateCalls.Load())
	}
}

func TestStoreLockPreventsSecondServiceUntilClose(t *testing.T) {
	f := newCoreFixture(t)
	if other, err := New(f.config, map[string]boxprovider.Provider{"docker": f.provider}); err == nil {
		_ = other.Close()
		t.Fatal("second service opened locked store")
	}
	f.reopen(t)
	status, body := f.call(t, "GET", "/v1/profiles", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if !bytes.Contains(body, []byte("profile-a")) {
		t.Fatalf("reopened service lost profile: %s", body)
	}
}

var _ boxprovider.Provider = (*fakeCoreProvider)(nil)
