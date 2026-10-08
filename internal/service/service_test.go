package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func TestProfilePreservesExplicitRootDebugAndDefaultsOmittedIdentity(t *testing.T) {
	f := newCoreFixture(t)
	c := f.config
	p := &c.Profiles[0]
	if err := json.Unmarshal([]byte(`{"workspace":"/workspace","debug":{"uid":0,"gid":0}}`), &p.Guest); err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if p.Guest.Debug != (guestapi.Identity{}) || !profileCapabilities(*p).RootDebug {
		t.Fatal("root debug was silently replaced")
	}
	p.Guest.Debug.GID = 1
	if err := c.Validate(); err == nil {
		t.Fatal("partially root debug identity accepted")
	}
	if err := json.Unmarshal([]byte(`{"workspace":"/workspace"}`), &p.Guest); err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if p.Guest.Debug.UID == 0 || profileCapabilities(*p).RootDebug {
		t.Fatal("omitted debug identity became root")
	}
}

func TestPersistentHomeIsFixedForNewBoxesAndPreservesLegacyLayout(t *testing.T) {
	c := Config{Profiles: []Profile{{ID: "home", Provider: "resumable-k8s-pod", Image: "registry.example.invalid/image@sha256:" + strings.Repeat("a", 64), NodeName: "node-a", Namespace: "boxes"}}}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	profile := c.Profiles[0]
	if !profileCapabilities(profile).PersistentHome {
		t.Fatal("persistentHome capability was not advertised")
	}
	if !runtimeSpec(boxRecord{Box: Box{ID: "box-1"}, Profile: profile}).PersistentHome {
		t.Fatal("persistentHome was not forwarded to the provider")
	}
	if profile.Guest.Workspace != "/home/agent/workspace" {
		t.Fatal("new resumable profile did not default workspace into HOME")
	}
	c.Profiles[0].PersistentHome = false
	c.Profiles[0].Guest.Workspace = "/workspace"
	if err := c.Validate(); err != nil || !c.Profiles[0].PersistentHome {
		t.Fatalf("persistent HOME could be disabled: %v", err)
	}
	var legacy Profile
	if err := json.Unmarshal([]byte(`{"provider":"resumable-k8s-pod","guest":{"workspace":"/home/agent/workspace"}}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if runtimeSpec(boxRecord{Profile: legacy}).PersistentHome || profileCapabilities(legacy).PersistentHome {
		t.Fatal("legacy stored profile was silently switched to a different filesystem")
	}
}

type fakeCoreProvider struct {
	mu           sync.Mutex
	guestURL     string
	created      bool
	createCalls  int
	destroyCalls int
	executionID  string
	guestCalls   int
	inspectErr   error
	cacheImage   func(context.Context, string, string, string) error
}

func (p *fakeCoreProvider) CacheImage(ctx context.Context, image, node, namespace string) error {
	if p.cacheImage != nil {
		return p.cacheImage(ctx, image, node, namespace)
	}
	return nil
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
	if p.inspectErr != nil {
		return boxprovider.Observation{}, p.inspectErr
	}
	if !p.created {
		return boxprovider.Observation{Phase: "deleted"}, nil
	}
	return boxprovider.Observation{Phase: "running", ExecutionID: p.executionID}, nil
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
	p.guestCalls++
	if !p.created {
		return boxprovider.Connection{}, boxprovider.ErrNotFound
	}
	return boxprovider.Connection{URL: p.guestURL}, nil
}
func (p *fakeCoreProvider) guestCount() int          { p.mu.Lock(); defer p.mu.Unlock(); return p.guestCalls }
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
		if r.Header.Get("Authorization") != "" {
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
		ClientID: "client-a",
		Profiles: []Profile{{ID: "profile-a", Provider: "docker", Image: "sha256:" + strings.Repeat("a", 64), CPU: 1, MemoryMiB: 512, Guest: guestapi.DefaultConfig()}},
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
		if token == otherClientToken {
			r.Header.Set("X-Cellbox-Client-ID", "client-b")
		}
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
	if box.Phase != "running" || box.Generation == 0 {
		t.Fatalf("created box is not ready: %+v", box)
	}
	return op, box
}

func TestStagedCreateHoldsWorkloadUntilActivation(t *testing.T) {
	f := newCoreFixture(t)
	input := map[string]any{"profileId": "profile-a", "ownerKey": "staged-owner", "staged": true}
	status, body := f.call(t, "POST", "/v1/boxes", testClientToken, "staged-create", input)
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	f.waitOperation(t, op.ID, "succeeded")
	status, body = f.call(t, "GET", "/v1/boxes/"+op.TargetID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	box := decodeResponse[Box](t, body)
	if box.Phase != "staged" || f.activateCalls.Load() != 0 {
		t.Fatalf("candidate started too soon: %+v", box)
	}
	record, err := f.service.rawBox(box.ID)
	if err != nil || !runtimeSpec(record).Staged || !record.RestoreComplete {
		t.Fatalf("invalid staged record: %+v, %v", record, err)
	}
	status, body = f.call(t, "POST", "/v1/boxes/"+box.ID+":activate", testClientToken, "staged-activate", nil)
	wantStatus(t, status, 202, body)
	f.waitOperation(t, decodeResponse[Operation](t, body).ID, "succeeded")
	status, body = f.call(t, "GET", "/v1/boxes/"+box.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if decodeResponse[Box](t, body).Phase != "running" || f.activateCalls.Load() != 1 {
		t.Fatal("activation did not start workload exactly once")
	}
}

func TestBoxQueriesObserveLifecycleWithoutGuestAndFilterOwnedIDs(t *testing.T) {
	f := newCoreFixture(t)
	_, first := f.createBox(t, "query-first")
	_, second := f.createBox(t, "query-second")
	guestCalls := f.provider.guestCount()
	f.guest.Close()
	status, body := f.call(t, "GET", "/v1/boxes?id="+first.ID+"&id="+second.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	boxes := decodeResponse[[]Box](t, body)
	if len(boxes) != 2 || boxes[0].Phase != "running" || boxes[1].Phase != "running" {
		t.Fatalf("filtered lifecycle query: %+v", boxes)
	}
	status, body = f.call(t, "GET", "/v1/boxes/"+first.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if got := f.provider.guestCount(); got != guestCalls {
		t.Fatalf("box queries called Guest %d times; want no additional calls", got-guestCalls)
	}
	status, body = f.call(t, "GET", "/v1/boxes?id="+first.ID, otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
}

func TestBoxQueryProviderFailureDoesNotChangeSavedLifecycle(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "query-inspect-error")
	before, err := f.service.rawBox(box.ID)
	if err != nil {
		t.Fatal(err)
	}
	f.provider.mu.Lock()
	f.provider.inspectErr = errors.New("provider unavailable")
	f.provider.mu.Unlock()
	status, body := f.call(t, "GET", "/v1/boxes/"+box.ID, testClientToken, "", nil)
	wantStatus(t, status, 502, body)
	after, err := f.service.rawBox(box.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Box.Phase != before.Box.Phase || after.Box.Version != before.Box.Version {
		t.Fatalf("failed observation changed lifecycle: before=%+v after=%+v", before.Box, after.Box)
	}
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

func TestRESTUnauthenticatedClientNamespaces(t *testing.T) {
	f := newCoreFixture(t)
	op, box := f.createBox(t, "create-isolation")
	status, body := f.call(t, "GET", "/v1/boxes/"+box.ID, "", "", nil)
	wantStatus(t, status, 200, body)
	status, body = f.call(t, "GET", "/v1/boxes/"+box.ID, otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "GET", "/v1/operations/"+op.ID, otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "GET", "/v1/boxes", otherClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if boxes := decodeResponse[[]Box](t, body); len(boxes) != 0 {
		t.Fatalf("other client sees boxes: %+v", boxes)
	}
}

func TestRESTWithoutClientCredentialsOrProfileAdmission(t *testing.T) {
	f := newCoreFixture(t)
	f.config.ClientID = ""
	f.reopen(t)
	status, body := f.call(t, "GET", "/v1/profiles", "", "", nil)
	wantStatus(t, status, 200, body)
	status, body = f.call(t, "POST", "/v1/boxes", "", "anonymous-create", map[string]string{"profileId": "profile-a", "ownerKey": "owner"})
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	f.waitOperation(t, op.ID, "succeeded")
	status, body = f.call(t, "GET", "/v1/boxes/"+op.TargetID, "ignored-invalid-token", "", nil)
	wantStatus(t, status, 200, body)
	if f.service.config.ClientID != "internal" {
		t.Fatal("unexpected default data namespace")
	}
	status, body = f.call(t, "POST", "/v1/boxes", "", "anonymous-create", map[string]string{"profileId": "profile-a", "ownerKey": "owner"})
	wantStatus(t, status, 202, body)
	if decodeResponse[Operation](t, body).ID != op.ID {
		t.Fatal("anonymous retries lost their idempotency namespace")
	}
}

func TestReadinessRetriesAStalledEndpointWithinStartupDeadline(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "create-probe-retry")
	var attempts atomic.Int32
	stalledCanceled := make(chan struct{})
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			<-r.Context().Done()
			close(stalledCanceled)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer endpoint.Close()
	f.provider.mu.Lock()
	f.provider.guestURL = endpoint.URL
	f.provider.mu.Unlock()
	if _, err := f.service.waitState(context.Background(), box.ID, "running", true); err != nil {
		t.Fatalf("transient endpoint stall exhausted startup deadline: %v", err)
	}
	if attempts.Load() < 2 {
		t.Fatal("stalled readiness request was not retried")
	}
	select {
	case <-stalledCanceled:
	case <-time.After(time.Second):
		t.Fatal("stalled readiness request was not canceled")
	}
}

func TestResumeCompletesWithUnhealthyGuestAndCancelsBackgroundProbe(t *testing.T) {
	for _, stalled := range []bool{false, true} {
		name := "HTTP503"
		if stalled {
			name = "stalled"
		}
		t.Run(name, func(t *testing.T) {
			f := newCoreFixture(t)
			var unquiesced atomic.Bool
			probeStarted := make(chan struct{}, 1)
			probeCanceled := make(chan struct{}, 1)
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/unquiesce":
					unquiesced.Store(true)
					w.WriteHeader(http.StatusNoContent)
				case "/healthz":
					select {
					case probeStarted <- struct{}{}:
					default:
					}
					if stalled {
						<-r.Context().Done()
						select {
						case probeCanceled <- struct{}{}:
						default:
						}
						return
					}
					w.WriteHeader(http.StatusServiceUnavailable)
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(func() { f.service.cancel(); endpoint.Close() })
			f.provider.mu.Lock()
			f.provider.guestURL = endpoint.URL
			f.provider.created = true
			f.provider.mu.Unlock()
			f.service.providers["resumable-k8s-pod"] = f.provider
			profile := f.config.Profiles[0]
			profile.Provider = "resumable-k8s-pod"
			if err := f.service.store.Update(func(st *State) error {
				st.Boxes["box-resume"] = boxRecord{
					Box:      Box{ID: "box-resume", Phase: "suspended", Generation: 1},
					ClientID: "client-a", Profile: profile,
					Handle:      boxprovider.Handle{Provider: profile.Provider, ID: "runtime-resume"},
					ExecutionID: "previous-execution",
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			op, err := f.service.action("client-a", "resume-async", "box-resume", "resume")
			if err != nil {
				t.Fatal(err)
			}
			f.waitOperation(t, op.ID, "succeeded")
			if !unquiesced.Load() {
				t.Fatal("resume completed before Guest accepted work")
			}
			select {
			case <-probeStarted:
			case <-time.After(time.Second):
				t.Fatal("background health check did not start")
			}
			// A later lifecycle transition must not be overwritten by an old probe.
			if err := f.service.store.Update(func(st *State) error {
				b := st.Boxes[op.TargetID]
				b.Box.Phase = "suspending"
				b.Box.OperationID = "next-operation"
				st.Boxes[op.TargetID] = b
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.service.Close(); err != nil {
				t.Fatal(err)
			}
			if stalled {
				select {
				case <-probeCanceled:
				case <-time.After(time.Second):
					t.Fatal("service shutdown did not cancel the background probe")
				}
			}
			b, err := f.service.rawBox(op.TargetID)
			if err != nil || b.Box.Phase != "suspending" || b.Box.OperationID != "next-operation" || b.Box.Error != nil {
				t.Fatalf("background probe changed lifecycle state: %+v, %v", b.Box, err)
			}
		})
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
		st.Executions[eid] = executionRecord{Execution: Execution{ID: eid, BoxID: box.ID, OperationID: op.ID, State: "running"}}
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
		b.Box.Phase = "failed"
		b.Box.Error = &APIError{Code: "RESTORE_FAILED", Message: "Archive checksum mismatch"}
		st.Boxes[box.ID] = b
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	status, body := f.call(t, "GET", "/v1/boxes/"+box.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if got := decodeResponse[Box](t, body); got.Phase != "failed" {
		t.Fatalf("query revived failed restore candidate: %+v", got)
	}
	status, body = f.call(t, "POST", "/v1/boxes/"+box.ID+":activate", testClientToken, "activate-failed-restore", nil)
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

func TestProfilesExposeOnlySupportedRuntimeBehaviorKinds(t *testing.T) {
	f := newCoreFixture(t)
	f.config.Profiles = append(f.config.Profiles, Profile{ID: "profile-k8s", Provider: "resumable-k8s-pod",
		Image: "example.invalid/agent@sha256:" + strings.Repeat("b", 64), Namespace: "test", NodeName: "test-node",
		CPU: 2, MemoryMiB: 1024, Guest: guestapi.DefaultConfig()})
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	f.service = nil
	var err error
	f.service, err = New(f.config, map[string]boxprovider.Provider{
		"docker": f.provider, "resumable-k8s-pod": f.provider,
	})
	if err != nil {
		t.Fatal(err)
	}
	status, body := f.call(t, "GET", "/v1/profiles", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	type profileResponse struct {
		Provider, Runtime, Behavior, Kind, Image, Workspace string
		CPU                                                 float64
		MemoryMiB                                           int64
		Agent                                               guestapi.Identity
	}
	profiles := decodeResponse[[]profileResponse](t, body)
	if len(profiles) != 2 {
		t.Fatalf("expected two admitted profiles: %s", body)
	}
	for _, profile := range profiles {
		switch profile.Provider {
		case "docker":
			if profile.Runtime != "docker" || profile.Behavior != "normal" || profile.Kind != "docker-normal" {
				t.Fatalf("wrong docker combination: %+v", profile)
			}
		case "resumable-k8s-pod":
			if profile.Runtime != "k8s" || profile.Behavior != "resumable" || profile.Kind != "k8s-resumable" {
				t.Fatalf("wrong k8s combination: %+v", profile)
			}
		default:
			t.Fatalf("unexpected provider: %+v", profile)
		}
		if profile.Image == "" || profile.Workspace == "" || profile.CPU < 1 || profile.MemoryMiB < 1 ||
			profile.Agent.UID != 11000 || profile.Agent.GID != 11000 {
			t.Fatalf("profile metadata missing: %+v", profile)
		}
	}
}

type homeRebuildProvider struct {
	*fakeCoreProvider
	calls atomic.Int32
}

func (p *homeRebuildProvider) Action(_ context.Context, _ boxprovider.Handle, action string) error {
	if action != "rebuild" {
		return errors.New("unexpected action")
	}
	p.calls.Add(1)
	p.setExecutionID("rebuilt-execution")
	return nil
}
func TestHomeRebuildGuardsAndIdempotencyRetainBox(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "home-rebuild-create")
	provider := &homeRebuildProvider{fakeCoreProvider: f.provider}
	f.service.providers["resumable-k8s-pod"] = provider
	change := func(phase string, persistent bool, lease bool) {
		t.Helper()
		if err := f.service.store.Update(func(st *State) error {
			b := st.Boxes[box.ID]
			b.Box.Phase = phase
			b.Profile.Provider = "resumable-k8s-pod"
			b.Profile.PersistentHome = persistent
			st.Boxes[box.ID] = b
			if lease {
				st.Leases["busy-lease"] = Lease{ID: "busy-lease", BoxID: box.ID, ExpiresAt: time.Now().Add(time.Minute)}
			} else {
				delete(st.Leases, "busy-lease")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		phase             string
		persistent, lease bool
	}{{"running", true, false}, {"failed", false, false}, {"failed", true, true}} {
		change(c.phase, c.persistent, c.lease)
		if _, err := f.service.action("client-a", "reject-"+c.phase+fmt.Sprint(c.persistent, c.lease), box.ID, "rebuild"); err == nil {
			t.Fatal("unsafe rebuild accepted", c)
		}
	}
	change("failed", true, false)
	op, err := f.service.action("client-a", "same-home-rebuild", box.ID, "rebuild")
	if err != nil {
		t.Fatal(err)
	}
	f.waitOperation(t, op.ID, "succeeded")
	again, err := f.service.action("client-a", "same-home-rebuild", box.ID, "rebuild")
	if err != nil || again.ID != op.ID || provider.calls.Load() != 1 {
		t.Fatal("rebuild was not idempotent", again, err)
	}
	record, err := f.service.rawBox(box.ID)
	if err != nil || record.Box.Phase != "running" || record.Box.Generation <= box.Generation {
		t.Fatal(record, err)
	}
	creates, deletes := f.provider.counts()
	if creates != 1 || deletes != 0 {
		t.Fatal("Box identity replaced", creates, deletes)
	}
	if f.activateCalls.Load() != 1 {
		t.Fatal("cold rebuild did not activate workload")
	}
	reconcile, err := f.service.action("client-a", "reconcile-rebuilt", box.ID, "reconcile")
	if err != nil {
		t.Fatal(err)
	}
	f.waitOperation(t, reconcile.ID, "succeeded")
	if f.activateCalls.Load() != 2 {
		t.Fatal("ready retry did not ensure workload activation")
	}

}
