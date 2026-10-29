package resumable

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/runtimecontrol"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	_ "modernc.org/sqlite"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type providerOfflineClient struct {
	client.Client
	blocked  bool
	accesses int
}

func (c *providerOfflineClient) access(operation string) error {
	c.accesses++
	return fmt.Errorf("unexpected raw Kubernetes %s", operation)
}
func (c *providerOfflineClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if c.blocked {
		return c.access("Get")
	}
	return c.Client.Get(ctx, key, obj, opts...)
}
func (c *providerOfflineClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if c.blocked {
		return c.access("List")
	}
	return c.Client.List(ctx, list, opts...)
}
func (c *providerOfflineClient) Create(context.Context, client.Object, ...client.CreateOption) error {
	return c.access("Create")
}
func (c *providerOfflineClient) Update(context.Context, client.Object, ...client.UpdateOption) error {
	return c.access("Update")
}
func (c *providerOfflineClient) Patch(context.Context, client.Object, client.Patch, ...client.PatchOption) error {
	return c.access("Patch")
}
func (c *providerOfflineClient) Delete(context.Context, client.Object, ...client.DeleteOption) error {
	return c.access("Delete")
}
func (c *providerOfflineClient) Status() client.SubResourceWriter {
	return providerOfflineStatus{SubResourceWriter: c.Client.Status(), parent: c}
}

type providerOfflineStatus struct {
	client.SubResourceWriter
	parent *providerOfflineClient
}

func (s providerOfflineStatus) Update(context.Context, client.Object, ...client.SubResourceUpdateOption) error {
	return s.parent.access("status Update")
}

func makeLocalCompletedResume(t *testing.T) (*Provider, boxprovider.Handle, context.Context, *providerOfflineClient, *api.Execution) {
	t.Helper()
	p, spec, ctx := fixture(t)
	h, err := p.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKey{Namespace: h.Namespace, Name: h.Name}
	baseline := &api.ResumablePod{}
	if err := p.Client.Get(ctx, key, baseline); err != nil {
		t.Fatal(err)
	}
	baseline.Spec.DesiredState = "Suspended"
	baseline.Spec.TransitionID = "before-resume"
	if err := p.Client.Update(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	baseline.Status.Phase = "Suspended"
	baseline.Status.ObservedTransitionID = "before-resume"
	if err := p.Client.Status().Update(ctx, baseline); err != nil {
		t.Fatal(err)
	}

	offline := &providerOfflineClient{Client: p.Client}
	p.Client = offline
	local := runtimecontrol.New(offline)
	p.Local = local
	dbPath := filepath.Join(t.TempDir(), "cellbox.db")
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE runtime_records (
        kind TEXT NOT NULL, id TEXT NOT NULL, client_id TEXT NOT NULL DEFAULT '',
        box_id TEXT NOT NULL DEFAULT '', record TEXT NOT NULL, PRIMARY KEY(kind,id))`); err != nil {
		t.Fatal(err)
	}
	if err := local.SetJournal(ctx, runtimecontrol.NewJournal(db)); err != nil {
		t.Fatal(err)
	}
	seeded := &api.ResumablePod{}
	if err := local.Get(ctx, key, seeded); err != nil {
		t.Fatal(err)
	}
	offline.blocked = true
	if err := p.RequestResume(ctx, h, "before-resume", "resume-transition"); err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{}
	if err := local.Get(ctx, key, w); err != nil {
		t.Fatal(err)
	}
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "warm-claimed", Namespace: h.Namespace, UID: "warm-pod-uid"}}
	handled, err := local.ClaimWarm(ctx, w, pod)
	if err != nil || !handled {
		t.Fatalf("ClaimWarm handled=%v err=%v", handled, err)
	}
	execution := &api.Execution{PodUID: string(pod.UID), ContainerID: "container-123", PodIP: "10.42.1.23"}
	w.Status.Phase = "Running"
	w.Status.ObservedGeneration = int64(w.Generation)
	w.Status.ObservedTransitionID = w.Spec.TransitionID
	w.Status.Execution = execution
	if err := local.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	return p, h, ctx, offline, execution
}

func TestGuestForExecutionUsesLocalCRIConfirmationOffline(t *testing.T) {
	p, h, ctx, offline, expected := makeLocalCompletedResume(t)
	confirmed := 0
	p.ConfirmExecutionLocal = func(_ context.Context, w *api.ResumablePod, pod *core.Pod) (*api.Execution, error) {
		confirmed++
		if w.Status.PodUID != string(pod.UID) || string(pod.UID) != expected.PodUID {
			t.Fatalf("CRI confirmation received mismatched local identities: boxPod=%s pod=%s", w.Status.PodUID, pod.UID)
		}
		copy := *expected
		return &copy, nil
	}

	connection, err := p.GuestForExecution(ctx, h, expected.PodUID)
	if err != nil {
		t.Fatal(err)
	}
	wantURL := "http://" + net.JoinHostPort(expected.PodIP, fmt.Sprint(guestapi.Port))
	if connection.URL != wantURL {
		t.Fatalf("guest URL=%q, want local CRI address %q", connection.URL, wantURL)
	}
	if confirmed != 1 {
		t.Fatalf("CRI identity confirmation calls=%d, want 1", confirmed)
	}
	if _, err := p.GuestForExecution(ctx, h, "replacement-pod-uid"); !errors.Is(err, boxprovider.ErrStaleExecution) {
		t.Fatalf("wrong expected Pod UID error=%v, want stale execution", err)
	}
	if confirmed != 1 {
		t.Fatalf("wrong expected UID reached CRI confirmation; calls=%d", confirmed)
	}

	p.ConfirmExecutionLocal = func(context.Context, *api.ResumablePod, *core.Pod) (*api.Execution, error) {
		return &api.Execution{PodUID: expected.PodUID, ContainerID: "replacement-container", PodIP: expected.PodIP}, nil
	}
	if _, err := p.GuestForExecution(ctx, h, expected.PodUID); !errors.Is(err, boxprovider.ErrStaleExecution) {
		t.Fatalf("replacement CRI execution error=%v, want stale execution", err)
	}
	p.ConfirmExecutionLocal = func(context.Context, *api.ResumablePod, *core.Pod) (*api.Execution, error) {
		return nil, errors.New("CRI reports container stopped")
	}
	if _, err := p.GuestForExecution(ctx, h, expected.PodUID); err == nil {
		t.Fatal("failed CRI confirmation was accepted")
	}
	if offline.accesses != 0 {
		t.Fatalf("provider attempted %d raw Kubernetes accesses while serving local completion", offline.accesses)
	}
}
