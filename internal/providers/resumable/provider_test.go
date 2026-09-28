package resumable

import (
	"context"
	"errors"
	"strings"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type uidClient struct{ client.Client }

func (c uidClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*api.ResumablePod); ok {
		obj.SetUID(types.UID("cr-uid"))
	}
	return c.Client.Create(ctx, obj, opts...)
}

type tokenExec struct {
	calls int
	after func()
}

func (e *tokenExec) Token(_ context.Context, _, _, _ string) (string, error) {
	e.calls++
	if e.after != nil {
		e.after()
	}
	return "private-token\n", nil
}

func fixture(t *testing.T) (*Provider, boxprovider.Spec, context.Context) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := core.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := uidClient{fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.ResumablePod{}, &core.Pod{}).Build()}
	p := New(c, &tokenExec{})
	s := boxprovider.Spec{BoxID: "box-1", Namespace: "boxes", NodeName: "node-a", Image: "registry.example.invalid/prepared@sha256:" + strings.Repeat("a", 64), Config: guestapi.DefaultConfig(), CPU: 0.5, MemoryMiB: 256, Staged: true}
	return p, s, context.Background()
}

func TestCreateAndImmutableOwnership(t *testing.T) {
	p, s, ctx := fixture(t)
	h, err := p.Create(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if h.Name != "cellbox-box-1" || h.ID != "cr-uid" || h.Namespace != "boxes" || h.NodeName != "node-a" {
		t.Fatalf("bad handle: %#v", h)
	}
	w := &api.ResumablePod{}
	if err = p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	if w.Spec.Container.ImagePullPolicy != core.PullIfNotPresent || len(w.Spec.Container.Ports) != 1 || len(w.Spec.Container.VolumeMounts) != 0 || len(w.Spec.Container.Env) != 0 || len(w.Spec.Container.Args) != 4 || w.Spec.Container.Args[3] != "--staged" {
		t.Fatalf("unexpected container: %#v", w.Spec.Container)
	}
	if w.Spec.Container.SecurityContext == nil || *w.Spec.Container.SecurityContext.RunAsUser != 0 {
		t.Fatal("guest must bootstrap as root")
	}
	caps := w.Spec.Container.SecurityContext.Capabilities
	if caps == nil || len(caps.Drop) != 1 || caps.Drop[0] != "ALL" || len(caps.Add) != 5 || caps.Add[0] != "CHOWN" || caps.Add[1] != "SETUID" || caps.Add[2] != "SETGID" || caps.Add[3] != "FOWNER" || caps.Add[4] != "DAC_OVERRIDE" {
		t.Fatalf("unexpected guest capabilities: %#v", caps)
	}
	if w.Spec.Container.Resources.Limits.Cpu().MilliValue() != 500 || w.Spec.Container.Resources.Limits.Memory().Value() != 256*1024*1024 {
		t.Fatal("resource limits missing")
	}
	if _, err = p.Create(ctx, s); err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	s.Image = "registry.example.invalid/other@sha256:" + strings.Repeat("b", 64)
	if _, err = p.Create(ctx, s); err == nil {
		t.Fatal("changed immutable image was accepted")
	}
	s.Image = "registry.example.invalid/prepared@sha256:" + strings.Repeat("a", 64)
	s.CPU = 1
	if _, err = p.Create(ctx, s); err == nil {
		t.Fatal("changed immutable resources were accepted")
	}
	s.CPU = 0
	if _, err = p.Create(ctx, s); err == nil {
		t.Fatal("zero CPU was accepted")
	}
	s.CPU = 0.5
	s.Image = "mutable:latest"
	if _, err = p.Create(ctx, s); err == nil {
		t.Fatal("mutable image tag was accepted")
	}
	h.ID = "wrong-uid"
	if err = p.Destroy(ctx, h); err == nil {
		t.Fatal("wrong UID could destroy CR")
	}
}

func TestLifecycleAndGuestIdentity(t *testing.T) {
	p, s, ctx := fixture(t)
	h, err := p.Create(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Action(ctx, h, "freeze"); !errors.Is(err, boxprovider.ErrUnsupported) {
		t.Fatalf("freeze: %v", err)
	}
	if err = p.Action(ctx, h, "suspend"); err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{}
	if err = p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	if w.Spec.DesiredState != "Suspended" {
		t.Fatal("suspend did not patch desired state")
	}
	w.Status.Phase = "Suspended"
	if err = p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	obs, err := p.Inspect(ctx, h)
	if err != nil || obs.State != "suspended" || obs.ExecutionID != "" {
		t.Fatalf("suspended: %#v %v", obs, err)
	}
	if err = p.Action(ctx, h, "resume"); err != nil {
		t.Fatal(err)
	}
	if err = p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase = "Running"
	w.Status.PodName = "pod-a"
	w.Status.PodUID = "pod-uid"
	if err = p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "pod-a", Namespace: "boxes", UID: "pod-uid", OwnerReferences: []meta.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "ResumablePod", Name: w.Name, UID: w.UID, Controller: boolPtr(true)}}}, Status: core.PodStatus{Phase: core.PodRunning, Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue}}}}
	if err = p.Client.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	svc := &core.Service{ObjectMeta: meta.ObjectMeta{Name: w.Name, Namespace: w.Namespace, OwnerReferences: pod.OwnerReferences}, Spec: core.ServiceSpec{Selector: map[string]string{api.OwnerLabel: string(w.UID), "recovery.gvisor.dev/serving": "true"}, Ports: w.Spec.ServicePorts}}
	if err = p.Client.Create(ctx, svc); err != nil {
		t.Fatal(err)
	}
	obs, err = p.Inspect(ctx, h)
	if err != nil || obs.State != "ready" || obs.ExecutionID != "pod-uid" {
		t.Fatalf("ready: %#v %v", obs, err)
	}
	conn, err := p.Guest(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if conn.URL != "http://cellbox-box-1.boxes.svc:40000" || conn.Token != "private-token" {
		t.Fatalf("guest: %#v", conn)
	}
	exec := p.Exec.(*tokenExec)
	exec.after = func() {
		pod.UID = "replacement"
		if updateErr := p.Client.Update(ctx, pod); updateErr != nil {
			t.Fatal(updateErr)
		}
	}
	if _, err = p.Guest(ctx, h); err == nil || !strings.Contains(err.Error(), "UID changed") {
		t.Fatalf("Pod replacement during token retrieval was accepted: %v", err)
	}
	exec.after = nil
	if err = p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	w.Finalizers = []string{api.Finalizer}
	if err = p.Client.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err = p.Destroy(ctx, h); err != nil {
		t.Fatal(err)
	}
	obs, err = p.Inspect(ctx, h)
	if err != nil || obs.State != "deleting" {
		t.Fatalf("deleting: %#v %v", obs, err)
	}
	if err = p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	w.Finalizers = nil
	if err = p.Client.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	obs, err = p.Inspect(ctx, h)
	if err != nil || obs.State != "deleted" {
		t.Fatalf("deleted: %#v %v", obs, err)
	}
	if err = p.Destroy(ctx, h); err != nil {
		t.Fatalf("idempotent destroy: %v", err)
	}
}
func boolPtr(v bool) *bool { return &v }
