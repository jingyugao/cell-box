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

func TestSharedDirectorySpecPropagation(t *testing.T) {
	p, s, ctx := fixture(t)
	s.SharedReadOnlyHostPath = "/srv/cocell/shared"
	h, err := p.Create(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{}
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	if w.Spec.SharedReadOnlyHostPath != s.SharedReadOnlyHostPath {
		t.Fatal("provider lost shared mount")
	}
	s.SharedReadOnlyHostPath = "/srv/other"
	if _, err := p.Create(ctx, s); err == nil {
		t.Fatal("reused box with a different shared directory")
	}
	s.BoxID = "box-invalid"
	s.SharedReadOnlyHostPath = "/"
	if _, err := p.Create(ctx, s); err == nil {
		t.Fatal("accepted root as shared directory")
	}
}

func TestPersistentHomeSpecPropagationAndReentryCheck(t *testing.T) {
	p, s, ctx := fixture(t)
	s.Config.Workspace = "/home/agent/workspace"
	s.PersistentHome = true
	h, err := p.Create(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{}
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	if !w.Spec.PersistentHome {
		t.Fatal("provider lost persistentHome")
	}
	s.PersistentHome = false
	if _, err := p.Create(ctx, s); err == nil {
		t.Fatal("re-entered Box with a different persistentHome setting")
	}
}

type uidClient struct{ client.Client }

func (c uidClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*api.ResumablePod); ok {
		obj.SetUID(types.UID("cr-uid"))
	}
	return c.Client.Create(ctx, obj, opts...)
}

type podGetHookClient struct {
	client.Client
	afterPodGet func()
}

func (c podGetHookClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := c.Client.Get(ctx, key, obj, opts...)
	if err == nil {
		if _, ok := obj.(*core.Pod); ok && c.afterPodGet != nil {
			c.afterPodGet()
		}
	}
	return err
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
	p := New(c)
	s := boxprovider.Spec{BoxID: "box-1", Namespace: "boxes", NodeName: "node-a", Image: "registry.example.invalid/prepared@sha256:" + strings.Repeat("a", 64), Config: guestapi.DefaultConfig(), CPU: 0.5, MemoryMiB: 256, Staged: true}
	return p, s, context.Background()
}

func TestCreateAndImmutableOwnership(t *testing.T) {
	p, s, ctx := fixture(t)
	s.CPU, s.MemoryMiB = 4, 8192
	h, err := p.Create(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if h.Name != "cellbox-box-1" || h.ID != "cr-uid" || h.Namespace != "boxes" || h.NodeName != "node-a" {
		t.Fatalf("bad handle: %#v", h)
	}
	if h.ImageID != s.Image {
		t.Fatalf("archive image identity missing from handle: got %q, want %q", h.ImageID, s.Image)
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
	if w.Spec.Container.Resources.Limits.Cpu().MilliValue() != 4000 || w.Spec.Container.Resources.Limits.Memory().Value() != 8192*1024*1024 {
		t.Fatal("resource limits missing")
	}
	for _, name := range []core.ResourceName{core.ResourceCPU, core.ResourceMemory} {
		request, exists := w.Spec.Container.Resources.Requests[name]
		if !exists || !request.IsZero() {
			t.Fatalf("%s request must be explicitly zero to prevent limit defaulting", name)
		}
	}
	repeated, err := p.Create(ctx, s)
	if err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	if repeated.ImageID != h.ImageID {
		t.Fatalf("idempotent create changed archive image identity: got %q, want %q", repeated.ImageID, h.ImageID)
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
	if err != nil || obs.Phase != "suspended" || obs.ExecutionID != "" {
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
	w.Status.Cycle = 7
	if err = p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "pod-a", Namespace: "boxes", UID: "pod-uid", OwnerReferences: []meta.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: api.Kind, Name: w.Name, UID: w.UID, Controller: boolPtr(true)}}}, Status: core.PodStatus{Phase: core.PodRunning, Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue}}}}
	if err = p.Client.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	svc := &core.Service{ObjectMeta: meta.ObjectMeta{Name: w.Name, Namespace: w.Namespace, OwnerReferences: pod.OwnerReferences}, Spec: core.ServiceSpec{Selector: map[string]string{api.OwnerLabel: string(w.UID), api.ServingLabel: "true"}, Ports: w.Spec.ServicePorts}}
	if err = p.Client.Create(ctx, svc); err != nil {
		t.Fatal(err)
	}
	pod.Status.Conditions[0].Status = core.ConditionFalse
	if err = p.Client.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	obs, err = p.Inspect(ctx, h)
	if err != nil || obs.Phase != "running" || obs.ExecutionID != "pod-uid" || obs.Generation != 7 {
		t.Fatalf("not-ready Pod changed lifecycle: %#v %v", obs, err)
	}
	if _, err = p.Guest(ctx, h); !errors.Is(err, boxprovider.ErrNotReady) {
		t.Fatalf("Guest accepted a not-ready Pod: %v", err)
	}
	pod.Status.Conditions[0].Status = core.ConditionTrue
	if err = p.Client.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Guest(ctx, h); !errors.Is(err, boxprovider.ErrNotReady) {
		t.Fatalf("Guest accepted a Pod before the controller allowed traffic: %v", err)
	}
	pod.Labels = map[string]string{api.ServingLabel: "true"}
	if err = p.Client.Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Guest(ctx, h); !errors.Is(err, boxprovider.ErrNotReady) {
		t.Fatalf("Guest accepted a Pod without an IP address: %v", err)
	}
	pod.Status.PodIP = "10.42.0.12"
	pod.Status.ContainerStatuses = []core.ContainerStatus{{Name: w.Spec.Container.Name, ContainerID: "containerd://one", State: core.ContainerState{Running: &core.ContainerStateRunning{}}}}
	pod.Status.Conditions[0].Status = core.ConditionFalse
	if err = p.Client.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	obs, err = p.Inspect(ctx, h)
	if err != nil || obs.Phase != "running" || obs.ExecutionID != "pod-uid" {
		t.Fatalf("running: %#v %v", obs, err)
	}
	conn, err := p.Guest(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if conn.URL != "http://10.42.0.12:40000" {
		t.Fatalf("guest: %#v", conn)
	}
	if _, err = p.GuestForExecution(ctx, h, "old-pod"); !errors.Is(err, boxprovider.ErrStaleExecution) {
		t.Fatalf("Guest accepted a stale execution: %v", err)
	}
	originalClient := p.Client
	p.Client = podGetHookClient{Client: originalClient, afterPodGet: func() {
		p.Client = originalClient
		pod.UID = "replacement"
		if updateErr := p.Client.Update(ctx, pod); updateErr != nil {
			t.Fatal(updateErr)
		}
	}}
	if _, err = p.Guest(ctx, h); err == nil || !strings.Contains(err.Error(), "UID changed") {
		t.Fatalf("Pod replacement during connection resolution was accepted: %v", err)
	}
	obs, err = p.Inspect(ctx, h)
	if err != nil || obs.Phase != "failed" {
		t.Fatalf("changed Pod identity was not observed as failed: %#v %v", obs, err)
	}
	pod.UID = "pod-uid"
	if err = p.Client.Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Phase = core.PodFailed
	if err = p.Client.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	obs, err = p.Inspect(ctx, h)
	if err != nil || obs.Phase != "failed" {
		t.Fatalf("exited Pod was not observed as failed: %#v %v", obs, err)
	}
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
	if err != nil || obs.Phase != "deleting" {
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
	if err != nil || obs.Phase != "deleted" {
		t.Fatalf("deleted: %#v %v", obs, err)
	}
	if err = p.Destroy(ctx, h); err != nil {
		t.Fatalf("idempotent destroy: %v", err)
	}
}

func TestInspectRejectsExecutionSwitchDuringPodRead(t *testing.T) {
	p, s, ctx := fixture(t)
	h, err := p.Create(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{}
	if err = p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase = "Running"
	w.Status.PodName = "pod-a"
	w.Status.PodUID = "pod-old"
	if err = p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "pod-a", Namespace: h.Namespace, UID: "pod-old", OwnerReferences: []meta.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: api.Kind, Name: w.Name, UID: w.UID, Controller: boolPtr(true)}}}, Status: core.PodStatus{Phase: core.PodRunning}}
	if err = p.Client.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	base := p.Client
	p.Client = podGetHookClient{Client: base, afterPodGet: func() {
		latest := &api.ResumablePod{}
		if err := base.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, latest); err != nil {
			t.Fatal(err)
		}
		latest.Status.Conditions = []meta.Condition{{Type: "Ready", Status: meta.ConditionTrue, Reason: "ProbeCompleted", LastTransitionTime: meta.Now()}}
		if err := base.Status().Update(ctx, latest); err != nil {
			t.Fatal(err)
		}
	}}
	if observed, err := p.Inspect(ctx, h); err != nil || observed.Phase != "running" {
		t.Fatalf("readiness-only update interrupted execution observation: %+v %v", observed, err)
	}
	p.Client = podGetHookClient{Client: base, afterPodGet: func() {
		latest := &api.ResumablePod{}
		if getErr := base.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, latest); getErr != nil {
			t.Fatal(getErr)
		}
		latest.Status.PodName = "pod-b"
		latest.Status.PodUID = "pod-new"
		if updateErr := base.Status().Update(ctx, latest); updateErr != nil {
			t.Fatal(updateErr)
		}
	}}
	if _, err = p.Inspect(ctx, h); err == nil || !strings.Contains(err.Error(), "changed during lifecycle inspection") {
		t.Fatalf("Inspect accepted a switched execution: %v", err)
	}
}
func boolPtr(v bool) *bool { return &v }

func TestRebuildRequiresFailedPersistentHomeAndReportsPendingRequest(t *testing.T) {
	p, spec, ctx := fixture(t)
	spec.PersistentHome = true
	h, err := p.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{}
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	if err := p.Action(ctx, h, "rebuild"); err == nil {
		t.Fatal("healthy box rebuilt")
	}
	w.Status.Phase = "Failed"
	w.Status.PodUID = "old"
	if err := p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := p.Action(ctx, h, "rebuild"); err != nil {
		t.Fatal(err)
	}
	if err := p.Client.Get(ctx, client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
	if w.Spec.RebuildNonce == "" || w.UID != types.UID(h.ID) {
		t.Fatal("lost restart request or box identity")
	}
	observed, err := p.Inspect(ctx, h)
	if err != nil || observed.Phase != "creating" || observed.ExecutionID != "" {
		t.Fatal("pending rebuild exposed old failed execution", observed, err)
	}
}

func TestDiskUpgradeKeepsOwnerAndHidesOldExecution(t *testing.T) {
	p, spec, ctx := fixture(t)
	spec.PersistentHome = true
	h, err := p.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{}
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase, w.Status.PodUID = "Running", "old-pod"
	if err := p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	oldImage := spec.Image
	spec.Image = "registry.example.invalid/prepared@sha256:" + strings.Repeat("b", 64)
	upgraded, err := p.Upgrade(ctx, h, spec, "upgrade-1")
	if err != nil || upgraded.ID != h.ID || upgraded.ImageID != spec.Image {
		t.Fatal(upgraded, err)
	}
	if err := p.Client.Get(ctx, client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
	if w.Spec.Upgrade.PreviousContainer.Image != oldImage || w.Spec.Container.Image != spec.Image {
		t.Fatal("lost image transition")
	}
	observed, err := p.Inspect(ctx, upgraded)
	if err != nil || observed.Phase != "creating" || observed.ExecutionID != "" {
		t.Fatal("exposed old execution", observed, err)
	}
	if _, err := p.Upgrade(ctx, h, spec, "upgrade-1"); err != nil {
		t.Fatal("lost response not replayable", err)
	}
	if _, err := p.Upgrade(ctx, h, spec, "upgrade-2"); err == nil {
		t.Fatal("overlapping upgrade accepted")
	}
	spec.NodeName = "another-node"
	if _, err := p.Upgrade(ctx, h, spec, "upgrade-1"); err == nil {
		t.Fatal("disk move accepted")
	}
}
