// SPDX-License-Identifier: Apache-2.0

package controller

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	lifecycle "cellbox.local/cellbox/internal/runtime"
	"context"
	"errors"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"strings"
	"testing"
	"time"
)

func TestSharedDirectoryMountAdmissionAndFingerprint(t *testing.T) {
	_, _, w := fixture(t)
	legacyHash := fingerprint(w)
	w.Spec.SharedReadOnlyHostPath = "/srv/cocell/shared"
	w.Spec.DebugReadOnlyHostPath = "/srv/debug"
	w.Status.SpecHash = fingerprint(w)
	if w.Status.SpecHash == legacyHash {
		t.Fatal("shared mount must affect immutable fingerprint")
	}
	if err := validate(w); err != nil {
		t.Fatal(err)
	}
	p := &core.Pod{Spec: core.PodSpec{Containers: []core.Container{w.Spec.Container}, Volumes: hostVolumes(w)}}
	p.Spec.Containers[0].VolumeMounts = hostMounts(w)
	if !admittedDebugMount(w, p) {
		t.Fatal("dedicated shared and debug mounts should coexist")
	}
	if p.Spec.Containers[0].VolumeMounts[1].MountPath != api.SharedMountPath || !p.Spec.Containers[0].VolumeMounts[1].ReadOnly {
		t.Fatal("shared directory must be read-only at fixed path")
	}
	tampered := p.DeepCopy()
	tampered.Spec.Containers[0].VolumeMounts[1].ReadOnly = false
	if admittedDebugMount(w, tampered) {
		t.Fatal("writable shared mount was admitted")
	}
	tampered = p.DeepCopy()
	tampered.Spec.Volumes[1].HostPath.Path = "/srv/other"
	if admittedDebugMount(w, tampered) {
		t.Fatal("different host directory was admitted")
	}
	tampered = p.DeepCopy()
	tampered.Spec.Containers[0].VolumeMounts[1].SubPath = "config.json"
	if admittedDebugMount(w, tampered) {
		t.Fatal("individual-file subPath was admitted")
	}
	w.Spec.SharedReadOnlyHostPath = "/srv/other"
	if validate(w) == nil {
		t.Fatal("shared directory must be immutable")
	}
	w.Status.SpecHash = ""
	for _, path := range []string{"/", "relative", "/srv/../other", "/srv/shared\n"} {
		w.Spec.SharedReadOnlyHostPath = path
		if validate(w) == nil {
			t.Fatalf("accepted invalid path %q", path)
		}
	}
}

type fakeRuntime struct {
	checkpoints, prepares, cleanups int
	prepareError, cleanupError      error
	checkpointError                 error
	checkpointFailures              int
	prepareFailures                 int
	inventoryError                  error
	inventoryPhases                 []string
}

func (f *fakeRuntime) Checkpoint(context.Context, *api.ResumablePod, *core.Pod) error {
	f.checkpoints++
	if f.checkpointFailures > 0 {
		f.checkpointFailures--
		return f.checkpointError
	}
	return nil
}
func (f *fakeRuntime) Prepare(context.Context, *api.ResumablePod, *core.Pod) error {
	f.prepares++
	if f.prepareFailures > 0 {
		f.prepareFailures--
		err := f.prepareError
		if f.prepareFailures == 0 {
			f.prepareError = nil
		}
		return err
	}
	return f.prepareError
}
func (f *fakeRuntime) Cleanup(context.Context, *core.Pod) error        { f.cleanups++; return f.cleanupError }
func (f *fakeRuntime) Forget(context.Context, *api.ResumablePod) error { return nil }
func (f *fakeRuntime) SyncInventory(_ context.Context, w *api.ResumablePod) error {
	f.inventoryPhases = append(f.inventoryPhases, w.Status.Phase)
	return f.inventoryError
}
func fixture(t *testing.T) (*Reconciler, *fakeRuntime, *api.ResumablePod) {
	t.Helper()
	s := runtime.NewScheme()
	core.AddToScheme(s)
	api.AddToScheme(s)
	w := &api.ResumablePod{TypeMeta: meta.TypeMeta{APIVersion: api.GroupVersion.String(), Kind: api.Kind}, ObjectMeta: meta.ObjectMeta{Name: "counter", Namespace: "test", UID: "12345678-aaaa", Finalizers: []string{api.Finalizer}}, Spec: api.Spec{NodeName: "node", DesiredState: "Running", Container: core.Container{Name: "server", Image: "image:local", ImagePullPolicy: core.PullNever}}, Status: api.Status{Phase: "Running", PodName: "old", PodUID: "old-uid", Cycle: 1, Since: meta.Now()}}
	w.Status.SpecHash = fingerprint(w)
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "old", Namespace: "test", UID: "old-uid"}, Spec: core.PodSpec{Containers: []core.Container{w.Spec.Container}}, Status: core.PodStatus{Phase: core.PodRunning, PodIP: "10.42.0.12", ContainerStatuses: []core.ContainerStatus{{Name: w.Spec.Container.Name, ContainerID: "containerd://one", State: core.ContainerState{Running: &core.ContainerStateRunning{}}}}, Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue}}}}
	no := false
	runtimeClass := api.RuntimeClass
	p.Spec.RestartPolicy = core.RestartPolicyNever
	p.Spec.RuntimeClassName = &runtimeClass
	p.Spec.AutomountServiceAccountToken = &no
	controllerutil.SetControllerReference(w, p, s)
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&api.ResumablePod{}, &core.Pod{}).WithObjects(w, p).Build()
	f := &fakeRuntime{}
	return &Reconciler{Client: c, Runtime: f, NodeName: "node"}, f, w
}

func TestPullPolicyRequiresImmutableImage(t *testing.T) {
	_, _, w := fixture(t)
	w.Spec.Container.ImagePullPolicy = core.PullIfNotPresent
	w.Spec.Container.Image = "registry.example.invalid/prepared@sha256:" + strings.Repeat("a", 64)
	w.Status.SpecHash = fingerprint(w)
	if err := validate(w); err != nil {
		t.Fatalf("immutable pullable image rejected: %v", err)
	}
	w.Spec.Container.Image = "registry.example.invalid/prepared:latest"
	w.Status.SpecHash = fingerprint(w)
	if err := validate(w); err == nil {
		t.Fatal("mutable pullable image accepted")
	}
	w.Spec.Container.ImagePullPolicy = core.PullAlways
	w.Spec.Container.Image = "registry.example.invalid/prepared@sha256:" + strings.Repeat("a", 64)
	w.Status.SpecHash = fingerprint(w)
	if err := validate(w); err == nil {
		t.Fatal("unsupported pull policy accepted")
	}
}
func step(t *testing.T, r *Reconciler, w *api.ResumablePod) {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Get(context.Background(), client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
}
func desired(t *testing.T, r *Reconciler, w *api.ResumablePod, s string) {
	t.Helper()
	w.Spec.DesiredState = s
	if err := r.Update(context.Background(), w); err != nil {
		t.Fatal(err)
	}
}
func TestSuspendResumeAndControllerRestart(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	desired(t, r, w, "Suspended")
	step(t, r, w)
	if w.Status.Phase != "Checkpointing" {
		t.Fatal(w.Status)
	}
	step(t, r, w)
	// Reinstantiate the reconciler: no volatile lifecycle state is necessary.
	r = &Reconciler{Client: r.Client, Runtime: f, NodeName: "node"}
	step(t, r, w)
	step(t, r, w)
	if w.Status.Phase != "Suspended" || w.Status.Snapshot == "" || f.checkpoints != 1 {
		t.Fatal(w.Status, f)
	}
	desired(t, r, w, "Running")
	step(t, r, w)
	step(t, r, w)
	p := &core.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Status.PodName}, p); err != nil {
		t.Fatal(err)
	}
	if len(p.Spec.SchedulingGates) != 1 {
		t.Fatal("restore scheduled before authorization")
	}
	p.UID = "new-uid"
	if err := r.Update(ctx, p); err != nil {
		t.Fatal(err)
	}
	step(t, r, w)
	step(t, r, w)
	if f.prepares != 1 {
		t.Fatal(f)
	}
	r.Get(ctx, client.ObjectKeyFromObject(p), p)
	if p.Annotations[api.TicketAnnotation] != "new-uid" || len(p.Spec.SchedulingGates) != 0 {
		t.Fatal(p)
	}
	p.Status = core.PodStatus{Phase: core.PodRunning, PodIP: "10.42.0.12", ContainerStatuses: []core.ContainerStatus{{Name: w.Spec.Container.Name, ContainerID: "containerd://restored", State: core.ContainerState{Running: &core.ContainerStateRunning{}}}}, Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionFalse}}}
	r.Status().Update(ctx, p)
	step(t, r, w)
	if w.Status.Phase != "Running" || w.Status.Snapshot != "" {
		t.Fatal("old snapshot must not be replayable after readiness", w.Status)
	}
}

func TestCheckpointStorageOutageRetriesWithoutFailingLifecycle(t *testing.T) {
	r, f, w := fixture(t)
	desired(t, r, w, "Suspended")
	step(t, r, w)
	if w.Status.Phase != "Checkpointing" {
		t.Fatal(w.Status)
	}
	f.checkpointError = lifecycle.ErrRetryableStorage
	f.checkpointFailures = 1
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)})
	if !errors.Is(err, lifecycle.ErrRetryableStorage) {
		t.Fatalf("expected retryable storage error, got %v", err)
	}
	if err = r.Get(context.Background(), client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
	if w.Status.Phase != "Checkpointing" {
		t.Fatalf("temporary storage outage moved lifecycle to %q", w.Status.Phase)
	}
	step(t, r, w)
	if w.Status.Phase != "Suspending" || f.checkpoints != 2 {
		t.Fatalf("checkpoint did not retry and advance: phase=%s calls=%d", w.Status.Phase, f.checkpoints)
	}
}

func TestInventoryStorageOutageBlocksResume(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	w.Status.Phase = "Suspended"
	w.Status.Snapshot = "checkpoint-1"
	w.Status.PodUID = ""
	w.Status.PodName = ""
	if err := r.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	cycle := w.Status.Cycle
	f.inventoryError = lifecycle.ErrRetryableStorage

	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)})
	if !errors.Is(err, lifecycle.ErrRetryableStorage) {
		t.Fatalf("expected inventory storage error, got %v", err)
	}
	if len(f.inventoryPhases) == 0 || f.inventoryPhases[len(f.inventoryPhases)-1] != "Restoring" {
		t.Fatalf("resume did not request index removal first: %v", f.inventoryPhases)
	}
	if err = r.Get(ctx, client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
	if w.Status.Phase != "Suspended" || w.Status.Cycle != cycle || f.prepares != 0 {
		t.Fatalf("resume progressed before inventory removal: status=%+v prepares=%d", w.Status, f.prepares)
	}
}

func TestRestoreStorageOutageRetriesWithoutFailingLifecycle(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	p := &core.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Name: "old", Namespace: "test"}, p); err != nil {
		t.Fatal(err)
	}
	p.Spec.SchedulingGates = []core.PodSchedulingGate{{Name: api.Gate}}
	if err := r.Update(ctx, p); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase = "Restoring"
	w.Status.Snapshot = "checkpoint-1"
	if err := r.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	f.prepareError = lifecycle.ErrRetryableStorage
	f.prepareFailures = 1
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)})
	if !errors.Is(err, lifecycle.ErrRetryableStorage) {
		t.Fatalf("expected retryable storage error, got %v", err)
	}
	if err = r.Get(ctx, client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
	if w.Status.Phase != "Restoring" {
		t.Fatalf("temporary storage outage moved lifecycle to %q", w.Status.Phase)
	}
	step(t, r, w)
	if f.prepares != 2 || w.Status.Phase != "Restoring" {
		t.Fatalf("restore did not retry in place: phase=%s calls=%d", w.Status.Phase, f.prepares)
	}
}

func TestLostExecutionNeverColdStarts(t *testing.T) {
	r, _, w := fixture(t)
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "old", Namespace: "test"}}
	r.Delete(context.Background(), p)
	step(t, r, w)
	step(t, r, w)
	if w.Status.Phase != "Failed" {
		t.Fatal(w.Status)
	}
	w.Spec.RetryNonce = "retry"
	r.Update(context.Background(), w)
	step(t, r, w)
	if w.Status.Phase != "Failed" {
		t.Fatal("cold fallback", w.Status)
	}
	ps := &core.PodList{}
	r.List(context.Background(), ps)
	if len(ps.Items) != 0 {
		t.Fatal("replacement created")
	}
}
func TestRestoreFailureRequiresCleanupAndExplicitRetry(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	p := &core.Pod{}
	r.Get(ctx, types.NamespacedName{Name: "old", Namespace: "test"}, p)
	p.Spec.SchedulingGates = []core.PodSchedulingGate{{Name: api.Gate}}
	r.Update(ctx, p)
	w.Status.Phase = "Restoring"
	w.Status.Snapshot = "checkpoint-1"
	r.Status().Update(ctx, w)
	f.prepareError = errors.New("bad checksum")
	step(t, r, w)
	if w.Status.Phase != "Failing" {
		t.Fatal(w.Status)
	}
	f.cleanupError = errors.New("runtime busy")
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)})
	if err == nil {
		t.Fatal("cleanup error hidden")
	}
	r.Get(ctx, client.ObjectKeyFromObject(w), w)
	if w.Status.Phase != "Failing" {
		t.Fatal(w.Status)
	}
	f.cleanupError = nil
	step(t, r, w)
	if w.Status.Phase != "Failed" {
		t.Fatal(w.Status)
	}
	step(t, r, w)
	if w.Status.Phase != "Failed" {
		t.Fatal("retried without nonce")
	}
	w.Spec.RetryNonce = "2"
	r.Update(ctx, w)
	step(t, r, w)
	if w.Status.Phase != "Restoring" || w.Status.PodUID != "" {
		t.Fatal(w.Status)
	}
}
func TestStartupTimeoutAndSpecMutation(t *testing.T) {
	for _, kind := range []string{"timeout", "mutation"} {
		t.Run(kind, func(t *testing.T) {
			r, _, w := fixture(t)
			if kind == "timeout" {
				w.Status.Phase = "Creating"
				w.Status.Since = meta.NewTime(time.Now().Add(-time.Hour))
				r.Status().Update(context.Background(), w)
				p := &core.Pod{}
				r.Get(context.Background(), types.NamespacedName{Name: "old", Namespace: "test"}, p)
				p.Status = core.PodStatus{Phase: core.PodPending}
				r.Status().Update(context.Background(), p)
			} else {
				w.Spec.Container.Args = []string{"other"}
				r.Update(context.Background(), w)
			}
			step(t, r, w)
			if w.Status.Phase != "Failing" {
				t.Fatal(w.Status)
			}
		})
	}
}
func TestForeignPodNeverDeleted(t *testing.T) {
	r, f, w := fixture(t)
	p := &core.Pod{}
	r.Get(context.Background(), types.NamespacedName{Name: "old", Namespace: "test"}, p)
	p.OwnerReferences = nil
	r.Update(context.Background(), p)
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)})
	if err == nil || f.cleanups != 0 {
		t.Fatal("identity collision was not rejected")
	}
}

func TestDeletionRetainsFinalizerUntilRuntimeCleanup(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	if err := r.Delete(ctx, w); err != nil {
		t.Fatal(err)
	}
	step(t, r, w)
	if w.Status.Phase != "Deleting" {
		t.Fatal(w.Status)
	}
	f.cleanupError = errors.New("still running")
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)}); err == nil {
		t.Fatal("cleanup error hidden")
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal("finalizer released early", err)
	}
	if !controllerutil.ContainsFinalizer(w, api.Finalizer) {
		t.Fatal("missing finalizer")
	}
	f.cleanupError = nil
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)}); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(w), &api.ResumablePod{}); client.IgnoreNotFound(err) != nil || err == nil {
		t.Fatal("CR not deleted", err)
	}
}

func TestDeletionWaitsForOwnedServiceAndPreservesForeignService(t *testing.T) {
	for _, ownedService := range []bool{true, false} {
		name := "foreign"
		if ownedService {
			name = "owned"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			r, _, w := fixture(t)
			pod := &core.Pod{}
			if err := r.Get(ctx, types.NamespacedName{Name: "old", Namespace: w.Namespace}, pod); err != nil {
				t.Fatal(err)
			}
			if err := r.Delete(ctx, pod); err != nil {
				t.Fatal(err)
			}
			w.Status.Phase = "Deleting"
			if err := r.Status().Update(ctx, w); err != nil {
				t.Fatal(err)
			}
			svc := &core.Service{ObjectMeta: meta.ObjectMeta{Name: w.Name, Namespace: w.Namespace, UID: "service-uid", Finalizers: []string{"test.example/hold"}}}
			if ownedService {
				if err := controllerutil.SetControllerReference(w, svc, r.Scheme()); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.Create(ctx, svc); err != nil {
				t.Fatal(err)
			}
			if err := r.Delete(ctx, w); err != nil {
				t.Fatal(err)
			}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)}
			result, err := r.Reconcile(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if err = r.Get(ctx, client.ObjectKeyFromObject(svc), svc); err != nil {
				t.Fatal("Service disappeared before its finalizer completed", err)
			}
			if !ownedService {
				if svc.DeletionTimestamp != nil {
					t.Fatal("foreign Service was deleted")
				}
			} else {
				if result.RequeueAfter == 0 || svc.DeletionTimestamp == nil {
					t.Fatal("owned Service deletion must be requested and requeued")
				}
				for i := 0; i < 2; i++ {
					step(t, r, w)
					if !controllerutil.ContainsFinalizer(w, api.Finalizer) {
						t.Fatal("workload finalizer released while Service is terminating")
					}
				}
				svc.Finalizers = nil
				if err = r.Update(ctx, svc); err != nil {
					t.Fatal(err)
				}
				if _, err = r.Reconcile(ctx, req); err != nil {
					t.Fatal(err)
				}
			}
			if err = r.Get(ctx, client.ObjectKeyFromObject(w), &api.ResumablePod{}); err == nil || client.IgnoreNotFound(err) != nil {
				t.Fatal("workload not deleted after its owned resources were removed", err)
			}
		})
	}
}

func TestInventoryFailureKeepsPausePending(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	pod := &core.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Name: "old", Namespace: "test"}, pod); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, pod); err != nil {
		t.Fatal(err)
	}
	w.Spec.DesiredState = "Suspended"
	if err := r.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase = "Suspending"
	w.Status.Snapshot = "checkpoint-1"
	if err := r.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	f.inventoryError = lifecycle.ErrRetryableStorage
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)})
	if !errors.Is(err, lifecycle.ErrRetryableStorage) {
		t.Fatalf("expected storage failure: %v", err)
	}
	if err = r.Get(ctx, client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
	if w.Status.Phase != "Suspending" {
		t.Fatalf("pause exposed before index publication: %s", w.Status.Phase)
	}
	f.inventoryError = nil
	step(t, r, w)
	if w.Status.Phase != "Suspended" {
		t.Fatalf("pause did not complete after index recovery: %s", w.Status.Phase)
	}
}
