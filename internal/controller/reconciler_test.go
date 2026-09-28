// SPDX-License-Identifier: Apache-2.0

package controller

import (
	api "cellbox.local/cellbox/api/v1alpha1"
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

type fakeRuntime struct {
	checkpoints, prepares, cleanups int
	prepareError, cleanupError      error
}

func (f *fakeRuntime) Checkpoint(context.Context, *api.ResumablePod, *core.Pod) error {
	f.checkpoints++
	return nil
}
func (f *fakeRuntime) Prepare(context.Context, *api.ResumablePod, *core.Pod) error {
	f.prepares++
	return f.prepareError
}
func (f *fakeRuntime) Cleanup(context.Context, *core.Pod) error        { f.cleanups++; return f.cleanupError }
func (f *fakeRuntime) Forget(context.Context, *api.ResumablePod) error { return nil }
func fixture(t *testing.T) (*Reconciler, *fakeRuntime, *api.ResumablePod) {
	t.Helper()
	s := runtime.NewScheme()
	core.AddToScheme(s)
	api.AddToScheme(s)
	w := &api.ResumablePod{TypeMeta: meta.TypeMeta{APIVersion: api.GroupVersion.String(), Kind: "ResumablePod"}, ObjectMeta: meta.ObjectMeta{Name: "counter", Namespace: "test", UID: "12345678-aaaa", Finalizers: []string{api.Finalizer}}, Spec: api.Spec{NodeName: "node", DesiredState: "Running", Container: core.Container{Name: "server", Image: "image:local", ImagePullPolicy: core.PullNever}}, Status: api.Status{Phase: "Running", PodName: "old", PodUID: "old-uid", Cycle: 1, Since: meta.Now()}}
	w.Status.SpecHash = fingerprint(w)
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "old", Namespace: "test", UID: "old-uid"}, Spec: core.PodSpec{Containers: []core.Container{w.Spec.Container}}, Status: core.PodStatus{Phase: core.PodRunning, Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue}}}}
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
	p.Status = core.PodStatus{Phase: core.PodRunning, Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue}}}
	r.Status().Update(ctx, p)
	step(t, r, w)
	if w.Status.Phase != "Running" || w.Status.Snapshot != "" {
		t.Fatal("old snapshot must not be replayable after readiness", w.Status)
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
