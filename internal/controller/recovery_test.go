package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func recoveryFixture(t *testing.T) (*Reconciler, *fakeRuntime, *api.ResumablePod) {
	t.Helper()
	r, f, w := fixture(t)
	r.RestartOnExecutionLoss = true
	w.Spec.PersistentHome, w.Spec.HomeVolumeUID = true, "retained-volume"
	if err := r.Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	w.Status.SpecHash = fingerprint(w)
	if err := r.Status().Update(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	return r, f, w
}

func TestExecutionRecoveryPreservesIdentityAndWaitsForCleanup(t *testing.T) {
	r, f, w := recoveryFixture(t)
	oldUID := w.UID
	p := &core.Pod{}
	if err := r.Get(t.Context(), client.ObjectKey{Namespace: w.Namespace, Name: "old"}, p); err != nil {
		t.Fatal(err)
	}
	p.Status.Phase = core.PodFailed
	if err := r.Status().Update(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	step(t, r, w)
	if w.Status.Phase != "Recovering" || w.Status.PodUID != "old-uid" {
		t.Fatal(w.Status)
	}
	f.cleanupError = errors.New("CRI temporarily unavailable")
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(w)}); err == nil {
		t.Fatal("cleanup failure ignored")
	}
	if err := r.Get(t.Context(), client.ObjectKeyFromObject(w), w); err != nil {
		t.Fatal(err)
	}
	if w.Status.Cycle != 1 {
		t.Fatal("replacement allocated before cleanup")
	}
	f.cleanupError = nil
	// A new controller resumes the durable recovery without another attempt.
	r = &Reconciler{Client: r.Client, Runtime: f, NodeName: r.NodeName, RestartOnExecutionLoss: true}
	step(t, r, w)
	if w.Status.Phase != "Creating" || w.Status.Cycle != 2 || w.Status.RecoveryAttempts != 1 || w.UID != oldUID || w.Spec.HomeVolumeUID != "retained-volume" || w.Status.Execution != nil || w.Status.Snapshot != "" {
		t.Fatalf("incorrect recovered assignment: %+v", w)
	}
	step(t, r, w)
	newPod := &core.Pod{}
	if err := r.Get(t.Context(), client.ObjectKey{Namespace: w.Namespace, Name: w.Status.PodName}, newPod); err != nil {
		t.Fatal(err)
	}
	if newPod.Spec.Volumes[0].HostPath.Path != "/var/lib/cellbox/homes/retained-volume/data" {
		t.Fatal(newPod.Spec.Volumes)
	}
}

func TestExecutionRecoveryPolicyAndBudget(t *testing.T) {
	r, _, w := recoveryFixture(t)
	if !r.canRecoverExecution(w) {
		t.Fatal("persistent execution not recoverable")
	}
	w.Status.RecoveryAttempts, w.Status.LastRecoveryTime = maxRecoveryAttempts, meta.Now()
	if r.canRecoverExecution(w) {
		t.Fatal("recovery budget ignored")
	}
	w.Status.LastRecoveryTime = meta.NewTime(time.Now().Add(-recoveryWindow - time.Second))
	if !r.canRecoverExecution(w) {
		t.Fatal("recovery budget did not expire")
	}
	w.Status.Snapshot = "unconsumed-checkpoint"
	if r.canRecoverExecution(w) {
		t.Fatal("checkpoint would be rolled back")
	}
	w.Status.Snapshot = ""
	w.Spec.DesiredState = "Suspended"
	if r.canRecoverExecution(w) {
		t.Fatal("suspend ignored")
	}
	w.Spec.DesiredState = "Running"
	w.Spec.PersistentHome = false
	if r.canRecoverExecution(w) {
		t.Fatal("ephemeral execution restarted")
	}
}

func TestSuspendDuringExecutionRecoveryCancelsColdStart(t *testing.T) {
	r, _, w := recoveryFixture(t)
	if err := r.Delete(context.Background(), &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "old", Namespace: "test"}}); err != nil {
		t.Fatal(err)
	}
	step(t, r, w)
	desired(t, r, w, "Suspended")
	step(t, r, w)
	if w.Status.Phase != "Failed" || w.Status.Cycle != 1 {
		t.Fatal(w.Status)
	}
}
