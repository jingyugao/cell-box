package controller

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/runtimecontrol"
	"cellbox.local/cellbox/internal/warmtemplates"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	_ "modernc.org/sqlite"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type localResumeRejectWrites struct {
	client.Client
	attempts     int
	readsBlocked bool
	readAttempts int
}

func (c *localResumeRejectWrites) reject(operation string) error {
	c.attempts++
	return errors.New("unexpected raw Kubernetes " + operation)
}
func (c *localResumeRejectWrites) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if c.readsBlocked {
		c.readAttempts++
		return errors.New("unexpected raw Kubernetes Get")
	}
	return c.Client.Get(ctx, key, obj, opts...)
}
func (c *localResumeRejectWrites) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if c.readsBlocked {
		c.readAttempts++
		return errors.New("unexpected raw Kubernetes List")
	}
	return c.Client.List(ctx, list, opts...)
}
func (c *localResumeRejectWrites) Create(context.Context, client.Object, ...client.CreateOption) error {
	return c.reject("Create")
}
func (c *localResumeRejectWrites) Update(context.Context, client.Object, ...client.UpdateOption) error {
	return c.reject("Update")
}
func (c *localResumeRejectWrites) Patch(context.Context, client.Object, client.Patch, ...client.PatchOption) error {
	return c.reject("Patch")
}
func (c *localResumeRejectWrites) Delete(context.Context, client.Object, ...client.DeleteOption) error {
	return c.reject("Delete")
}
func (c *localResumeRejectWrites) Status() client.SubResourceWriter {
	return localResumeRejectStatus{SubResourceWriter: c.Client.Status(), parent: c}
}

type localResumeRejectStatus struct {
	client.SubResourceWriter
	parent *localResumeRejectWrites
}

func (s localResumeRejectStatus) Update(context.Context, client.Object, ...client.SubResourceUpdateOption) error {
	return s.parent.reject("status Update")
}

type localResumeWarmRuntime struct {
	*fakeRuntime
	deadline      time.Time
	waitNil       int
	activateCalls int
	lastActivated types.UID
}

func (*localResumeWarmRuntime) RegisterWarm(context.Context, *core.Pod, string) error { return nil }
func (r *localResumeWarmRuntime) WarmState(context.Context, *core.Pod) (string, time.Time, error) {
	return "waiting", r.deadline, nil
}
func (r *localResumeWarmRuntime) ActivateWarm(_ context.Context, _ *api.ResumablePod, p *core.Pod) (bool, error) {
	r.activateCalls++
	r.lastActivated = p.UID
	return true, nil
}
func (*localResumeWarmRuntime) ReapWarm(context.Context, string, map[string]bool) error { return nil }
func (r *localResumeWarmRuntime) WaitWarmStarted(_ context.Context, _ *api.ResumablePod, p *core.Pod) (*api.Execution, error) {
	if r.waitNil > 0 {
		r.waitNil--
		return nil, nil
	}
	return &api.Execution{PodUID: string(p.UID), ContainerID: "container-1", PodIP: "10.0.0.8"}, nil
}
func (*localResumeWarmRuntime) InvalidateSnapshot(context.Context, *api.ResumablePod) error {
	return nil
}
func (*localResumeWarmRuntime) ResumeGuest(_ context.Context, w *api.ResumablePod, p *core.Pod) error {
	if w.Status.Execution == nil || w.Status.Execution.PodUID != string(p.UID) {
		return errors.New("guest execution was not confirmed")
	}
	return nil
}

func TestWarmResumeReconcilesEntirelyFromLocalJournal(t *testing.T) {
	t.Run("immediate", testWarmResumeImmediate)
	t.Run("restart-while-restoring", testWarmResumeRestartWhileRestoring)
}

func testWarmResumeImmediate(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cellbox.db")
	dsn := "file:" + dbPath + "?" + url.Values{"_pragma": {"busy_timeout(5000)"}, "_txlock": {"immediate"}}.Encode()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE runtime_records (
        kind TEXT NOT NULL, id TEXT NOT NULL, client_id TEXT NOT NULL DEFAULT '',
        box_id TEXT NOT NULL DEFAULT '', record TEXT NOT NULL, PRIMARY KEY(kind,id))`); err != nil {
		t.Fatal(err)
	}

	scheme := runtime.NewScheme()
	if err := core.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{TypeMeta: meta.TypeMeta{APIVersion: api.GroupVersion.String(), Kind: api.Kind},
		ObjectMeta: meta.ObjectMeta{Name: "cellbox-local", Namespace: "test", UID: types.UID("box-uid-123"), Finalizers: []string{api.Finalizer},
			Labels: map[string]string{"cellbox.local/managed": "true", "cellbox.local/box-id": "local"}},
		Spec: api.Spec{NodeName: "node", DesiredState: "Suspended", TransitionID: "before-resume",
			Container: core.Container{Name: "server", Image: "image:local", ImagePullPolicy: core.PullNever}},
		Status: api.Status{Phase: "Suspended", ObservedTransitionID: "before-resume", Snapshot: "checkpoint-7", Cycle: 7}}
	w.Status.SpecHash = fingerprint(w)
	warm := warmExecutionPod(w, "warm-local")
	warm.UID = types.UID("warm-pod-uid")
	warm.Spec.SchedulingGates = nil
	warm.Labels = map[string]string{warmLabel: "true", api.ServingLabel: "false"}
	warm.Annotations = map[string]string{warmSpec: w.Status.SpecHash}
	warm.Status = core.PodStatus{Phase: core.PodPending, PodIP: "10.0.0.8"}

	rawFake := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.ResumablePod{}, &core.Pod{}).WithObjects(w, warm).Build()
	rejecting := &localResumeRejectWrites{Client: rawFake}
	local := runtimecontrol.New(rejecting)
	journal := runtimecontrol.NewJournal(db)
	if err := local.SetJournal(ctx, journal); err != nil {
		t.Fatal(err)
	}
	baseline := &api.ResumablePod{}
	key := client.ObjectKeyFromObject(w)
	if err := local.Get(ctx, key, baseline); err != nil {
		t.Fatal(err)
	}
	handle := boxprovider.Handle{Provider: "resumable-k8s-pod", ID: string(w.UID), Name: w.Name, Namespace: w.Namespace, NodeName: w.Spec.NodeName}
	if err := local.RequestResume(ctx, handle, "before-resume", "resume-7"); err != nil {
		t.Fatal(err)
	}
	// The normal baseline read above is the final Kubernetes access. Reconcile
	// must use the journal and the controller's cached warm slot from here on.
	rejecting.readsBlocked = true

	runtimeBackend := &localResumeWarmRuntime{fakeRuntime: &fakeRuntime{}, deadline: time.Now().Add(time.Minute)}
	r := &Reconciler{Client: local, Runtime: runtimeBackend, NodeName: "node"}
	r.WarmPool = &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 1,
		ReadDesired: func(context.Context) ([]warmtemplates.Template, string, error) {
			return []warmtemplates.Template{{ID: "template-local", Slots: 1, Spec: w.Spec}}, "template-hash", nil
		},
		cachedPods: []core.Pod{*warm.DeepCopy()},
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}

	localResult := &api.ResumablePod{}
	if err := local.Get(ctx, key, localResult); err != nil {
		t.Fatal(err)
	}
	if localResult.Status.Phase != "Running" || localResult.Spec.DesiredState != "Running" || localResult.Status.PodUID != string(warm.UID) {
		t.Fatalf("local reconcile did not complete Running: spec=%s phase=%s podUID=%s", localResult.Spec.DesiredState, localResult.Status.Phase, localResult.Status.PodUID)
	}
	if localResult.Status.ResumeReadyTransitionID != "resume-7" || localResult.Status.ObservedTransitionID != "resume-7" {
		t.Fatal("completion preceded guest reopening")
	}
	if err := r.WarmPool.removeIdle(ctx, warm); err != nil {
		t.Fatalf("maintenance reclaimed a locally active slot: %v", err)
	}
	localRecord, err := local.LocalRecord(ctx, key)
	if err != nil || localRecord == nil || !localRecord.Complete {
		t.Fatalf("local completion record missing: record=%#v err=%v", localRecord, err)
	}
	localPod := localRecord.Pod
	if localPod == nil {
		t.Fatal("local completion record lost its warm Pod")
	}
	owner := meta.GetControllerOf(localPod)
	if owner == nil || owner.UID != w.UID || localPod.Labels[api.OwnerLabel] != string(w.UID) || localPod.Labels[api.ServingLabel] != "true" {
		t.Fatalf("local Pod record missing owner/serving: pod=%#v owner=%#v", localPod, owner)
	}
	rawCR := &api.ResumablePod{}
	if err := rawFake.Get(ctx, key, rawCR); err != nil {
		t.Fatal(err)
	}
	rawPod := &core.Pod{}
	if err := rawFake.Get(ctx, client.ObjectKeyFromObject(warm), rawPod); err != nil {
		t.Fatal(err)
	}
	if rawCR.Spec.DesiredState != "Suspended" || rawCR.Status.Phase != "Suspended" || rawCR.Spec.TransitionID != "before-resume" {
		t.Fatalf("raw Cellbox changed before Sync: desired=%s phase=%s transition=%s", rawCR.Spec.DesiredState, rawCR.Status.Phase, rawCR.Spec.TransitionID)
	}
	if len(rawPod.OwnerReferences) != 0 || rawPod.Labels[api.ServingLabel] != "false" || rawPod.Labels[api.OwnerLabel] != "" {
		t.Fatalf("raw warm slot changed before Sync: owners=%v labels=%v", rawPod.OwnerReferences, rawPod.Labels)
	}
	if rejecting.attempts != 0 {
		t.Fatalf("controller attempted %d raw Kubernetes writes", rejecting.attempts)
	}
	if rejecting.readAttempts != 0 {
		t.Fatalf("controller attempted %d raw Kubernetes reads after baseline seeding", rejecting.readAttempts)
	}
	if runtimeBackend.prepares != 0 {
		t.Fatalf("warm resume unexpectedly prepared a new Pod %d times", runtimeBackend.prepares)
	}
}

func testWarmResumeRestartWhileRestoring(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "cellbox.db")
	dsn := "file:" + dbPath + "?" + url.Values{"_pragma": {"busy_timeout(5000)"}, "_txlock": {"immediate"}}.Encode()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(`CREATE TABLE runtime_records (
        kind TEXT NOT NULL, id TEXT NOT NULL, client_id TEXT NOT NULL DEFAULT '',
        box_id TEXT NOT NULL DEFAULT '', record TEXT NOT NULL, PRIMARY KEY(kind,id))`); err != nil {
		t.Fatal(err)
	}

	scheme := runtime.NewScheme()
	if err := core.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{TypeMeta: meta.TypeMeta{APIVersion: api.GroupVersion.String(), Kind: api.Kind},
		ObjectMeta: meta.ObjectMeta{Name: "cellbox-restart", Namespace: "test", UID: types.UID("box-restart-123"), Finalizers: []string{api.Finalizer},
			Labels: map[string]string{"cellbox.local/managed": "true", "cellbox.local/box-id": "restart"}},
		Spec: api.Spec{NodeName: "node", DesiredState: "Suspended", TransitionID: "before-resume",
			Container: core.Container{Name: "server", Image: "image:local", ImagePullPolicy: core.PullNever}},
		Status: api.Status{Phase: "Suspended", ObservedTransitionID: "before-resume", Snapshot: "checkpoint-7", Cycle: 7}}
	w.Status.SpecHash = fingerprint(w)
	warm := warmExecutionPod(w, "warm-restart")
	warm.UID = types.UID("warm-restart-uid")
	warm.Spec.SchedulingGates = nil
	warm.Labels = map[string]string{warmLabel: "true", api.ServingLabel: "false"}
	warm.Annotations = map[string]string{warmSpec: w.Status.SpecHash}
	warm.Status = core.PodStatus{Phase: core.PodPending, PodIP: "10.0.0.8"}
	rawFake := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.ResumablePod{}, &core.Pod{}).WithObjects(w, warm).Build()
	rejecting := &localResumeRejectWrites{Client: rawFake}
	firstClient := runtimecontrol.New(rejecting)
	if err := firstClient.SetJournal(ctx, runtimecontrol.NewJournal(db)); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(w)
	baseline := &api.ResumablePod{}
	if err := firstClient.Get(ctx, key, baseline); err != nil {
		t.Fatal(err)
	}
	handle := boxprovider.Handle{Provider: "resumable-k8s-pod", ID: string(w.UID), Name: w.Name, Namespace: w.Namespace, NodeName: w.Spec.NodeName}
	if err := firstClient.RequestResume(ctx, handle, "before-resume", "resume-7"); err != nil {
		t.Fatal(err)
	}
	rejecting.readsBlocked = true

	firstRuntime := &localResumeWarmRuntime{fakeRuntime: &fakeRuntime{}, deadline: time.Now().Add(time.Minute), waitNil: 1}
	firstReconciler := &Reconciler{Client: firstClient, Runtime: firstRuntime, NodeName: "node"}
	firstReconciler.WarmPool = &WarmPool{Reconciler: firstReconciler, Namespace: w.Namespace, Size: 1,
		ReadDesired: func(context.Context) ([]warmtemplates.Template, string, error) {
			return []warmtemplates.Template{{ID: "template-restart", Slots: 1, Spec: w.Spec}}, "template-hash", nil
		},
		cachedPods: []core.Pod{*warm.DeepCopy()},
	}
	if _, err := firstReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	localRestoring := &api.ResumablePod{}
	if err := firstClient.Get(ctx, key, localRestoring); err != nil {
		t.Fatal(err)
	}
	if localRestoring.Status.Phase != "Restoring" || localRestoring.Status.Cycle != 8 || localRestoring.Status.PodUID != string(warm.UID) {
		t.Fatalf("first reconcile did not persist the claimed Restoring state: phase=%s cycle=%d podUID=%s", localRestoring.Status.Phase, localRestoring.Status.Cycle, localRestoring.Status.PodUID)
	}
	firstRecord, err := firstClient.LocalRecord(ctx, key)
	if err != nil || firstRecord == nil || firstRecord.Complete || firstRecord.Pod == nil || firstRecord.Pod.UID != warm.UID {
		t.Fatalf("incomplete restore claim was not durable: record=%#v err=%v", firstRecord, err)
	}
	if firstRuntime.activateCalls != 1 || firstRuntime.lastActivated != warm.UID {
		t.Fatalf("first attempt activation count=%d podUID=%s", firstRuntime.activateCalls, firstRuntime.lastActivated)
	}

	// Simulate a controller process restart before the next retry. The second
	// controller has no warm-pool cache and must resume the durable assignment.
	secondClient := runtimecontrol.New(rejecting)
	if err := secondClient.SetJournal(ctx, runtimecontrol.NewJournal(db)); err != nil {
		t.Fatal(err)
	}
	secondRuntime := &localResumeWarmRuntime{fakeRuntime: &fakeRuntime{}, deadline: time.Now().Add(time.Minute)}
	secondReconciler := &Reconciler{Client: secondClient, Runtime: secondRuntime, NodeName: "node"}
	secondReconciler.WarmPool = &WarmPool{Reconciler: secondReconciler, Namespace: w.Namespace, Size: 1,
		ReadDesired: func(context.Context) ([]warmtemplates.Template, string, error) {
			return []warmtemplates.Template{{ID: "template-restart", Slots: 1, Spec: w.Spec}}, "template-hash", nil
		},
		cachedPods: nil,
	}
	if _, err := secondReconciler.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	completed := &api.ResumablePod{}
	if err := secondClient.Get(ctx, key, completed); err != nil {
		t.Fatal(err)
	}
	if completed.Status.Phase != "Running" || completed.Status.Cycle != 8 || completed.Status.PodUID != string(warm.UID) {
		t.Fatalf("restart did not finish the same restore: phase=%s cycle=%d podUID=%s", completed.Status.Phase, completed.Status.Cycle, completed.Status.PodUID)
	}
	if completed.Status.ResumeReadyTransitionID != completed.Spec.TransitionID {
		t.Fatal("restart completed before reopening the guest")
	}
	completedRecord, err := secondClient.LocalRecord(ctx, key)
	if err != nil || completedRecord == nil || !completedRecord.Complete || completedRecord.Pod == nil || completedRecord.Pod.UID != warm.UID {
		t.Fatalf("restart completion lost durable Pod claim: record=%#v err=%v", completedRecord, err)
	}
	if secondRuntime.lastActivated != warm.UID {
		t.Fatalf("restart activated a different Pod UID: %s", secondRuntime.lastActivated)
	}
	if rejecting.attempts != 0 || rejecting.readAttempts != 0 {
		t.Fatalf("restart reconciliation used Kubernetes: writes=%d reads=%d", rejecting.attempts, rejecting.readAttempts)
	}
	rawCR := &api.ResumablePod{}
	if err := rawFake.Get(ctx, key, rawCR); err != nil {
		t.Fatal(err)
	}
	rawPod := &core.Pod{}
	if err := rawFake.Get(ctx, client.ObjectKeyFromObject(warm), rawPod); err != nil {
		t.Fatal(err)
	}
	if rawCR.Spec.DesiredState != "Suspended" || rawCR.Status.Phase != "Suspended" || len(rawPod.OwnerReferences) != 0 || rawPod.Labels[api.ServingLabel] != "false" {
		t.Fatalf("restart changed raw Kubernetes objects: CR=%s/%s podOwner=%v podLabels=%v", rawCR.Spec.DesiredState, rawCR.Status.Phase, rawPod.OwnerReferences, rawPod.Labels)
	}
}
