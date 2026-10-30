package runtimecontrol

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/boxprovider"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type rejectWrites struct{ client.Client }

func (c *rejectWrites) Create(context.Context, client.Object, ...client.CreateOption) error {
	return errors.New("unexpected raw Create")
}
func (c *rejectWrites) Update(context.Context, client.Object, ...client.UpdateOption) error {
	return errors.New("unexpected raw Update")
}
func (c *rejectWrites) Patch(context.Context, client.Object, client.Patch, ...client.PatchOption) error {
	return errors.New("unexpected raw Patch")
}
func (c *rejectWrites) Delete(context.Context, client.Object, ...client.DeleteOption) error {
	return errors.New("unexpected raw Delete")
}
func (c *rejectWrites) Status() client.SubResourceWriter {
	return rejectStatusWriter{SubResourceWriter: c.Client.Status()}
}

type rejectStatusWriter struct{ client.SubResourceWriter }

func (w rejectStatusWriter) Update(context.Context, client.Object, ...client.SubResourceUpdateOption) error {
	return errors.New("unexpected raw status Update")
}

func localFixture(t *testing.T, objects ...client.Object) (*Client, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := core.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	raw := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&api.ResumablePod{}).WithObjects(objects...).Build()
	return New(raw), raw
}

func localResumeObjects(name string, uid types.UID) (*api.ResumablePod, *core.Pod) {
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{Name: name, Namespace: "test", UID: uid},
		Spec:   api.Spec{DesiredState: "Suspended", NodeName: "node", TransitionID: "previous"},
		Status: api.Status{Phase: "Suspended", ObservedTransitionID: "previous"}}
	w.Labels = map[string]string{"cellbox.jingyugao.github.io/managed": "true", "cellbox.jingyugao.github.io/box-id": name}
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "warm-" + name, Namespace: "test", UID: types.UID("pod-" + name),
		Labels: map[string]string{api.ServingLabel: "false"}},
		Spec: core.PodSpec{NodeName: "node"}}
	return w, pod
}

func localHandle(w *api.ResumablePod) boxprovider.Handle {
	return boxprovider.Handle{Provider: "resumable-k8s-pod", ID: string(w.UID), Name: w.Name, Namespace: w.Namespace, NodeName: w.Spec.NodeName}
}

func configureJournal(t *testing.T, c *Client, journal *Journal) {
	t.Helper()
	if err := c.SetJournal(t.Context(), journal); err != nil {
		t.Fatal(err)
	}
}

func beginLocalResume(t *testing.T, c *Client, w *api.ResumablePod, pod *core.Pod) {
	t.Helper()
	if err := c.RequestResume(t.Context(), localHandle(w), "previous", "resume-1"); err != nil {
		t.Fatal(err)
	}
	current := &api.ResumablePod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(w), current); err != nil {
		t.Fatal(err)
	}
	handled, err := c.ClaimWarm(t.Context(), current, pod)
	if err != nil || !handled {
		t.Fatalf("ClaimWarm handled=%v err=%v", handled, err)
	}
	before := pod.DeepCopy()
	pod.Labels[api.ServingLabel] = "true"
	if err := c.Patch(t.Context(), pod, client.MergeFrom(before)); err != nil {
		t.Fatal(err)
	}
	local, err := c.LocalRecord(t.Context(), client.ObjectKeyFromObject(w))
	if err != nil || local == nil || local.Resource.Status.Phase != "Restoring" {
		t.Fatalf("local restore state=%#v err=%v", local, err)
	}
	local.Resource.Status.Phase = "Running"
	local.Resource.Status.PodName = pod.Name
	local.Resource.Status.PodUID = string(pod.UID)
	if err := c.Status().Update(t.Context(), local.Resource); err != nil {
		t.Fatal(err)
	}
}

func TestLocalResumeCompletesWithoutRawKubernetesWrites(t *testing.T) {
	_, journal, _ := openTestJournal(t)
	w, pod := localResumeObjects("one", "uid-one")
	baseClient, _ := localFixture(t, w, pod)
	configureJournal(t, baseClient, journal)
	// A successful baseline Get seeds the local journal before writes are blocked.
	got := &api.ResumablePod{}
	if err := baseClient.Get(t.Context(), client.ObjectKeyFromObject(w), got); err != nil {
		t.Fatal(err)
	}
	baseClient.Client = &rejectWrites{Client: baseClient.Client}

	ch, stop, err := baseClient.Subscribe(t.Context(), client.ObjectKeyFromObject(w))
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	_ = next(t, ch)
	beginLocalResume(t, baseClient, got, pod)
	completed, err := baseClient.LocalRecord(t.Context(), client.ObjectKeyFromObject(w))
	if err != nil || completed == nil || completed.Resource.Status.ResumePath != api.ResumePathWarmPool {
		t.Fatalf("warm resume path was not retained: record=%#v err=%v", completed, err)
	}
	select {
	case event := <-ch:
		if event.Resource == nil || event.Resource.Status.Phase != "Running" {
			t.Fatalf("subscriber received non-running local state: %#v", event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("local Running state was not published")
	}
	rawCR := &api.ResumablePod{}
	if err := baseClient.Client.(*rejectWrites).Client.Get(t.Context(), client.ObjectKeyFromObject(w), rawCR); err != nil {
		t.Fatal(err)
	}
	rawPod := &core.Pod{}
	if err := baseClient.Client.(*rejectWrites).Client.Get(t.Context(), client.ObjectKeyFromObject(pod), rawPod); err != nil {
		t.Fatal(err)
	}
	if rawCR.Spec.DesiredState != "Suspended" || rawCR.Status.Phase != "Suspended" {
		t.Fatalf("raw CR changed during local resume: spec=%s phase=%s", rawCR.Spec.DesiredState, rawCR.Status.Phase)
	}
	if len(rawPod.OwnerReferences) != 0 {
		t.Fatalf("raw warm Pod was unexpectedly assigned: %#v", rawPod.OwnerReferences)
	}
}

func TestRequestResumeClearsPreviousResumePath(t *testing.T) {
	_, journal, _ := openTestJournal(t)
	w, _ := localResumeObjects("new-transition", "uid-new-transition")
	w.Status.ResumePath = api.ResumePathWarmPool
	c, _ := localFixture(t, w)
	configureJournal(t, c, journal)
	got := &api.ResumablePod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(w), got); err != nil {
		t.Fatal("failed to seed local journal")
	}
	if err := c.RequestResume(t.Context(), localHandle(w), "previous", "resume-next"); err != nil {
		t.Fatal(err)
	}
	current := &api.ResumablePod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(w), current); err != nil {
		t.Fatal(err)
	}
	if current.Status.ResumePath != "" {
		t.Fatalf("new resume retained previous path %q", current.Status.ResumePath)
	}
}

func TestLocalResumeRecoversAndSyncsAfterClientRestart(t *testing.T) {
	db, journal, _ := openTestJournal(t)
	defer db.Close()
	w, pod := localResumeObjects("restart", "uid-restart")
	first, raw := localFixture(t, w, pod)
	configureJournal(t, first, journal)
	baseline := &api.ResumablePod{}
	if err := first.Get(t.Context(), client.ObjectKeyFromObject(w), baseline); err != nil {
		t.Fatal(err)
	}
	beginLocalResume(t, first, baseline, pod)

	second := New(raw)
	configureJournal(t, second, NewJournal(db))
	recovered := &api.ResumablePod{}
	if err := second.Get(t.Context(), client.ObjectKeyFromObject(w), recovered); err != nil {
		t.Fatal(err)
	}
	if recovered.Status.Phase != "Running" || recovered.Spec.DesiredState != "Running" {
		t.Fatalf("restart did not recover local Running state: spec=%s phase=%s", recovered.Spec.DesiredState, recovered.Status.Phase)
	}
	if recovered.Status.ResumePath != api.ResumePathWarmPool {
		t.Fatalf("restart lost warm resume path: %q", recovered.Status.ResumePath)
	}
	if err := second.Sync(t.Context(), client.ObjectKeyFromObject(w)); err != nil {
		t.Fatal(err)
	}
	if err := second.Sync(t.Context(), client.ObjectKeyFromObject(w)); err != nil {
		t.Fatalf("repeated Sync was not idempotent: %v", err)
	}
	rawCR := &api.ResumablePod{}
	if err := raw.Get(t.Context(), client.ObjectKeyFromObject(w), rawCR); err != nil {
		t.Fatal(err)
	}
	rawPod := &core.Pod{}
	if err := raw.Get(t.Context(), client.ObjectKeyFromObject(pod), rawPod); err != nil {
		t.Fatal(err)
	}
	if rawCR.Spec.DesiredState != "Running" || rawCR.Status.Phase != "Running" {
		t.Fatalf("Sync did not publish CR state: spec=%s phase=%s", rawCR.Spec.DesiredState, rawCR.Status.Phase)
	}
	if len(rawPod.OwnerReferences) != 1 || rawPod.OwnerReferences[0].UID != w.UID || rawPod.Labels[api.OwnerLabel] != string(w.UID) || rawPod.Labels[api.ServingLabel] != "true" {
		t.Fatalf("Sync did not publish Pod ownership/serving state: owner=%#v labels=%v", rawPod.OwnerReferences, rawPod.Labels)
	}
}

func TestLocalResumeClaimIsExclusiveAcrossCellboxes(t *testing.T) {
	_, journal, _ := openTestJournal(t)
	w1, pod := localResumeObjects("first", "uid-first")
	w2, _ := localResumeObjects("second", "uid-second")
	c, _ := localFixture(t, w1, w2, pod)
	configureJournal(t, c, journal)
	for _, w := range []*api.ResumablePod{w1, w2} {
		got := &api.ResumablePod{}
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(w), got); err != nil {
			t.Fatal(err)
		}
		if err := c.RequestResume(t.Context(), localHandle(got), "previous", fmt.Sprintf("resume-%s", w.Name)); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(t.Context(), client.ObjectKeyFromObject(w), got); err != nil {
			t.Fatal(err)
		}
	}
	current1, current2 := &api.ResumablePod{}, &api.ResumablePod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(w1), current1); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(w2), current2); err != nil {
		t.Fatal(err)
	}
	secondCandidate := pod.DeepCopy() // Simulate a fresh raw Pod read before applying the local owner projection.
	if handled, err := c.ClaimWarm(t.Context(), current1, pod); err != nil || !handled {
		t.Fatalf("first claim handled=%v err=%v", handled, err)
	}
	if handled, err := c.ClaimWarm(t.Context(), current2, secondCandidate); !handled || !errors.Is(err, ErrPodClaimed) {
		t.Fatalf("second claim handled=%v err=%v, want ErrPodClaimed", handled, err)
	}
}

func TestLocalResumeSyncDoesNotOverwriteReplacementUID(t *testing.T) {
	_, journal, _ := openTestJournal(t)
	original, pod := localResumeObjects("replaced", "old-uid")
	oldClient, _ := localFixture(t, original, pod)
	configureJournal(t, oldClient, journal)
	baseline := &api.ResumablePod{}
	if err := oldClient.Get(t.Context(), client.ObjectKeyFromObject(original), baseline); err != nil {
		t.Fatal(err)
	}
	beginLocalResume(t, oldClient, baseline, pod)

	replacement, replacementPod := localResumeObjects("replaced", "new-uid")
	newClient, raw := localFixture(t, replacement, replacementPod)
	configureJournal(t, newClient, journal)
	_ = newClient.Sync(t.Context(), client.ObjectKeyFromObject(replacement))
	stored := &api.ResumablePod{}
	if err := raw.Get(t.Context(), client.ObjectKeyFromObject(replacement), stored); err != nil {
		t.Fatal(err)
	}
	if stored.UID != "new-uid" || stored.Spec.DesiredState != "Suspended" || stored.Status.Phase != "Suspended" {
		t.Fatalf("replacement was overwritten: uid=%s spec=%s phase=%s", stored.UID, stored.Spec.DesiredState, stored.Status.Phase)
	}
}

func TestLostLocalExecutionFailsAndSyncLeavesReplacementPodUntouched(t *testing.T) {
	_, journal, _ := openTestJournal(t)
	w, pod := localResumeObjects("lost", "uid-lost")
	c, raw := localFixture(t, w, pod)
	configureJournal(t, c, journal)
	baseline := &api.ResumablePod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(w), baseline); err != nil {
		t.Fatal(err)
	}
	beginLocalResume(t, c, baseline, pod)

	q := queue(t, c)
	for q.Len() > 0 {
		request, shutdown := q.Get()
		if shutdown {
			t.Fatal("queue shut down unexpectedly")
		}
		q.Done(request)
		q.Forget(request)
	}
	key := client.ObjectKeyFromObject(w)
	current, err := c.LocalRecord(t.Context(), key)
	if err != nil || current == nil || !current.Complete || current.Resource.Status.Phase != "Running" {
		t.Fatalf("expected completed local Running record: record=%#v err=%v", current, err)
	}
	oldRawPod := &core.Pod{}
	if err := raw.Get(t.Context(), client.ObjectKeyFromObject(pod), oldRawPod); err != nil {
		t.Fatal(err)
	}
	if err := raw.Delete(t.Context(), oldRawPod); err != nil {
		t.Fatal(err)
	}
	replacementPod := oldRawPod.DeepCopy()
	replacementPod.UID = "replacement-pod-uid"
	replacementPod.ResourceVersion = ""
	replacementPod.OwnerReferences = nil
	if err := raw.Create(t.Context(), replacementPod); err != nil {
		t.Fatal(err)
	}
	if err := c.observeExternal(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	if q.Len() != 1 {
		t.Fatalf("lost execution did not notify reconcile queue: queue length=%d", q.Len())
	}
	failed, err := c.LocalRecord(t.Context(), key)
	if err != nil || failed == nil || failed.Complete || failed.Resource.Status.Phase != "Failing" {
		t.Fatalf("lost local execution did not enter Failing: record=%#v err=%v", failed, err)
	}

	failed.Resource.Status.Phase = "Failed"
	if err := c.Status().Update(t.Context(), failed.Resource); err != nil {
		t.Fatal(err)
	}
	if err := c.Sync(t.Context(), key); err != nil {
		t.Fatalf("failed transition Sync: %v", err)
	}
	storedPod := &core.Pod{}
	if err := raw.Get(t.Context(), client.ObjectKeyFromObject(replacementPod), storedPod); err != nil {
		t.Fatal(err)
	}
	if storedPod.UID != replacementPod.UID || len(storedPod.OwnerReferences) != 0 || storedPod.Labels[api.ServingLabel] != "false" || storedPod.Labels[api.OwnerLabel] != "" {
		t.Fatalf("Sync modified same-name replacement Pod: uid=%s owners=%v labels=%v", storedPod.UID, storedPod.OwnerReferences, storedPod.Labels)
	}
	storedCR := &api.ResumablePod{}
	if err := raw.Get(t.Context(), key, storedCR); err != nil {
		t.Fatal(err)
	}
	if storedCR.Status.Phase != "Failed" {
		t.Fatalf("failed status not synced: phase=%s", storedCR.Status.Phase)
	}
}

func TestObserveExternalDeletionAndReplacementAreIdempotent(t *testing.T) {
	for _, mode := range []string{"deleted", "uid-replaced"} {
		t.Run(mode, func(t *testing.T) {
			_, journal, _ := openTestJournal(t)
			w, pod := localResumeObjects("external-"+mode, types.UID("uid-"+mode))
			c, raw := localFixture(t, w, pod)
			configureJournal(t, c, journal)
			baseline := &api.ResumablePod{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(w), baseline); err != nil {
				t.Fatal(err)
			}
			beginLocalResume(t, c, baseline, pod)
			key := client.ObjectKeyFromObject(w)
			rawCR := &api.ResumablePod{}
			if err := raw.Get(t.Context(), key, rawCR); err != nil {
				t.Fatal(err)
			}
			if err := raw.Delete(t.Context(), rawCR); err != nil {
				t.Fatal(err)
			}
			if mode == "uid-replaced" {
				replacement := rawCR.DeepCopy()
				replacement.UID = types.UID("replacement-" + mode)
				replacement.ResourceVersion = ""
				if err := raw.Create(t.Context(), replacement); err != nil {
					t.Fatal(err)
				}
			}
			observed, err := c.LocalRecord(t.Context(), key)
			if err != nil || observed == nil {
				t.Fatalf("missing local record before external observation: %#v err=%v", observed, err)
			}
			beforeRevision := observed.Revision
			if err := c.observeExternal(t.Context(), observed); err != nil {
				t.Fatal(err)
			}
			removed, err := c.LocalRecord(t.Context(), key)
			if err != nil || removed == nil || removed.Resource.DeletionTimestamp == nil || removed.Complete {
				t.Fatalf("external CR deletion was not recorded locally: record=%#v err=%v", removed, err)
			}
			if removed.Revision != beforeRevision+1 {
				t.Fatalf("deletion revision=%d, want %d", removed.Revision, beforeRevision+1)
			}
			if err := c.observeExternal(t.Context(), removed); err != nil {
				t.Fatal(err)
			}
			afterRepeat, err := c.LocalRecord(t.Context(), key)
			if err != nil || afterRepeat == nil {
				t.Fatalf("missing local record after repeated observation: %#v err=%v", afterRepeat, err)
			}
			if afterRepeat.Revision != removed.Revision || afterRepeat.Resource.ResourceVersion != removed.Resource.ResourceVersion {
				t.Fatalf("repeated deletion observation bumped local version: revision %d->%d resourceVersion %q->%q", removed.Revision, afterRepeat.Revision, removed.Resource.ResourceVersion, afterRepeat.Resource.ResourceVersion)
			}
		})
	}
}

func TestSyncIntentRejectsMixedDesiredStateAndTransitionPairs(t *testing.T) {
	for _, mixed := range []struct {
		name, desired, transition string
	}{
		{name: "base-state-target-transition", desired: "Suspended", transition: "resume-1"},
		{name: "target-state-base-transition", desired: "Running", transition: "previous"},
	} {
		t.Run(mixed.name, func(t *testing.T) {
			_, journal, _ := openTestJournal(t)
			w, pod := localResumeObjects("mixed-"+mixed.name, types.UID("uid-"+mixed.name))
			c, raw := localFixture(t, w, pod)
			configureJournal(t, c, journal)
			baseline := &api.ResumablePod{}
			if err := c.Get(t.Context(), client.ObjectKeyFromObject(w), baseline); err != nil {
				t.Fatal(err)
			}
			beginLocalResume(t, c, baseline, pod)
			record, err := journal.Load(t.Context(), client.ObjectKeyFromObject(w))
			if err != nil || record == nil || record.Base == nil {
				t.Fatalf("resume journal lacks base/target: record=%#v err=%v", record, err)
			}
			current := &api.ResumablePod{}
			key := client.ObjectKeyFromObject(w)
			if err := raw.Get(t.Context(), key, current); err != nil {
				t.Fatal(err)
			}
			current.Spec.DesiredState = mixed.desired
			current.Spec.TransitionID = mixed.transition
			if err := raw.Update(t.Context(), current); err != nil {
				t.Fatal(err)
			}
			if _, err := c.syncIntent(t.Context(), record); !errors.Is(err, boxprovider.ErrStaleTransition) {
				t.Fatalf("mixed state/transition pair error=%v, want stale transition", err)
			}
			unchanged := &api.ResumablePod{}
			if err := raw.Get(t.Context(), key, unchanged); err != nil {
				t.Fatal(err)
			}
			if unchanged.Spec.DesiredState != mixed.desired || unchanged.Spec.TransitionID != mixed.transition {
				t.Fatalf("syncIntent wrote mixed pair: state=%s transition=%s", unchanged.Spec.DesiredState, unchanged.Spec.TransitionID)
			}
		})
	}
}

func TestSyncRejectsTerminalPodBeforePublishingResume(t *testing.T) {
	_, journal, _ := openTestJournal(t)
	w, pod := localResumeObjects("terminal", "uid-terminal")
	c, raw := localFixture(t, w, pod)
	configureJournal(t, c, journal)
	baseline := &api.ResumablePod{}
	if err := c.Get(t.Context(), client.ObjectKeyFromObject(w), baseline); err != nil {
		t.Fatal(err)
	}
	beginLocalResume(t, c, baseline, pod)

	terminalPod := &core.Pod{}
	if err := raw.Get(t.Context(), client.ObjectKeyFromObject(pod), terminalPod); err != nil {
		t.Fatal(err)
	}
	terminalPod.Status.Phase = core.PodFailed
	if err := raw.Status().Update(t.Context(), terminalPod); err != nil {
		t.Fatal(err)
	}
	verifiedTerminal := &core.Pod{}
	if err := raw.Get(t.Context(), client.ObjectKeyFromObject(pod), verifiedTerminal); err != nil {
		t.Fatal(err)
	}
	if verifiedTerminal.UID != pod.UID || verifiedTerminal.Status.Phase != core.PodFailed {
		t.Fatalf("test setup did not persist terminal Pod state: uid=%s phase=%s", verifiedTerminal.UID, verifiedTerminal.Status.Phase)
	}

	err := c.Sync(t.Context(), client.ObjectKeyFromObject(w))
	if err == nil {
		t.Fatal("Sync accepted a terminal Pod without observing the failure first")
	}
	local, err := c.LocalRecord(t.Context(), client.ObjectKeyFromObject(w))
	if err != nil || local == nil || !local.Complete || local.Resource.Status.Phase != "Running" || local.Pod == nil || local.Pod.UID != pod.UID {
		t.Fatalf("failed Sync did not preserve the completed pending record: record=%#v err=%v", local, err)
	}
	storedPod := &core.Pod{}
	if err := raw.Get(t.Context(), client.ObjectKeyFromObject(pod), storedPod); err != nil {
		t.Fatal(err)
	}
	if storedPod.UID != pod.UID || storedPod.Status.Phase != core.PodFailed || len(storedPod.OwnerReferences) != 0 || storedPod.Labels[api.OwnerLabel] != "" || storedPod.Labels[api.ServingLabel] != "false" {
		t.Fatalf("Sync published metadata for terminal Pod: uid=%s phase=%s owners=%v labels=%v", storedPod.UID, storedPod.Status.Phase, storedPod.OwnerReferences, storedPod.Labels)
	}
	storedCR := &api.ResumablePod{}
	if err := raw.Get(t.Context(), client.ObjectKeyFromObject(w), storedCR); err != nil {
		t.Fatal(err)
	}
	if storedCR.Status.Phase != "Suspended" {
		t.Fatalf("Sync published CR status despite terminal Pod: phase=%s", storedCR.Status.Phase)
	}
}

func TestExternalNodeChangeReclaimsOriginalLocalExecution(t *testing.T) {
	_, journal, _ := openTestJournal(t)
	w, pod := localResumeObjects("node-change", "uid-node-change")
	c, raw := localFixture(t, w, pod)
	configureJournal(t, c, journal)
	beginLocalResume(t, c, w, pod)
	current := &api.ResumablePod{}
	key := client.ObjectKeyFromObject(w)
	if err := raw.Get(t.Context(), key, current); err != nil {
		t.Fatal(err)
	}
	current.Spec.NodeName = "another-node"
	if err := raw.Update(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	r, err := c.LocalRecord(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.observeExternal(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	reclaim, err := c.LocalRecord(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if reclaim.Resource.DeletionTimestamp == nil || reclaim.Resource.Spec.NodeName != w.Spec.NodeName || reclaim.Pod.UID != pod.UID || reclaim.Complete {
		t.Fatalf("node edit lost the original cleanup identity: %#v", reclaim)
	}
}

func TestSyncPublishesNothingBeforeTheJournalIsDurable(t *testing.T) {
	db, _, _ := openTestJournal(t)
	durable, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatal(err)
	}
	durable.Close()
	w, pod := localResumeObjects("barrier", "barrier-uid")
	c, raw := localFixture(t, w, pod)
	journal, err := OpenRelaxedJournal(t.Context(), db, durable, filepath.Join(t.TempDir(), "journal.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close(context.Background())
	configureJournal(t, c, journal)
	beginLocalResume(t, c, w, pod)
	if err := c.Sync(t.Context(), client.ObjectKeyFromObject(w)); err == nil {
		t.Fatal("Sync succeeded although the journal could not be made durable")
	}
	published := &api.ResumablePod{}
	if err := raw.Get(t.Context(), client.ObjectKeyFromObject(w), published); err != nil {
		t.Fatal(err)
	}
	if published.Spec.TransitionID != "previous" || published.Status.Phase != "Suspended" {
		t.Fatalf("Kubernetes moved ahead of the journal: %q %q", published.Spec.TransitionID, published.Status.Phase)
	}
}
