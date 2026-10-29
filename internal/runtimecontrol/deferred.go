package runtimecontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/telemetry"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func (c *Client) getJournal() *Journal {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.journal
}

// SetJournal borrows the service database. Its lifetime must include the
// controller and synchronizer. Pending operations are queued before workers start.
func (c *Client) SetJournal(ctx context.Context, journal *Journal) error {
	records, err := journal.list(ctx, false)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.journal = journal
	c.mu.Unlock()
	for _, record := range records {
		key := client.ObjectKeyFromObject(record.Resource)
		unlock := c.lock(key)
		latest, err := journal.Load(ctx, key)
		if err == nil && latest != nil {
			c.publish(key, latest.Resource, false)
			c.Notify(key)
		}
		unlock()
		if err != nil {
			return err
		}
	}
	c.wakeSync()
	return nil
}

func (c *Client) HasJournal() bool { return c.getJournal() != nil }

func (c *Client) LocalRecord(ctx context.Context, key client.ObjectKey) (*Record, error) {
	j := c.getJournal()
	if j == nil {
		return nil, nil
	}
	r, err := j.Load(ctx, key)
	if err != nil || r == nil || !r.Pending {
		return nil, err
	}
	return r, nil
}

// StoredResource includes the synchronized baseline, so a concurrent background
// publication cannot turn local execution validation into a Kubernetes read.
func (c *Client) StoredResource(ctx context.Context, key client.ObjectKey) (*api.ResumablePod, error) {
	j := c.getJournal()
	if j == nil {
		return nil, nil
	}
	r, err := j.Load(ctx, key)
	if err != nil || r == nil {
		return nil, err
	}
	return r.Resource.DeepCopy(), nil
}

func localVersion(revision uint64) string { return fmt.Sprintf("local-%d", revision) }

func (c *Client) remember(ctx context.Context, w *api.ResumablePod) error {
	j := c.getJournal()
	if j == nil {
		return nil
	}
	key := client.ObjectKeyFromObject(w)
	for range 4 {
		r, err := j.Load(ctx, key)
		if err != nil {
			return err
		}
		if r != nil && (r.Pending || (r.Resource.UID == w.UID && r.Resource.ResourceVersion == w.ResourceVersion)) {
			return nil
		}
		var revision uint64
		if r != nil {
			revision = r.Revision
		}
		if err := j.Save(ctx, &Record{Resource: w.DeepCopy()}, revision); errors.Is(err, ErrJournalConflict) {
			continue
		} else {
			return err
		}
	}
	return ErrJournalConflict
}

func (c *Client) initialSnapshot(ctx context.Context, key client.ObjectKey, w *api.ResumablePod) error {
	if j := c.getJournal(); j != nil {
		unlock := c.lock(key)
		r, err := j.Load(ctx, key)
		if err != nil {
			unlock()
			return err
		}
		if r != nil {
			*w = *r.Resource.DeepCopy()
			c.publish(key, w, false)
			unlock()
			return nil
		}
		unlock()
	}
	return c.Get(ctx, key, w)
}

func (c *Client) localGet(ctx context.Context, key client.ObjectKey, obj client.Object) (bool, error) {
	w, ok := obj.(*api.ResumablePod)
	if !ok {
		return false, nil
	}
	r, err := c.LocalRecord(ctx, key)
	if err != nil || r == nil {
		return false, err
	}
	*w = *r.Resource.DeepCopy()
	return true, nil
}

func (c *Client) podRecord(ctx context.Context, pod *core.Pod) (*Record, error) {
	j := c.getJournal()
	if j == nil {
		return nil, nil
	}
	records, err := j.ListPending(ctx)
	if err != nil {
		return nil, err
	}
	for i := range records {
		r := &records[i]
		if r.Pod != nil && r.Pod.Namespace == pod.Namespace && r.Pod.Name == pod.Name && (pod.UID == "" || r.Pod.UID == pod.UID) {
			return r, nil
		}
	}
	return nil, nil
}

// projectPod preserves the kubelet's live status while applying the locally
// committed ownership/serving metadata that has not reached the API server yet.
func (c *Client) projectPod(ctx context.Context, pod *core.Pod) error {
	r, err := c.podRecord(ctx, pod)
	if err != nil || r == nil {
		return err
	}
	if owner := meta.GetControllerOf(pod); owner != nil && owner.UID != r.Resource.UID {
		return boxprovider.ErrStaleExecution
	}
	pod.OwnerReferences = append([]meta.OwnerReference(nil), r.Pod.OwnerReferences...)
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[api.OwnerLabel] = string(r.Resource.UID)
	pod.Labels[api.ServingLabel] = r.Pod.Labels[api.ServingLabel]
	return nil
}

func (c *Client) IsClaimed(ctx context.Context, pod *core.Pod) (bool, error) {
	r, err := c.podRecord(ctx, pod)
	return r != nil, err
}

// RequestResume records the intent locally before any runtime side effect.
// Existing snapshots are seeded outside the request path by normal reconciliation.
func (c *Client) RequestResume(ctx context.Context, h boxprovider.Handle, previous, transitionID string, requestedPorts ...int) error {
	ports, portErr := boxprovider.NormalizeWaitPorts(requestedPorts)
	if portErr != nil {
		return portErr
	}
	j := c.getJournal()
	if j == nil {
		return errors.New("local runtime journal is not configured")
	}
	key := client.ObjectKey{Namespace: h.Namespace, Name: h.Name}
	// A newly started server may not have observed this CR yet. This read has no
	// side effects; an already observed sandbox needs no API round trip here.
	seed, err := j.Load(ctx, key)
	if err != nil {
		return err
	}
	if seed == nil {
		if err := c.Get(ctx, key, &api.ResumablePod{}); err != nil {
			return err
		}
	}
	defer c.lock(key)()
	r, err := j.Load(ctx, key)
	if err != nil {
		return err
	}
	if r == nil {
		return boxprovider.ErrNotFound
	}
	w := r.Resource
	if transitionID == "" || string(w.UID) != h.ID || w.Spec.NodeName != h.NodeName || w.DeletionTimestamp != nil ||
		w.Labels["cellbox.jingyugao.github.io/managed"] != "true" || w.Labels["cellbox.jingyugao.github.io/box-id"] != strings.TrimPrefix(h.Name, "cellbox-") {
		return boxprovider.ErrStaleTransition
	}
	if w.Spec.TransitionID == transitionID && w.Spec.DesiredState == "Running" {
		if reflect.DeepEqual(w.Spec.WaitPorts, ports) {
			return nil
		}
		return boxprovider.ErrStaleTransition
	}
	if r.Pending || w.Spec.DesiredState != "Suspended" || w.Status.Phase != "Suspended" || w.Spec.TransitionID != previous || w.Status.ObservedTransitionID != previous {
		return boxprovider.ErrStaleTransition
	}
	r.Base = w.DeepCopy()
	r.Resource = w.DeepCopy()
	r.Resource.Spec.DesiredState = "Running"
	r.Resource.Spec.TransitionID = transitionID
	r.Resource.Spec.WaitPorts = ports
	r.Resource.Status.ResumePath = ""
	telemetry.InjectOperation(ctx, r.Resource)
	r.Resource.Generation++
	r.Resource.ResourceVersion = localVersion(r.Revision + 1)
	r.Pending, r.Complete, r.Pod = true, false, nil
	if err := j.Save(ctx, r, r.Revision); err != nil {
		return err
	}
	c.publish(key, r.Resource, false)
	c.Notify(key)
	return nil
}

// ClaimWarm commits the execution assignment and Pod reservation in one SQLite
// transaction. Recording Restoring here makes a crash before ActivateWarm resumable.
func (c *Client) ClaimWarm(ctx context.Context, w *api.ResumablePod, pod *core.Pod) (bool, error) {
	key := client.ObjectKeyFromObject(w)
	defer c.lock(key)()
	r, err := c.LocalRecord(ctx, key)
	if err != nil || r == nil {
		return false, err
	}
	if r.Resource.UID != w.UID || r.Resource.Spec.TransitionID != w.Spec.TransitionID || r.Resource.ResourceVersion != w.ResourceVersion {
		return true, boxprovider.ErrStaleTransition
	}
	if r.Pod != nil && r.Pod.UID != pod.UID {
		return true, ErrPodClaimed
	}
	if err := controllerutil.SetControllerReference(w, pod, c.Scheme()); err != nil {
		return true, err
	}
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[api.OwnerLabel] = string(w.UID)
	r.Pod = pod.DeepCopy()
	assignment := w.DeepCopy()
	assignment.Status.PodName, assignment.Status.PodUID = pod.Name, string(pod.UID)
	assignment.Status.Phase, assignment.Status.Since = "Restoring", meta.Now()
	assignment.Status.ResumePath = api.ResumePathWarmPool
	assignment.ResourceVersion = localVersion(r.Revision + 1)
	r.Resource = assignment
	if err := c.getJournal().Save(ctx, r, r.Revision); err != nil {
		return true, err
	}
	*w = *assignment.DeepCopy()
	c.publish(key, w, false)
	return true, nil
}

func (c *Client) localStatus(ctx context.Context, obj client.Object) (bool, error) {
	w, ok := obj.(*api.ResumablePod)
	if !ok {
		return false, nil
	}
	key := client.ObjectKeyFromObject(w)
	defer c.lock(key)()
	r, err := c.LocalRecord(ctx, key)
	if err != nil || r == nil {
		return false, err
	}
	if w.UID != r.Resource.UID || w.ResourceVersion != r.Resource.ResourceVersion {
		return true, apierrors.NewConflict(schema.GroupResource{Group: api.GroupVersion.Group, Resource: "cellboxes"}, w.Name, ErrJournalConflict)
	}
	r.Resource.Status = w.DeepCopy().Status
	r.Resource.ResourceVersion = localVersion(r.Revision + 1)
	r.Complete = w.Status.Phase == "Running" || w.Status.Phase == "Suspended" || w.Status.Phase == "Failed"
	if err := c.getJournal().Save(ctx, r, r.Revision); err != nil {
		return true, err
	}
	w.ResourceVersion = r.Resource.ResourceVersion
	c.publish(key, r.Resource, false)
	c.Notify(key)
	if r.Complete {
		c.wakeSync()
	}
	return true, nil
}

func (c *Client) localPatch(ctx context.Context, obj client.Object, patch client.Patch) (bool, error) {
	pod, ok := obj.(*core.Pod)
	if !ok {
		return false, nil
	}
	r, err := c.podRecord(ctx, pod)
	if err != nil || r == nil {
		return false, err
	}
	key := client.ObjectKeyFromObject(r.Resource)
	defer c.lock(key)()
	r, err = c.LocalRecord(ctx, key)
	if err != nil || r == nil {
		return false, err
	}
	before, err := json.Marshal(r.Pod)
	if err != nil {
		return true, err
	}
	data, err := patch.Data(obj)
	if err != nil {
		return true, err
	}
	var after []byte
	switch patch.Type() {
	case types.MergePatchType:
		after, err = jsonpatch.MergePatch(before, data)
	case types.JSONPatchType:
		var p jsonpatch.Patch
		p, err = jsonpatch.DecodePatch(data)
		if err == nil {
			after, err = p.Apply(before)
		}
	default:
		return true, errors.New("unsupported deferred Pod patch")
	}
	if err != nil {
		return true, err
	}
	var updated core.Pod
	if err := json.Unmarshal(after, &updated); err != nil {
		return true, err
	}
	if updated.UID != r.Pod.UID || updated.Name != r.Pod.Name || updated.Namespace != r.Pod.Namespace {
		return true, boxprovider.ErrStaleExecution
	}
	r.Pod = &updated
	if err := c.getJournal().Save(ctx, r, r.Revision); err != nil {
		return true, err
	}
	return true, nil
}

func (c *Client) localUpdate(ctx context.Context, obj client.Object) (bool, error) {
	w, ok := obj.(*api.ResumablePod)
	if !ok {
		return false, nil
	}
	key := client.ObjectKeyFromObject(w)
	defer c.lock(key)()
	r, err := c.LocalRecord(ctx, key)
	if err != nil || r == nil {
		return false, err
	}
	if w.UID != r.Resource.UID || w.ResourceVersion != r.Resource.ResourceVersion {
		return true, boxprovider.ErrStaleTransition
	}
	if w.DeletionTimestamp != nil && !controllerutil.ContainsFinalizer(w, api.Finalizer) {
		current := &api.ResumablePod{}
		err := c.Client.Get(ctx, key, current)
		if err != nil && !apierrors.IsNotFound(err) {
			return true, err
		}
		if err == nil && current.UID == w.UID && current.DeletionTimestamp != nil {
			controllerutil.RemoveFinalizer(current, api.Finalizer)
			if err = c.Client.Update(ctx, current); err != nil {
				return true, err
			}
		}
		if err := c.getJournal().Delete(ctx, key, r.Revision); err != nil {
			return true, err
		}
		c.publish(key, w, true)
		c.Notify(key)
		return true, nil
	}
	r.Resource = w.DeepCopy()
	r.Resource.ResourceVersion = localVersion(r.Revision + 1)
	if err := c.getJournal().Save(ctx, r, r.Revision); err != nil {
		return true, err
	}
	w.ResourceVersion = r.Resource.ResourceVersion
	c.publish(key, w, false)
	c.Notify(key)
	return true, nil
}

func (c *Client) wakeSync() {
	select {
	case c.syncWake <- struct{}{}:
	default:
	}
}

// FallbackResume retains the existing Kubernetes lifecycle when the pool misses.
// No Pod has been claimed or runtime operation executed at this point.
func (c *Client) FallbackResume(ctx context.Context, w *api.ResumablePod) (bool, error) {
	key := client.ObjectKeyFromObject(w)
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	r, err := c.LocalRecord(ctx, key)
	if err != nil || r == nil {
		return false, err
	}
	if r.Pod != nil {
		return true, errors.New("cannot fall back after claiming a warm execution")
	}
	current, err := c.syncIntent(ctx, r)
	if err != nil {
		return true, err
	}
	defer c.lock(key)()
	latest, err := c.LocalRecord(ctx, key)
	if err != nil || latest == nil {
		return true, err
	}
	if latest.Revision != r.Revision {
		return true, ErrJournalConflict
	}
	r.Resource, r.Base, r.Pending, r.Complete = current, nil, false, false
	if err := c.getJournal().Save(ctx, r, r.Revision); err != nil {
		return true, err
	}
	c.publish(key, current, false)
	c.Notify(key)
	return true, nil
}

func sameProfile(a, b api.Spec) bool {
	a.DesiredState, b.DesiredState, a.TransitionID, b.TransitionID = "", "", "", ""
	a.WaitPorts, b.WaitPorts = nil, nil
	return reflect.DeepEqual(a, b)
}

func (c *Client) syncIntent(ctx context.Context, r *Record) (*api.ResumablePod, error) {
	current := &api.ResumablePod{}
	if err := c.Client.Get(ctx, client.ObjectKeyFromObject(r.Resource), current); err != nil {
		return nil, err
	}
	if current.UID != r.Resource.UID || current.DeletionTimestamp != nil {
		return nil, boxprovider.ErrStaleTransition
	}
	if current.Spec.TransitionID == r.Resource.Spec.TransitionID && current.Spec.DesiredState == r.Resource.Spec.DesiredState &&
		!reflect.DeepEqual(current.Spec.WaitPorts, r.Resource.Spec.WaitPorts) {
		return nil, boxprovider.ErrStaleTransition
	}
	if r.Base == nil || !sameProfile(current.Spec, r.Base.Spec) ||
		!((current.Spec.TransitionID == r.Base.Spec.TransitionID && current.Spec.DesiredState == r.Base.Spec.DesiredState) ||
			(current.Spec.TransitionID == r.Resource.Spec.TransitionID && current.Spec.DesiredState == r.Resource.Spec.DesiredState)) {
		return nil, boxprovider.ErrStaleTransition
	}
	if current.Spec.DesiredState != r.Resource.Spec.DesiredState || current.Spec.TransitionID != r.Resource.Spec.TransitionID || !reflect.DeepEqual(current.Spec.WaitPorts, r.Resource.Spec.WaitPorts) {
		old := current.DeepCopy()
		current.Spec.DesiredState, current.Spec.TransitionID = r.Resource.Spec.DesiredState, r.Resource.Spec.TransitionID
		current.Spec.WaitPorts = append([]int(nil), r.Resource.Spec.WaitPorts...)
		if current.Annotations == nil {
			current.Annotations = map[string]string{}
		}
		if trace := r.Resource.Annotations["cellbox.jingyugao.github.io/operation-trace"]; trace != "" {
			current.Annotations["cellbox.jingyugao.github.io/operation-trace"] = trace
		} else {
			delete(current.Annotations, "cellbox.jingyugao.github.io/operation-trace")
		}
		if err := c.Client.Patch(ctx, current, client.MergeFromWithOptions(old, client.MergeFromWithOptimisticLock{})); err != nil {
			return nil, err
		}
	}
	return current, nil
}

// Sync never holds a local admission lock across API calls. A blocked API server
// therefore cannot delay local execution or access to a completed warm resume.
func (c *Client) Sync(ctx context.Context, key client.ObjectKey) error {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	r, err := c.LocalRecord(ctx, key)
	if err != nil || r == nil {
		return err
	}
	if !r.Complete {
		return boxprovider.ErrNotReady
	}
	current, err := c.syncIntent(ctx, r)
	if err != nil {
		return err
	}
	if r.Pod != nil && r.Resource.Status.Phase == "Running" {
		pod := &core.Pod{}
		if err := c.Client.Get(ctx, client.ObjectKeyFromObject(r.Pod), pod); err != nil {
			return err
		}
		if pod.UID != r.Pod.UID || pod.DeletionTimestamp != nil {
			return boxprovider.ErrStaleExecution
		}
		if pod.Status.Phase == core.PodFailed || pod.Status.Phase == core.PodSucceeded ||
			(r.Resource.Status.Execution != nil && !api.ConfirmedExecution(r.Resource, pod)) {
			return boxprovider.ErrStaleExecution
		}
		if owner := meta.GetControllerOf(pod); owner != nil && owner.UID != r.Resource.UID {
			return boxprovider.ErrStaleExecution
		}
		old := pod.DeepCopy()
		if err := controllerutil.SetControllerReference(current, pod, c.Scheme()); err != nil {
			return err
		}
		if pod.Labels == nil {
			pod.Labels = map[string]string{}
		}
		pod.Labels[api.OwnerLabel], pod.Labels[api.ServingLabel] = string(current.UID), r.Pod.Labels[api.ServingLabel]
		if !reflect.DeepEqual(old.OwnerReferences, pod.OwnerReferences) || !reflect.DeepEqual(old.Labels, pod.Labels) {
			if err := c.Client.Patch(ctx, pod, client.MergeFromWithOptions(old, client.MergeFromWithOptimisticLock{})); err != nil {
				return err
			}
		}
	}
	current.Status = r.Resource.DeepCopy().Status
	current.Status.ObservedGeneration = current.Generation
	for i := range current.Status.Conditions {
		current.Status.Conditions[i].ObservedGeneration = current.Generation
	}
	if err := c.Client.Status().Update(ctx, current); err != nil {
		return err
	}
	defer c.lock(key)()
	latest, err := c.LocalRecord(ctx, key)
	if err != nil || latest == nil {
		return err
	}
	if latest.Revision != r.Revision {
		return ErrJournalConflict
	}
	latest.Resource, latest.Base, latest.Pod = current, nil, nil
	latest.Pending, latest.Complete = false, false
	if err := c.getJournal().Save(ctx, latest, latest.Revision); err != nil {
		return err
	}
	c.publish(key, current, false)
	c.Notify(key)
	return nil
}

// Flush orders a later suspend/destroy after publication of an earlier local
// resume. Only those later lifecycle actions wait; resume completion does not.
func (c *Client) Flush(ctx context.Context, key client.ObjectKey) error { return c.Sync(ctx, key) }

// observeExternal handles deletion/replacement even while the local snapshot is
// ahead of Kubernetes. The local Pod assignment remains available to finalization.
func (c *Client) observeExternal(ctx context.Context, r *Record) error {
	if r.Resource.DeletionTimestamp != nil {
		return nil
	}
	current := &api.ResumablePod{}
	err := c.Client.Get(ctx, client.ObjectKeyFromObject(r.Resource), current)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	deleted := apierrors.IsNotFound(err) || current.UID != r.Resource.UID || current.DeletionTimestamp != nil
	changed := !deleted && r.Base != nil && !reflect.DeepEqual(current.Spec, r.Base.Spec) && !reflect.DeepEqual(current.Spec, r.Resource.Spec)
	// A completed local execution must still notice loss before publication.
	lost := false
	if !deleted && !changed && r.Complete && r.Resource.Status.Phase == "Running" && r.Pod != nil {
		pod := &core.Pod{}
		err := c.Client.Get(ctx, client.ObjectKeyFromObject(r.Pod), pod)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		lost = apierrors.IsNotFound(err) || pod.UID != r.Pod.UID || pod.DeletionTimestamp != nil || pod.Status.Phase == core.PodFailed || pod.Status.Phase == core.PodSucceeded ||
			(r.Resource.Status.Execution != nil && !api.ConfirmedExecution(r.Resource, pod))
	}
	if !deleted && !changed && !lost {
		return nil
	}
	if deleted && r.Resource.DeletionTimestamp != nil {
		return nil
	}
	key := client.ObjectKeyFromObject(r.Resource)
	defer c.lock(key)()
	latest, err := c.LocalRecord(ctx, key)
	if err != nil || latest == nil {
		return err
	}
	if latest.Revision != r.Revision {
		return nil
	}
	if deleted {
		now := meta.Now()
		latest.Resource.DeletionTimestamp = &now
		if current.UID == latest.Resource.UID && current.DeletionTimestamp != nil {
			latest.Resource.DeletionTimestamp = current.DeletionTimestamp.DeepCopy()
		}
	} else if changed && !sameProfile(current.Spec, latest.Resource.Spec) {
		// The execution belongs to the admitted profile and node. Reclaim it
		// using that original identity before allowing an incompatible CR edit
		// to be reconciled normally; never move a live claim to another node.
		now := meta.Now()
		latest.Resource.DeletionTimestamp = &now
	} else if lost {
		latest.Resource.Status.Phase = "Failing"
		latest.Resource.Status.Message = "Local execution lost before Kubernetes publication"
	} else {
		latest.Base = current.DeepCopy()
		latest.Resource.Spec, latest.Resource.Generation = current.Spec, current.Generation
	}
	latest.Resource.ResourceVersion = localVersion(latest.Revision + 1)
	latest.Complete = false
	if err := c.getJournal().Save(ctx, latest, latest.Revision); err != nil {
		return err
	}
	c.publish(key, latest.Resource, false)
	c.Notify(key)
	return nil
}

type Synchronizer struct{ Client *Client }

func (*Synchronizer) NeedLeaderElection() bool { return true }

func (s *Synchronizer) Start(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.Client.syncWake:
		case <-ticker.C:
		}
		j := s.Client.getJournal()
		if j == nil {
			continue
		}
		records, err := j.ListPending(ctx)
		if err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "cannot read deferred runtime records")
			continue
		}
		for i := range records {
			r := &records[i]
			key := client.ObjectKeyFromObject(r.Resource)
			// Bound background API work independently of local request processing.
			syncCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := s.Client.observeExternal(syncCtx, r)
			if err == nil && r.Complete {
				err = s.Client.Sync(syncCtx, key)
			}
			cancel()
			if err != nil && ctx.Err() == nil && !errors.Is(err, boxprovider.ErrNotReady) {
				ctrl.LoggerFrom(ctx).Error(err, "deferred Kubernetes synchronization will retry", "resource", key)
			}
		}
	}
}
