package controller

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const warmLabel = "cellbox.local/warm-pool"
const warmSpec = "cellbox.local/warm-spec"

type warmRuntime interface {
	RegisterWarm(context.Context, *core.Pod, string) error
	WarmState(context.Context, *core.Pod) (string, time.Time, error)
	ActivateWarm(context.Context, *api.ResumablePod, *core.Pod) (bool, error)
	ReapWarm(context.Context, string, map[string]bool) error
}

// WarmPool has a node-wide bound, shared across compatible templates. It never
// substitutes one image/configuration for another. A miss uses ordinary restore.
// One extra slot is allowed while replacing a lease approaching its deadline.
type WarmPool struct {
	Reconciler *Reconciler
	Namespace  string
	Size       int
	mu         sync.Mutex
}

func (*WarmPool) NeedLeaderElection() bool { return true }
func (p *WarmPool) Start(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := p.reconcile(ctx); err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "warm pool reconcile failed")
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func executionPod(w *api.ResumablePod, name string) *core.Pod {
	no := false
	grace := int64(3)
	runtime := api.RuntimeClass
	container := *w.Spec.Container.DeepCopy()
	container.VolumeMounts = hostMounts(w)
	return &core.Pod{ObjectMeta: meta.ObjectMeta{Name: name, Namespace: w.Namespace, Labels: map[string]string{api.OwnerLabel: string(w.UID)}}, Spec: core.PodSpec{Containers: []core.Container{container}, Volumes: hostVolumes(w), RuntimeClassName: &runtime, RestartPolicy: core.RestartPolicyNever, AutomountServiceAccountToken: &no, EnableServiceLinks: &no, Hostname: "recoverable", TerminationGracePeriodSeconds: &grace, NodeSelector: map[string]string{"kubernetes.io/hostname": w.Spec.NodeName}, SchedulingGates: []core.PodSchedulingGate{{Name: api.Gate}}}}
}

func (p *WarmPool) pods(ctx context.Context) ([]core.Pod, error) {
	var list core.PodList
	if err := p.Reconciler.List(ctx, &list, client.InNamespace(p.Namespace), client.MatchingLabels{warmLabel: "true"}); err != nil {
		return nil, err
	}
	out := []core.Pod{}
	for _, pod := range list.Items {
		if pod.Spec.NodeSelector["kubernetes.io/hostname"] == p.Reconciler.NodeName {
			out = append(out, pod)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreationTimestamp.Before(&out[j].CreationTimestamp) })
	return out, nil
}

func (p *WarmPool) remove(ctx context.Context, pod *core.Pod) error {
	r := p.Reconciler
	if pod.DeletionTimestamp == nil {
		uid := pod.UID
		if err := r.Delete(ctx, pod, &client.DeleteOptions{Preconditions: &meta.Preconditions{UID: &uid, ResourceVersion: &pod.ResourceVersion}}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return r.Runtime.Cleanup(ctx, pod)
}

func (p *WarmPool) reconcile(ctx context.Context) error {
	r := p.Reconciler
	backend, ok := r.Runtime.(warmRuntime)
	if !ok {
		return fmt.Errorf("runtime does not support warm Pods")
	}
	pods, err := p.pods(ctx)
	if err != nil {
		return err
	}
	existing := map[string]bool{}
	for _, pod := range pods {
		existing[string(pod.UID)] = true
	}
	if err = backend.ReapWarm(ctx, p.Namespace, existing); err != nil {
		return err
	}
	var workloads api.ResumablePodList
	if err = r.List(ctx, &workloads, client.InNamespace(p.Namespace)); err != nil {
		return err
	}
	templates := map[string]*api.ResumablePod{}
	var candidates []*api.ResumablePod
	restoring := false
	for i := range workloads.Items {
		w := &workloads.Items[i]
		eligible := w.Status.Phase == "Creating" || w.Status.Phase == "Running" || w.Status.Phase == "Checkpointing" || w.Status.Phase == "Suspending" || w.Status.Phase == "Suspended" || w.Status.Phase == "Restoring"
		if w.Spec.NodeName == r.NodeName && w.DeletionTimestamp == nil && w.Status.SpecHash != "" && eligible && validate(w) == nil {
			if w.Status.Phase == "Restoring" && time.Since(w.Status.Since.Time) < 2*time.Second {
				restoring = true
			}
			templates[w.Status.SpecHash] = w
			candidates = append(candidates, w)
		}
	}
	// Recent workloads get capacity first. Two recent workloads sharing a
	// template share two slots; old suspended configurations do not grow the pool.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Status.Since.Equal(&candidates[j].Status.Since) {
			return candidates[i].Name < candidates[j].Name
		}
		return candidates[j].Status.Since.Before(&candidates[i].Status.Since)
	})
	targets := map[string]int{}
	if len(candidates) > 0 {
		for i := 0; i < p.Size; i++ {
			targets[candidates[i%len(candidates)].Status.SpecHash]++
		}
	}
	keys := []string{}
	for k := range targets {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	counts := map[string]int{}
	total, available := 0, 0
	var aging []*core.Pod
	for i := range pods {
		pod := &pods[i]
		if owner := meta.GetControllerOf(pod); owner != nil {
			recorded := false
			for _, w := range workloads.Items {
				if w.UID == owner.UID && w.Status.PodUID == string(pod.UID) {
					recorded = true
					break
				}
			}
			phase, deadline, e := backend.WarmState(ctx, pod)
			if !recorded && e == nil && (phase == "expired" || phase == "retired" || (phase == "waiting" && time.Now().After(deadline))) {
				if err = p.remove(ctx, pod); err != nil {
					return err
				}
			}
			continue
		}
		hash := pod.Annotations[warmSpec]
		phase, deadline, stateErr := backend.WarmState(ctx, pod)
		expired := (!deadline.IsZero() && time.Until(deadline) < 5*time.Second) || (deadline.IsZero() && !pod.CreationTimestamp.IsZero() && time.Since(pod.CreationTimestamp.Time) > 90*time.Second)
		if pod.DeletionTimestamp != nil || targets[hash] == 0 || expired || phase == "expired" || phase == "retired" || pod.Status.Phase == core.PodFailed || pod.Status.Phase == core.PodSucceeded {
			if err = p.remove(ctx, pod); err != nil {
				return err
			}
			continue
		}
		if len(pod.Spec.SchedulingGates) > 0 {
			if len(pod.Spec.SchedulingGates) != 1 || pod.Spec.SchedulingGates[0].Name != api.Gate {
				return fmt.Errorf("unexpected warm Pod gate")
			}
			if err = backend.RegisterWarm(ctx, pod, hash); err != nil {
				return err
			}
			old := pod.DeepCopy()
			pod.Spec.SchedulingGates = nil
			if err = r.Patch(ctx, pod, client.MergeFrom(old)); err != nil {
				return err
			}
		} else if stateErr != nil {
			return stateErr
		}
		total++
		if phase == "waiting" {
			available++
		}
		if phase == "waiting" && time.Until(deadline) < 15*time.Second {
			aging = append(aging, pod)
			continue
		}
		counts[hash]++
	}
	// Retire the rotating slot only after another loop has prepared its successor.
	if total >= p.Size+1 {
		if available >= p.Size+1 && len(aging) > 0 {
			return p.remove(ctx, aging[0])
		}
		return nil
	}
	for _, hash := range keys {
		if counts[hash] >= targets[hash] {
			continue
		}
		// Give requested restores priority over speculative runtime creation.
		// Lease retirement still runs above, and replenishment resumes on the
		// next pool pass as soon as active restores finish.
		if restoring {
			return nil
		}
		pod := executionPod(templates[hash], "")
		pod.GenerateName = "cb-warm-"
		pod.Labels = map[string]string{warmLabel: "true", api.ServingLabel: "false"}
		pod.Annotations = map[string]string{warmSpec: hash}
		return r.Create(ctx, pod)
	}
	return nil
}

func (p *WarmPool) acquire(ctx context.Context, w *api.ResumablePod) (*core.Pod, error) {
	if p.Size == 0 {
		return nil, nil
	}
	started := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	locked := time.Now()
	var listed time.Time
	var stateTime, patchTime time.Duration
	defer func() {
		ctrl.LoggerFrom(ctx).Info("warm acquisition timing", "lockMs", locked.Sub(started).Milliseconds(), "listMs", listed.Sub(locked).Milliseconds(), "leaseMs", stateTime.Milliseconds(), "patchMs", patchTime.Milliseconds())
	}()
	r := p.Reconciler
	backend := r.Runtime.(warmRuntime)
	pods, err := p.pods(ctx)
	listed = time.Now()
	if err != nil {
		return nil, err
	}
	for i := range pods {
		pod := &pods[i]
		owner := meta.GetControllerOf(pod)
		if owner != nil && !owned(w, pod) {
			continue
		}
		if pod.Annotations[warmSpec] != w.Status.SpecHash || pod.DeletionTimestamp != nil {
			continue
		}
		stateStarted := time.Now()
		phase, deadline, err := backend.WarmState(ctx, pod)
		stateTime += time.Since(stateStarted)
		if err != nil || phase != "waiting" || time.Until(deadline) < 5*time.Second {
			// Recover an adoption whose CR status update was interrupted.
			if owned(w, pod) {
				if err = p.remove(ctx, pod); err != nil {
					return nil, err
				}
			}
			continue
		}
		if !owned(w, pod) {
			old := pod.DeepCopy()
			if err = controllerutil.SetControllerReference(w, pod, r.Scheme()); err != nil {
				return nil, err
			}
			pod.Labels[api.OwnerLabel] = string(w.UID)
			patchStarted := time.Now()
			err = r.Patch(ctx, pod, client.MergeFromWithOptions(old, client.MergeFromWithOptimisticLock{}))
			patchTime += time.Since(patchStarted)
			if err != nil {
				return nil, err
			}
		}
		return pod, nil
	}
	return nil, nil
}

// Reclaim an adopted slot even if the controller crashed before recording its
// UID in CR status. Finalizer completion must not rely on garbage collection.
func (p *WarmPool) cleanupUnused(ctx context.Context, w *api.ResumablePod) (bool, error) {
	pods, err := p.pods(ctx)
	if err != nil {
		return false, err
	}
	done := true
	for i := range pods {
		pod := &pods[i]
		if owned(w, pod) && string(pod.UID) != w.Status.PodUID {
			done = false
			if err = p.remove(ctx, pod); err != nil {
				return false, err
			}
		}
	}
	return done, nil
}
