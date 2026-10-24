package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/runtimecontrol"
	"cellbox.local/cellbox/internal/warmtemplates"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	Reconciler         *Reconciler
	Namespace          string
	Size               int
	ReadDesired        func(context.Context) ([]warmtemplates.Template, string, error)
	PublishObservation func(warmtemplates.Snapshot)
	mu                 sync.Mutex
	cachedPods         []core.Pod
	failureMu          sync.Mutex
	failures           map[string]warmFailure
}

type warmFailure struct {
	fingerprint string
	at          time.Time
}

func (p *WarmPool) recordFailure(uid, hash string) {
	p.failureMu.Lock()
	defer p.failureMu.Unlock()
	if p.failures == nil {
		p.failures = map[string]warmFailure{}
	}
	p.failures[uid] = warmFailure{fingerprint: hash, at: time.Now()}
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

func executionPod(w *api.ResumablePod, name string, settings ...string) *core.Pod {
	no := false
	grace := int64(3)
	runtimeClass, nodeBase := api.RuntimeClass, homevolume.DefaultBase
	if len(settings) > 0 && settings[0] != "" {
		runtimeClass = settings[0]
	}
	if len(settings) > 1 && settings[1] != "" {
		nodeBase = settings[1]
	}
	// Resolve external CLI hostnames before cluster search suffixes. A suffix
	// answered by upstream fake-IP DNS can otherwise route Go TLS to the wrong host.
	ndots := "1"
	container := *w.Spec.Container.DeepCopy()
	container.VolumeMounts = hostMounts(w)
	return &core.Pod{ObjectMeta: meta.ObjectMeta{Name: name, Namespace: w.Namespace, Labels: map[string]string{api.OwnerLabel: string(w.UID)}}, Spec: core.PodSpec{DNSConfig: &core.PodDNSConfig{Options: []core.PodDNSConfigOption{{Name: "ndots", Value: &ndots}}}, Containers: []core.Container{container}, Volumes: hostVolumes(w, nodeBase), RuntimeClassName: &runtimeClass, RestartPolicy: core.RestartPolicyNever, AutomountServiceAccountToken: &no, EnableServiceLinks: &no, Hostname: "recoverable", TerminationGracePeriodSeconds: &grace, NodeSelector: map[string]string{"kubernetes.io/hostname": w.Spec.NodeName}, SchedulingGates: []core.PodSchedulingGate{{Name: api.Gate}}}}
}

func warmExecutionPod(w *api.ResumablePod, name string, settings ...string) *core.Pod {
	template := w.DeepCopy()
	// The adapter binds the claimed Box's volumes before creating its child
	// Gofer. Warm containers must never use the template Box's volumes.
	template.Spec.PersistentHome = false
	template.Spec.HomeVolumeUID = ""
	template.Spec.Mounts = nil
	return executionPod(template, name, settings...)
}

func (p *WarmPool) pods(ctx context.Context, options ...client.ListOption) ([]core.Pod, error) {
	var list core.PodList
	options = append(options, client.InNamespace(p.Namespace), client.MatchingLabels{warmLabel: "true"})
	if err := p.Reconciler.List(ctx, &list, options...); err != nil {
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

func (p *WarmPool) locallyClaimed(ctx context.Context, pod *core.Pod) (bool, error) {
	if local, ok := p.Reconciler.Client.(*runtimecontrol.Client); ok {
		return local.IsClaimed(ctx, pod)
	}
	return false, nil
}

// Serialize retirement against acquisition, then release the pool lock before
// contacting Kubernetes. The lease cannot be claimed after it becomes retired.
func (p *WarmPool) removeIdle(ctx context.Context, pod *core.Pod) error {
	p.mu.Lock()
	claimed, err := p.locallyClaimed(ctx, pod)
	if err == nil && !claimed {
		if backend, ok := p.Reconciler.Runtime.(interface {
			TryRetireWarm(context.Context, *core.Pod) (bool, error)
		}); ok {
			var retired bool
			retired, err = backend.TryRetireWarm(ctx, pod)
			claimed = !retired
		}
	}
	p.mu.Unlock()
	if err != nil || claimed {
		return err
	}
	return p.remove(ctx, pod)
}

// allocateWarmSlots uses max-min fair allocation: equally sized demands receive
// equal capacity, and a template below its fair share releases the remainder to
// the others. Identical Specs have already been folded into one demand.
func allocateWarmSlots(demands map[string]int, budget int) map[string]int {
	targets := make(map[string]int, len(demands))
	keys := make([]string, 0, len(demands))
	for key, demand := range demands {
		if demand > 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	remaining := budget
	for remaining > 0 {
		active := 0
		for _, key := range keys {
			if targets[key] < demands[key] {
				active++
			}
		}
		if active == 0 {
			break
		}
		share := remaining / active
		if share == 0 {
			share = 1
		}
		progress := 0
		for _, key := range keys {
			room := demands[key] - targets[key]
			if room <= 0 {
				continue
			}
			give := share
			if give > room {
				give = room
			}
			if give > remaining {
				give = remaining
			}
			targets[key] += give
			remaining -= give
			progress += give
			if remaining == 0 {
				break
			}
		}
		if progress == 0 {
			break
		}
	}
	return targets
}

func (p *WarmPool) reconcile(ctx context.Context) error {
	if p.ReadDesired == nil {
		return fmt.Errorf("warm pool desired-template reader is required")
	}
	configured, desiredHash, err := p.ReadDesired(ctx)
	if err != nil {
		return fmt.Errorf("read warm template configuration: %w", err)
	}
	if desiredHash == "" {
		return fmt.Errorf("warm template configuration hash is empty")
	}
	if err := p.reconcilePools(ctx, configured); err != nil {
		return err
	}
	return p.publishSnapshot(ctx, configured, desiredHash)
}

func (p *WarmPool) reconcilePools(ctx context.Context, configured []warmtemplates.Template) error {
	r := p.Reconciler
	backend, ok := r.Runtime.(warmRuntime)
	if !ok {
		return fmt.Errorf("runtime does not support warm Pods")
	}
	pods, err := p.pods(ctx)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.cachedPods = make([]core.Pod, len(pods))
	for i := range pods {
		p.cachedPods[i] = *pods[i].DeepCopy()
	}
	p.mu.Unlock()
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
	demands := map[string]int{}
	restoring := false
	for i := range workloads.Items {
		w := &workloads.Items[i]
		if w.Spec.NodeName == r.NodeName && w.Status.Phase == "Restoring" && time.Since(w.Status.Since.Time) < 2*time.Second {
			restoring = true
		}
	}
	for _, configuredTemplate := range configured {
		if configuredTemplate.Slots == 0 || configuredTemplate.Spec.NodeName != r.NodeName {
			continue
		}
		w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{Name: configuredTemplate.ID, Namespace: p.Namespace, UID: types.UID("00000000-0000-4000-8000-000000000001")}, Spec: configuredTemplate.Spec}
		if err := validate(w, p.Reconciler.nodeBase()); err != nil {
			return fmt.Errorf("invalid warm template %q: %w", configuredTemplate.ID, err)
		}
		hash := fingerprint(w)
		w.Status.SpecHash = hash
		if templates[hash] == nil {
			templates[hash] = w
		}
		demands[hash] += configuredTemplate.Slots
	}
	targets := allocateWarmSlots(demands, p.Size)
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
		if claimed, err := p.locallyClaimed(ctx, pod); err != nil {
			return err
		} else if claimed {
			restoring = true
			continue
		}
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
				if err = p.removeIdle(ctx, pod); err != nil {
					return err
				}
			}
			continue
		}
		hash := pod.Annotations[warmSpec]
		phase, deadline, stateErr := backend.WarmState(ctx, pod)
		expired := (!deadline.IsZero() && time.Until(deadline) < 5*time.Second) || (deadline.IsZero() && !pod.CreationTimestamp.IsZero() && time.Since(pod.CreationTimestamp.Time) > 90*time.Second)
		if pod.DeletionTimestamp != nil || targets[hash] == 0 || expired || phase == "expired" || phase == "retired" || pod.Status.Phase == core.PodFailed || pod.Status.Phase == core.PodSucceeded {
			if pod.Status.Phase == core.PodFailed || pod.Status.Phase == core.PodSucceeded {
				p.recordFailure(string(pod.UID), hash)
			}
			if err = p.removeIdle(ctx, pod); err != nil {
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
		if phase == "waiting" && time.Until(deadline) < 15*time.Second {
			aging = append(aging, pod)
			if phase == "waiting" {
				available++
			}
			total++
			continue
		}
		if counts[hash] >= targets[hash] {
			// Drop excess idle capacity before filling other templates' fair
			// shares. Keep the oldest compatible slots because pods are sorted by
			// creation time.
			if err = p.removeIdle(ctx, pod); err != nil {
				return err
			}
			continue
		}
		if phase == "waiting" {
			available++
		}
		counts[hash]++
		total++
	}
	// Retire the rotating slot only after another loop has prepared its successor.
	if total >= p.Size+1 {
		if available >= p.Size+1 && len(aging) > 0 {
			return p.removeIdle(ctx, aging[0])
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
		pod := warmExecutionPod(templates[hash], "", r.runtimeClass(), r.nodeBase())
		pod.GenerateName = "cb-warm-"
		pod.Labels = map[string]string{warmLabel: "true", api.ServingLabel: "false"}
		pod.Annotations = map[string]string{warmSpec: hash}
		return r.Create(ctx, pod)
	}
	return nil
}

func (p *WarmPool) publishSnapshot(ctx context.Context, configured []warmtemplates.Template, desiredHash string) error {
	r := p.Reconciler
	backend, ok := r.Runtime.(warmRuntime)
	if !ok {
		return fmt.Errorf("runtime does not support warm Pods")
	}
	type pool struct {
		observation boxprovider.WarmPoolObservation
	}
	groups := map[string]*pool{}
	demands := map[string]int{}
	for _, template := range configured {
		if template.Spec.NodeName != r.NodeName {
			continue
		}
		w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{Name: template.ID, Namespace: p.Namespace, UID: types.UID("00000000-0000-4000-8000-000000000001")}, Spec: template.Spec}
		if err := validate(w, p.Reconciler.nodeBase()); err != nil {
			return fmt.Errorf("invalid warm template %q: %w", template.ID, err)
		}
		hash := fingerprint(w)
		entry := groups[hash]
		if entry == nil {
			entry = &pool{observation: boxprovider.WarmPoolObservation{PoolID: poolID(p.Namespace, r.NodeName, hash), DesiredHash: desiredHash}}
			groups[hash] = entry
		}
		entry.observation.TemplateIDs = append(entry.observation.TemplateIDs, template.ID)
		entry.observation.RequestedSlots += template.Slots
		demands[hash] += template.Slots
	}
	allocated := allocateWarmSlots(demands, p.Size)
	for hash, entry := range groups {
		entry.observation.AllocatedSlots = allocated[hash]
		sort.Strings(entry.observation.TemplateIDs)
	}
	pods, err := p.pods(ctx)
	if err != nil {
		return err
	}
	seenFailed := map[string]bool{}
	for i := range pods {
		pod := &pods[i]
		if claimed, err := p.locallyClaimed(ctx, pod); err != nil {
			return err
		} else if claimed {
			continue
		}
		if meta.GetControllerOf(pod) != nil || pod.DeletionTimestamp != nil {
			continue
		}
		entry := groups[pod.Annotations[warmSpec]]
		if entry == nil {
			continue
		}
		if pod.Status.Phase == core.PodFailed || pod.Status.Phase == core.PodSucceeded {
			entry.observation.FailedSlots++
			seenFailed[string(pod.UID)] = true
			continue
		}
		rc := pod.Spec.Containers
		if len(rc) > 0 {
			requests, limits := rc[0].Resources.Requests, rc[0].Resources.Limits
			entry.observation.Resources.CPURequestMilli += requests.Cpu().MilliValue()
			entry.observation.Resources.CPULimitMilli += limits.Cpu().MilliValue()
			entry.observation.Resources.MemoryRequestBytes += requests.Memory().Value()
			entry.observation.Resources.MemoryLimitBytes += limits.Memory().Value()
		}
		// A newly created gated Pod has no runtime ticket until RegisterWarm.
		// This is normal preparation, not an observation failure.
		if len(pod.Spec.SchedulingGates) > 0 {
			entry.observation.PreparingSlots++
			continue
		}
		phase, _, stateErr := backend.WarmState(ctx, pod)
		if stateErr != nil {
			return stateErr
		}
		if phase == "waiting" {
			entry.observation.ReadySlots++
		} else {
			entry.observation.PreparingSlots++
		}
	}
	p.failureMu.Lock()
	for uid, failure := range p.failures {
		if time.Since(failure.at) > 15*time.Second {
			delete(p.failures, uid)
			continue
		}
		if seenFailed[uid] {
			continue
		}
		if entry := groups[failure.fingerprint]; entry != nil {
			entry.observation.FailedSlots++
		}
	}
	p.failureMu.Unlock()
	observedAt := time.Now().UTC()
	pools := make([]boxprovider.WarmPoolObservation, 0, len(groups))
	for _, entry := range groups {
		entry.observation.ObservedAt = observedAt
		pools = append(pools, entry.observation)
	}
	sort.Slice(pools, func(i, j int) bool { return pools[i].PoolID < pools[j].PoolID })
	if p.PublishObservation == nil {
		return fmt.Errorf("warm pool observation publisher is required")
	}
	p.PublishObservation(warmtemplates.Snapshot{NodeName: r.NodeName, DesiredHash: desiredHash, ObservedAt: observedAt, Pools: pools})
	return nil
}

func poolID(namespace, node, fingerprint string) string {
	h := sha256.Sum256([]byte(namespace + "\x00" + node + "\x00" + fingerprint))
	return hex.EncodeToString(h[:])
}

func (p *WarmPool) acquire(ctx context.Context, w *api.ResumablePod) (result *core.Pod, retErr error) {
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
		listTime := int64(0)
		if !listed.IsZero() {
			listTime = listed.Sub(locked).Milliseconds()
		}
		ctrl.LoggerFrom(ctx).Info("warm acquisition timing", "lockMs", locked.Sub(started).Milliseconds(), "listMs", listTime, "leaseMs", stateTime.Milliseconds(), "patchMs", patchTime.Milliseconds())
	}()
	r := p.Reconciler
	backend := r.Runtime.(warmRuntime)
	if p.ReadDesired == nil {
		return nil, fmt.Errorf("warm pool desired-template reader is required")
	}
	configured, _, err := p.ReadDesired(ctx)
	if err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "cannot read warm templates; using cold restore")
		return nil, nil
	}
	allowed := false
	for _, template := range configured {
		if template.Slots < 1 || template.Spec.NodeName != r.NodeName {
			continue
		}
		candidate := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{Name: template.ID, Namespace: p.Namespace, UID: types.UID("00000000-0000-4000-8000-000000000001")}, Spec: template.Spec}
		if validate(candidate, r.nodeBase()) == nil && fingerprint(candidate) == warmFingerprint(w) {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, nil
	}
	warmHash := warmFingerprint(w)
	if w.Spec.NodeName != r.NodeName {
		return nil, nil
	}
	local, localMode := r.Client.(*runtimecontrol.Client)
	if localMode {
		record, e := local.LocalRecord(ctx, client.ObjectKeyFromObject(w))
		if e != nil {
			return nil, e
		}
		localMode = record != nil
	}
	// Local admission uses background-discovered candidates and a durable node
	// lease. It does not contact Kubernetes to reserve an already running slot.
	var pods []core.Pod
	if localMode {
		for i := range p.cachedPods {
			pods = append(pods, *p.cachedPods[i].DeepCopy())
		}
	} else {
		pods, err = p.pods(ctx, &client.ListOptions{Raw: &meta.ListOptions{ResourceVersion: "0", ResourceVersionMatch: meta.ResourceVersionMatchNotOlderThan}})
	}
	listed = time.Now()
	if err != nil {
		return nil, err
	}
	for i := range pods {
		pod := &pods[i]
		if claimed, e := p.locallyClaimed(ctx, pod); e != nil {
			return nil, e
		} else if claimed {
			continue
		}
		if localMode && len(pod.Spec.SchedulingGates) != 0 {
			continue
		}
		owner := meta.GetControllerOf(pod)
		if owner != nil && !owned(w, pod) {
			continue
		}
		if pod.Annotations[warmSpec] != warmHash || pod.DeletionTimestamp != nil || !admittedDebugMount(w, pod, r.nodeBase()) || pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != r.runtimeClass() {
			continue
		}
		if owned(w, pod) {
			// Recovery of an interrupted adoption still verifies live ownership.
			current := &core.Pod{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return nil, err
			}
			if current.UID != pod.UID || !owned(w, current) || current.DeletionTimestamp != nil || current.Annotations[warmSpec] != warmHash {
				continue
			}
			pod = current
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
			if localMode {
				_, err := local.ClaimWarm(ctx, w, pod)
				if errors.Is(err, runtimecontrol.ErrPodClaimed) {
					continue
				}
				if err != nil {
					return nil, err
				}
				return pod, nil
			}
			old := pod.DeepCopy()
			if err = controllerutil.SetControllerReference(w, pod, r.Scheme()); err != nil {
				return nil, err
			}
			pod.Labels[api.OwnerLabel] = string(w.UID)
			patchStarted := time.Now()
			err = r.Patch(ctx, pod, client.MergeFromWithOptions(old, client.MergeFromWithOptimisticLock{}))
			patchTime += time.Since(patchStarted)
			if err != nil {
				if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
					continue // A cached candidate was already claimed or retired.
				}
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
