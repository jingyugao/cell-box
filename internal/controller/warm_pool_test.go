package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/warmtemplates"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type poolRuntime struct {
	*fakeRuntime
	deadlines map[string]time.Time
}

type cachedCandidates struct {
	client.Client
	snapshot core.PodList
	reads    int
}

type noConfigMapClient struct {
	client.Client
	touched bool
}

func (c *noConfigMapClient) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*core.ConfigMap); ok {
		c.touched = true
		return fmt.Errorf("unexpected ConfigMap Get")
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *noConfigMapClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if _, ok := obj.(*core.ConfigMap); ok {
		c.touched = true
		return fmt.Errorf("unexpected ConfigMap Create")
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *noConfigMapClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, ok := obj.(*core.ConfigMap); ok {
		c.touched = true
		return fmt.Errorf("unexpected ConfigMap Update")
	}
	return c.Client.Update(ctx, obj, opts...)
}

func (c *noConfigMapClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if _, ok := obj.(*core.ConfigMap); ok {
		c.touched = true
		return fmt.Errorf("unexpected ConfigMap Delete")
	}
	return c.Client.Delete(ctx, obj, opts...)
}

func (c *noConfigMapClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*core.ConfigMapList); ok {
		c.touched = true
		return fmt.Errorf("unexpected ConfigMap List")
	}
	return c.Client.List(ctx, list, opts...)
}

func (c *cachedCandidates) List(ctx context.Context, list client.ObjectList, options ...client.ListOption) error {
	opts := &client.ListOptions{}
	for _, option := range options {
		option.ApplyToList(opts)
	}
	if pods, ok := list.(*core.PodList); ok && opts.Raw != nil && opts.Raw.ResourceVersion == "0" {
		c.reads++
		*pods = *c.snapshot.DeepCopy()
		return nil
	}
	return c.Client.List(ctx, list, options...)
}

func (*poolRuntime) RegisterWarm(context.Context, *core.Pod, string) error { return nil }
func (f *poolRuntime) WarmState(_ context.Context, p *core.Pod) (string, time.Time, error) {
	return "waiting", f.deadlines[string(p.UID)], nil
}
func (*poolRuntime) ActivateWarm(context.Context, *api.ResumablePod, *core.Pod) (bool, error) {
	return true, nil
}
func (*poolRuntime) ReapWarm(context.Context, string, map[string]bool) error { return nil }

func setWarmTemplate(t *testing.T, r *Reconciler, w *api.ResumablePod, templateID string) {
	t.Helper()
	templateSpec := w.Spec
	template := warmtemplates.Template{ID: templateID, Slots: 1, Spec: templateSpec}
	encoded, err := json.Marshal(template)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Create(context.Background(), &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Namespace: w.Namespace, Name: warmtemplates.ConfigMapName}, Data: map[string]string{templateID: string(encoded)}}); err != nil {
		t.Fatal(err)
	}
}

func replaceWarmTemplateSpec(t *testing.T, r *Reconciler, namespace, templateID string, spec api.Spec) {
	t.Helper()
	cm := &core.ConfigMap{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: warmtemplates.ConfigMapName}, cm); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(warmtemplates.Template{ID: templateID, Slots: 1, Spec: spec})
	if err != nil {
		t.Fatal(err)
	}
	cm.Data[templateID] = string(encoded)
	if err = r.Update(context.Background(), cm); err != nil {
		t.Fatal(err)
	}
}

func TestWarmPoolConcurrentAdoptionRestartAndTemplateMiss(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprintf("persistentHome=%v", persistent), func(t *testing.T) { testWarmPoolConcurrentAdoption(t, persistent) })
	}
}

func testWarmPoolConcurrentAdoption(t *testing.T, persistent bool) {
	ctx := context.Background()
	r, f, w := fixture(t)
	w.Spec.PersistentHome = persistent
	setWarmTemplate(t, r, w, "template-a")
	w.Status.SpecHash = fingerprint(w)
	backend := &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{}}
	r.Runtime = backend
	pool := &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 2}
	r.WarmPool = pool
	for i := 0; i < 2; i++ {
		pod := warmExecutionPod(w, fmt.Sprintf("warm-%d", i))
		pod.UID = types.UID(fmt.Sprintf("slot-%d", i))
		pod.Spec.SchedulingGates = nil
		pod.Labels = map[string]string{warmLabel: "true"}
		pod.Annotations = map[string]string{warmSpec: w.Status.SpecHash}
		if err := r.Create(ctx, pod); err != nil {
			t.Fatal(err)
		}
		backend.deadlines[string(pod.UID)] = time.Now().Add(time.Minute)
	}
	other := w.DeepCopy()
	other.Name = "other"
	other.UID = "87654321-bbbb"
	// Both claimers see the same stale cache snapshot. The second must skip
	// the first one's CAS conflict and adopt the other slot immediately.
	cached := &cachedCandidates{Client: r.Client}
	if err := r.List(ctx, &cached.snapshot, client.MatchingLabels{warmLabel: "true"}); err != nil {
		t.Fatal(err)
	}
	r.Client = cached
	type result struct {
		pod   *core.Pod
		err   error
		owner types.UID
	}
	out := make(chan result, 2)
	var wg sync.WaitGroup
	for _, owner := range []*api.ResumablePod{w, other} {
		wg.Add(1)
		go func(owner *api.ResumablePod) {
			defer wg.Done()
			pod, err := pool.acquire(ctx, owner)
			out <- result{pod, err, owner.UID}
		}(owner)
	}
	wg.Wait()
	close(out)
	seen := map[types.UID]bool{}
	for x := range out {
		if x.err != nil || x.pod == nil {
			configured, readErr := warmtemplates.Read(ctx, r, w.Namespace)
			var configuredHashes []string
			for _, template := range configured {
				candidate := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{Name: template.ID, Namespace: w.Namespace}, Spec: template.Spec}
				configuredHashes = append(configuredHashes, fingerprint(candidate)+" valid="+fmt.Sprint(validate(candidate)))
			}
			t.Fatalf("adopt: err=%v pod=%v read=%v configured=%v actualHash=%s valid=%v", x.err, x.pod, readErr, configuredHashes, w.Status.SpecHash, validate(w))
		}
		if seen[x.pod.UID] {
			t.Fatal("same Pod assigned twice")
		}
		seen[x.pod.UID] = true
		if meta.GetControllerOf(x.pod).UID != x.owner {
			t.Fatal("wrong owner")
		}
	}
	if cached.reads != 2 {
		t.Fatalf("cached candidate reads: %d", cached.reads)
	}
	r.Client = cached.Client
	restarted := &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 2}
	pod, err := restarted.acquire(ctx, w)
	if err != nil || pod == nil || meta.GetControllerOf(pod).UID != w.UID {
		t.Fatalf("lost adoption across restart: %v", err)
	}
	mismatch := w.DeepCopy()
	mismatch.UID = "different"
	mismatch.Status.SpecHash = "different-spec"
	if p, err := pool.acquire(ctx, mismatch); err != nil || p != nil {
		t.Fatalf("incompatible template reused: %v", err)
	}
	var pods core.PodList
	if err = r.List(ctx, &pods, client.MatchingLabels{warmLabel: "true"}); err != nil || len(pods.Items) != 2 {
		t.Fatal("unexpected extra Pods")
	}
}

func TestWarmPoolUsesIndependentTemplatesAndDrainsRemovedSlots(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	setWarmTemplate(t, r, w, "template-a")
	w.Spec.PersistentHome = false
	w.Status.SpecHash = fingerprint(w)
	backend := &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{}}
	r.Runtime = backend
	pool := &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 1}
	r.WarmPool = pool

	old := warmExecutionPod(w, "warm-old")
	old.UID = types.UID("warm-old-uid")
	old.Spec.SchedulingGates = nil
	old.Labels = map[string]string{warmLabel: "true"}
	old.Annotations = map[string]string{warmSpec: w.Status.SpecHash}
	if err := r.Create(ctx, old); err != nil {
		t.Fatal(err)
	}
	backend.deadlines[string(old.UID)] = time.Now().Add(time.Minute)
	newImage := "registry.example.invalid/cellbox@sha256:" + strings.Repeat("b", 64)
	other := w.DeepCopy()
	other.Name, other.UID = "other-image", types.UID("other-image-uid")
	other.ResourceVersion = ""
	other.Spec.Container.Image = newImage
	other.Status.SpecHash = fingerprint(other)
	if err := r.Create(ctx, other); err != nil {
		t.Fatal(err)
	}
	otherWarm := warmExecutionPod(other, "warm-other-image")
	otherWarm.UID = types.UID("warm-other-image-uid")
	otherWarm.Spec.SchedulingGates = nil
	otherWarm.Labels = map[string]string{warmLabel: "true"}
	otherWarm.Annotations = map[string]string{warmSpec: other.Status.SpecHash}
	if err := r.Create(ctx, otherWarm); err != nil {
		t.Fatal(err)
	}
	backend.deadlines[string(otherWarm.UID)] = time.Now().Add(time.Minute)
	if pod, err := pool.acquire(ctx, other); err != nil || pod != nil {
		t.Fatalf("unconfigured template slot was adopted: pod=%v err=%v", pod, err)
	}

	// Replace the target with a different complete runtime template.
	cm := &core.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: warmtemplates.ConfigMapName}, cm); err != nil {
		t.Fatal(err)
	}
	newSpec := w.Spec
	newSpec.Container.Image = newImage
	newTemplate := warmtemplates.Template{ID: "new-template", Slots: 1, Spec: newSpec}
	encoded, _ := json.Marshal(newTemplate)
	cm.Data = map[string]string{"new-template": string(encoded)}
	if err := r.Update(ctx, cm); err != nil {
		t.Fatal(err)
	}
	if pod, err := pool.acquire(ctx, w); err != nil || pod != nil {
		t.Fatalf("removed template slot was adopted: pod=%v err=%v", pod, err)
	}
	if err := pool.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	var pods core.PodList
	if err := r.List(ctx, &pods, client.MatchingLabels{warmLabel: "true"}); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 || pods.Items[0].Spec.Containers[0].Image != newImage {
		t.Fatalf("pool did not switch exclusively to the new template: %#v", pods.Items)
	}
}

func TestWarmPoolPreparesTemplateWithoutAnyBoxAndSharesIdenticalSpecs(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	if err := r.Delete(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, &core.Pod{ObjectMeta: meta.ObjectMeta{Namespace: w.Namespace, Name: "old"}}); err != nil {
		t.Fatal(err)
	}
	backend := &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{}}
	r.Runtime = backend
	pool := &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 2}
	w.Spec.DesiredState = "Running"
	spec := w.Spec
	first := warmtemplates.Template{ID: "template-a", Slots: 1, Spec: spec}
	second := warmtemplates.Template{ID: "template-b", Slots: 1, Spec: spec}
	data := map[string]string{}
	for _, template := range []warmtemplates.Template{first, second} {
		encoded, _ := json.Marshal(template)
		data[template.ID] = string(encoded)
	}
	if err := r.Create(ctx, &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Namespace: w.Namespace, Name: warmtemplates.ConfigMapName}, Data: data}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err := pool.reconcile(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var pods core.PodList
	if err := r.List(ctx, &pods, client.MatchingLabels{warmLabel: "true"}); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("two templates with one identical Spec should share two node slots; got %d", len(pods.Items))
	}
	for _, pod := range pods.Items {
		if pod.Spec.Containers[0].Image != spec.Container.Image || pod.Spec.NodeSelector["kubernetes.io/hostname"] != spec.NodeName {
			t.Fatalf("warm Pod does not match configured runtime Spec: %#v", pod.Spec)
		}
	}
}

func TestWarmPoolPublishesPerNodeSnapshotWithCapacityAndConfiguredResources(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	w.Spec.Container.Resources = core.ResourceRequirements{Requests: core.ResourceList{core.ResourceCPU: resource.MustParse("500m"), core.ResourceMemory: resource.MustParse("128Mi")}, Limits: core.ResourceList{core.ResourceCPU: resource.MustParse("1"), core.ResourceMemory: resource.MustParse("256Mi")}}
	setWarmTemplate(t, r, w, "revision-one")
	w.Status.SpecHash = fingerprint(w)
	backend := &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{}}
	r.Runtime = backend
	pool := &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 2}
	for i := 0; i < 1; i++ {
		pod := warmExecutionPod(w, "warm-observed")
		pod.UID = "warm-observed-uid"
		pod.Spec.SchedulingGates = nil
		pod.Labels = map[string]string{warmLabel: "true"}
		pod.Annotations = map[string]string{warmSpec: w.Status.SpecHash}
		if err := r.Create(ctx, pod); err != nil {
			t.Fatal(err)
		}
		backend.deadlines[string(pod.UID)] = time.Now().Add(time.Minute)
	}
	if err := pool.publishSnapshot(ctx); err != nil {
		t.Fatal(err)
	}
	cm := &core.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: warmtemplates.StatusConfigMapName(r.NodeName)}, cm); err != nil {
		t.Fatal(err)
	}
	var snapshot warmtemplates.Snapshot
	if err := json.Unmarshal([]byte(cm.Data["snapshot.json"]), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.NodeName != r.NodeName || len(snapshot.Pools) != 1 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	got := snapshot.Pools[0]
	if got.PoolID == "" || got.DesiredHash != snapshot.DesiredHash || len(got.TemplateIDs) != 1 || got.TemplateIDs[0] != "revision-one" || got.RequestedSlots != 1 || got.AllocatedSlots != 1 || got.ReadySlots != 1 || got.PreparingSlots != 0 || got.FailedSlots != 0 || got.Resources.CPURequestMilli <= 0 || got.Resources.MemoryRequestBytes <= 0 || got.ObservedAt.IsZero() {
		t.Fatalf("incomplete pool observation: %#v", got)
	}
	// A new gated Pod has no registered ticket yet and must be preparing,
	// regardless of what the runtime probe would report for an existing ticket.
	gated := warmExecutionPod(w, "warm-not-registered")
	gated.UID = "warm-not-registered-uid"
	gated.Labels = map[string]string{warmLabel: "true"}
	gated.Annotations = map[string]string{warmSpec: w.Status.SpecHash}
	if err := r.Create(ctx, gated); err != nil {
		t.Fatal(err)
	}
	if err := pool.publishSnapshot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: warmtemplates.StatusConfigMapName(r.NodeName)}, cm); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(cm.Data["snapshot.json"]), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Pools[0].ReadySlots != 1 || snapshot.Pools[0].PreparingSlots != 1 {
		t.Fatalf("unregistered Pod counted as ready: %+v", snapshot.Pools[0])
	}
}

func TestLocalWarmPoolWaitsForInitialConfigurationWithoutKubernetesConfigMaps(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	originalClient := r.Client
	configMapGuard := &noConfigMapClient{Client: originalClient}
	r.Client = configMapGuard
	backend := &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{}}
	r.Runtime = backend
	store := warmtemplates.NewLocalStore()
	pool := &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 2, LocalStore: store}
	warm := warmExecutionPod(w, "warm-existing")
	warm.UID = "warm-existing-uid"
	warm.Spec.SchedulingGates = nil
	warm.Labels = map[string]string{warmLabel: "true"}
	if err := r.Create(ctx, warm); err != nil {
		t.Fatal(err)
	}
	if err := pool.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	var pods core.PodList
	if err := r.List(ctx, &pods, client.InNamespace(w.Namespace), client.MatchingLabels{warmLabel: "true"}); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 || pods.Items[0].UID != warm.UID {
		t.Fatalf("uninitialized store cleared existing warm slot: %#v", pods.Items)
	}
	if err := store.Set(w.Namespace, nil); err != nil {
		t.Fatal(err)
	}
	if err := pool.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if configMapGuard.touched {
		t.Fatal("local warm pool performed ConfigMap I/O")
	}
	r.Client = originalClient
	if err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: warmtemplates.StatusConfigMapName(r.NodeName)}, &core.ConfigMap{}); err == nil {
		t.Fatal("local empty-demand acknowledgement was written to Kubernetes instead of the local store")
	}
	if snapshot, ok := store.Snapshot(w.Namespace, r.NodeName); !ok || snapshot.DesiredHash == "" || snapshot.ObservedGeneration == 0 || len(snapshot.Pools) != 0 {
		t.Fatalf("initialized empty config was not acknowledged locally: %#v exists=%v", snapshot, ok)
	}
}

func TestLocalWarmPoolAcquireUsesConfiguredSlotWithoutConfigMapIO(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	w.Status.SpecHash = fingerprint(w)
	store := warmtemplates.NewLocalStore()
	if err := store.Set(w.Namespace, []warmtemplates.Template{{ID: "revision-one", Slots: 1, Spec: w.Spec}}); err != nil {
		t.Fatal(err)
	}
	backend := &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{}}
	r.Runtime = backend
	warm := warmExecutionPod(w, "warm-local-acquire")
	warm.UID = types.UID("warm-local-acquire-uid")
	warm.Spec.SchedulingGates = nil
	warm.Labels = map[string]string{warmLabel: "true"}
	warm.Annotations = map[string]string{warmSpec: w.Status.SpecHash}
	if err := r.Create(ctx, warm); err != nil {
		t.Fatal(err)
	}
	backend.deadlines[string(warm.UID)] = time.Now().Add(time.Minute)
	originalClient := r.Client
	guard := &noConfigMapClient{Client: originalClient}
	r.Client = guard
	pool := &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 1, LocalStore: store}
	got, err := pool.acquire(ctx, w)
	r.Client = originalClient
	if err != nil || got == nil || got.UID != warm.UID || meta.GetControllerOf(got) == nil || meta.GetControllerOf(got).UID != w.UID {
		t.Fatalf("local configured warm slot was not adopted: pod=%#v err=%v", got, err)
	}
	if guard.touched {
		t.Fatal("warm-slot acquisition performed Kubernetes ConfigMap I/O")
	}
}

func TestWarmPoolScaleDownKeepsClaimedPodAndPublishesAllocatedBudget(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	setWarmTemplate(t, r, w, "revision-one")
	cm := &core.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: warmtemplates.ConfigMapName}, cm); err != nil {
		t.Fatal(err)
	}
	var configured warmtemplates.Template
	if err := json.Unmarshal([]byte(cm.Data["revision-one"]), &configured); err != nil {
		t.Fatal(err)
	}
	configured.Slots = 3
	encoded, err := json.Marshal(configured)
	if err != nil {
		t.Fatal(err)
	}
	cm.Data["revision-one"] = string(encoded)
	if err := r.Update(ctx, cm); err != nil {
		t.Fatal(err)
	}
	w.Status.SpecHash = fingerprint(w)
	backend := &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{}}
	r.Runtime = backend
	claimed := warmExecutionPod(w, "warm-claimed")
	claimed.UID = "warm-claimed-uid"
	claimed.Spec.SchedulingGates = nil
	claimed.Labels = map[string]string{warmLabel: "true"}
	claimed.Annotations = map[string]string{warmSpec: w.Status.SpecHash}
	if err := controllerutil.SetControllerReference(w, claimed, r.Scheme()); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	backend.deadlines[string(claimed.UID)] = time.Now().Add(time.Minute)
	for i := 0; i < 2; i++ {
		pod := warmExecutionPod(w, fmt.Sprintf("warm-idle-%d", i))
		pod.UID = types.UID(fmt.Sprintf("warm-idle-uid-%d", i))
		pod.Spec.SchedulingGates = nil
		pod.Labels = map[string]string{warmLabel: "true"}
		pod.Annotations = map[string]string{warmSpec: w.Status.SpecHash}
		if err := r.Create(ctx, pod); err != nil {
			t.Fatal(err)
		}
		backend.deadlines[string(pod.UID)] = time.Now().Add(time.Minute)
	}
	pool := &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 1}
	if err := pool.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(claimed), claimed); err != nil {
		t.Fatalf("claimed Pod was removed during idle scale-down: %v", err)
	}
	var pods core.PodList
	if err := r.List(ctx, &pods, client.MatchingLabels{warmLabel: "true"}); err != nil {
		t.Fatal(err)
	}
	idle := 0
	for i := range pods.Items {
		if meta.GetControllerOf(&pods.Items[i]) == nil {
			idle++
		}
	}
	if idle != 1 {
		t.Fatalf("idle pool after scale-down = %d, want 1", idle)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: warmtemplates.StatusConfigMapName(r.NodeName)}, cm); err != nil {
		t.Fatal(err)
	}
	var snapshot warmtemplates.Snapshot
	if err := json.Unmarshal([]byte(cm.Data["snapshot.json"]), &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Pools) != 1 || snapshot.Pools[0].RequestedSlots != 3 || snapshot.Pools[0].AllocatedSlots != 1 || snapshot.Pools[0].ReadySlots != 1 {
		t.Fatalf("scale-down snapshot = %#v", snapshot.Pools)
	}
}

func TestWarmPoolFairlyDistributesConfiguredNodeBudget(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	if err := r.Delete(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, &core.Pod{ObjectMeta: meta.ObjectMeta{Namespace: w.Namespace, Name: "old"}}); err != nil {
		t.Fatal(err)
	}
	backend := &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{}}
	r.Runtime = backend
	pool := &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 6}
	first := w.Spec
	first.Container.Image = "registry.example.invalid/first@sha256:" + strings.Repeat("a", 64)
	first.Container.ImagePullPolicy = core.PullIfNotPresent
	second := first
	second.Container = *first.Container.DeepCopy()
	second.Container.Image = "registry.example.invalid/second@sha256:" + strings.Repeat("b", 64)
	data := map[string]string{}
	for _, template := range []warmtemplates.Template{
		{ID: "template-a", Slots: 3, Spec: first},
		{ID: "template-b", Slots: 3, Spec: second},
	} {
		encoded, _ := json.Marshal(template)
		data[template.ID] = string(encoded)
	}
	if err := r.Create(ctx, &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Namespace: w.Namespace, Name: warmtemplates.ConfigMapName}, Data: data}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		if err := pool.reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		var current core.PodList
		if err := r.List(ctx, &current, client.MatchingLabels{warmLabel: "true"}); err != nil {
			t.Fatal(err)
		}
		for _, pod := range current.Items {
			backend.deadlines[string(pod.UID)] = time.Now().Add(time.Minute)
		}
	}
	var pods core.PodList
	if err := r.List(ctx, &pods, client.MatchingLabels{warmLabel: "true"}); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	var hashes []string
	for _, pod := range pods.Items {
		counts[pod.Spec.Containers[0].Image]++
		hashes = append(hashes, pod.Annotations[warmSpec]+"="+pod.Spec.Containers[0].Image)
	}
	if len(pods.Items) != 6 || counts[first.Container.Image] != 3 || counts[second.Container.Image] != 3 {
		t.Fatalf("node budget should be max-min distributed across distinct templates: total=%d counts=%v pods=%v", len(pods.Items), counts, hashes)
	}

	// Shrinking the node budget must promptly retire the excess slots from both
	// templates so the fair share is represented by the actual pool.
	pool.Size = 4
	for i := 0; i < 12; i++ {
		if err := pool.reconcile(ctx); err != nil {
			t.Fatal(err)
		}
		var current core.PodList
		if err := r.List(ctx, &current, client.MatchingLabels{warmLabel: "true"}); err != nil {
			t.Fatal(err)
		}
		for _, pod := range current.Items {
			backend.deadlines[string(pod.UID)] = time.Now().Add(time.Minute)
		}
	}
	if err := r.List(ctx, &pods, client.MatchingLabels{warmLabel: "true"}); err != nil {
		t.Fatal(err)
	}
	counts = map[string]int{}
	for _, pod := range pods.Items {
		counts[pod.Spec.Containers[0].Image]++
	}
	if len(pods.Items) != 4 || counts[first.Container.Image] != 2 || counts[second.Container.Image] != 2 {
		t.Fatalf("shrunk budget did not retire excess slots while preserving fair shares: total=%d counts=%v", len(pods.Items), counts)
	}
}

func TestAllocateWarmSlotsUsesFairNodeBudget(t *testing.T) {
	if got := allocateWarmSlots(map[string]int{"a": 3, "b": 3}, 4); got["a"] != 2 || got["b"] != 2 {
		t.Fatalf("equal demands should share budget equally: %v", got)
	}
	if got := allocateWarmSlots(map[string]int{"a": 1, "b": 5}, 4); got["a"] != 1 || got["b"] != 3 {
		t.Fatalf("unused demand should flow to the other template: %v", got)
	}
	if got := allocateWarmSlots(map[string]int{"a": 4, "b": 4, "c": 4}, 2); got["a"]+got["b"]+got["c"] != 2 {
		t.Fatalf("allocation exceeded node budget: %v", got)
	}
}

func TestWarmPoolMissingDefaultDrainsUnclaimedAndKeepsClaimed(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	setWarmTemplate(t, r, w, "template-a")
	backend := &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{}}
	r.Runtime = backend
	pool := &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 1}
	claimed := warmExecutionPod(w, "warm-claimed")
	claimed.UID = types.UID("warm-claimed-uid")
	claimed.Spec.SchedulingGates = nil
	claimed.Labels = map[string]string{warmLabel: "true", api.OwnerLabel: string(w.UID)}
	claimed.Annotations = map[string]string{warmSpec: w.Status.SpecHash}
	if err := controllerutil.SetControllerReference(w, claimed, r.Scheme()); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	unclaimed := warmExecutionPod(w, "warm-unclaimed")
	unclaimed.UID = types.UID("warm-unclaimed-uid")
	unclaimed.Spec.SchedulingGates = nil
	unclaimed.Labels = map[string]string{warmLabel: "true"}
	unclaimed.Annotations = map[string]string{warmSpec: w.Status.SpecHash}
	if err := r.Create(ctx, unclaimed); err != nil {
		t.Fatal(err)
	}
	backend.deadlines[string(claimed.UID)] = time.Now().Add(time.Minute)
	backend.deadlines[string(unclaimed.UID)] = time.Now().Add(time.Minute)
	cm := &core.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: warmtemplates.ConfigMapName}, cm); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, cm); err != nil {
		t.Fatal(err)
	}
	if pod, err := pool.acquire(ctx, w); err != nil || pod != nil {
		t.Fatalf("missing target must disable adoption: pod=%v err=%v", pod, err)
	}
	if err := pool.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: claimed.Name}, &core.Pod{}); err != nil {
		t.Fatalf("claimed box was removed without a default target: %v", err)
	}
	if err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: unclaimed.Name}, &core.Pod{}); err == nil {
		t.Fatal("unclaimed warm slot was not drained with no configured default")
	}
}
