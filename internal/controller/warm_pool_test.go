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
	"cellbox.local/cellbox/internal/warmdefaults"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	core "k8s.io/api/core/v1"
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

func setWarmDefault(t *testing.T, r *Reconciler, w *api.ResumablePod, profileID string) {
	t.Helper()
	image := "registry.example.invalid/cellbox@sha256:" + strings.Repeat("a", 64)
	w.Spec.Container.Image = image
	if err := r.Update(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	w.Status.SpecHash = fingerprint(w)
	if err := r.Status().Update(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(warmdefaults.Image{ProfileID: profileID, NodeName: r.NodeName, Image: image})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Create(context.Background(), &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Namespace: w.Namespace, Name: warmdefaults.ConfigMapName}, Data: map[string]string{profileID: string(encoded)}}); err != nil {
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
	setWarmDefault(t, r, w, "default")
	w.Spec.PersistentHome = persistent
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
			t.Fatalf("adopt: %v", x.err)
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

func TestWarmPoolUsesOnlyConfiguredDefaultImagesAndSwitchDrainsUnusedSlots(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	setWarmDefault(t, r, w, "default")
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
		t.Fatalf("nondefault image slot was adopted: pod=%v err=%v", pod, err)
	}

	// Switch the target to a new immutable image while keeping the old CR alive.
	cm := &core.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: warmdefaults.ConfigMapName}, cm); err != nil {
		t.Fatal(err)
	}
	cm.Data["default"] = `{"profileId":"default","nodeName":"node","image":"` + newImage + `"}`
	if err := r.Update(ctx, cm); err != nil {
		t.Fatal(err)
	}
	if pod, err := pool.acquire(ctx, w); err != nil || pod != nil {
		t.Fatalf("stale image slot was adopted after default switch: pod=%v err=%v", pod, err)
	}
	if err := pool.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	var pods core.PodList
	if err := r.List(ctx, &pods, client.MatchingLabels{warmLabel: "true"}); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 || pods.Items[0].Annotations[warmSpec] != other.Status.SpecHash {
		t.Fatalf("pool did not switch exclusively to the new default image: %#v", pods.Items)
	}
}

func TestWarmPoolMissingDefaultDrainsUnclaimedAndKeepsClaimed(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	setWarmDefault(t, r, w, "default")
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
	if err := r.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: warmdefaults.ConfigMapName}, cm); err != nil {
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
