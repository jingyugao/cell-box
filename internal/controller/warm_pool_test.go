package controller

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"context"
	"fmt"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sync"
	"testing"
	"time"
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

func TestWarmPoolConcurrentAdoptionRestartAndTemplateMiss(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	backend := &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{}}
	r.Runtime = backend
	pool := &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 2}
	r.WarmPool = pool
	for i := 0; i < 2; i++ {
		pod := executionPod(w, fmt.Sprintf("warm-%d", i))
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
