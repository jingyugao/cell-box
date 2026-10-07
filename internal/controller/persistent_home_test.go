// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/homevolume"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPersistentHomePodMountAndFingerprint(t *testing.T) {
	_, _, w := fixture(t)
	legacyHash := fingerprint(w)
	if err := validate(w); err != nil {
		t.Fatalf("legacy workload invalid: %v", err)
	}

	w.Spec.PersistentHome = true
	homeHash := fingerprint(w)
	if homeHash == legacyHash {
		t.Fatal("enabling persistentHome must change the immutable fingerprint")
	}
	w.Status.SpecHash = legacyHash
	if err := validate(w); err == nil {
		t.Fatal("persistentHome mode change was accepted against the old fingerprint")
	}
	w.Spec.PersistentHome = false
	if got := fingerprint(w); got != legacyHash {
		t.Fatal("disabled persistentHome changed the legacy fingerprint")
	}
	if err := validate(w); err != nil {
		t.Fatalf("legacy workload fingerprint changed: %v", err)
	}

	w.Spec.PersistentHome = true
	w.Status.SpecHash = homeHash
	if err := validate(w); err != nil {
		t.Fatalf("persistent-home workload invalid: %v", err)
	}
	pod := executionPod(w, "new-execution")
	wantPath, err := homevolume.Path(homevolume.DefaultBase, string(w.UID))
	if err != nil {
		t.Fatal(err)
	}

	var homeVolume *core.Volume
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == "agent-home" {
			homeVolume = &pod.Spec.Volumes[i]
			break
		}
	}
	if homeVolume == nil || homeVolume.HostPath == nil || homeVolume.HostPath.Path != wantPath || homeVolume.HostPath.Type == nil || *homeVolume.HostPath.Type != core.HostPathDirectory {
		t.Fatalf("HOME must be this Box's pre-existing host directory: got %#v, want %q with HostPathDirectory", homeVolume, wantPath)
	}
	if homeVolume.HostPath.Type == nil || *homeVolume.HostPath.Type == core.HostPathDirectoryOrCreate {
		t.Fatal("HOME mount must not create an uninitialized directory")
	}
	var homeMount *core.VolumeMount
	for i := range pod.Spec.Containers[0].VolumeMounts {
		if pod.Spec.Containers[0].VolumeMounts[i].Name == "agent-home" {
			homeMount = &pod.Spec.Containers[0].VolumeMounts[i]
			break
		}
	}
	if homeMount == nil || homeMount.MountPath != homevolume.MountPath || homeMount.SubPath != "" || homeMount.ReadOnly {
		t.Fatalf("unexpected HOME mount: %#v", homeMount)
	}
	if !admittedDebugMount(w, pod) {
		t.Fatal("generated persistent-home Pod did not pass mount admission")
	}

	tampered := pod.DeepCopy()
	tampered.Spec.Volumes[0].HostPath.Path = "/var/lib/cellbox/homes/another-box/data"
	if admittedDebugMount(w, tampered) {
		t.Fatal("Pod mounting another Box's HOME was admitted")
	}
}

type persistentHomePoolRuntime struct {
	*poolRuntime
	registerCalls int
	activateCalls int
}

func (r *persistentHomePoolRuntime) RegisterWarm(context.Context, *core.Pod, string) error {
	r.registerCalls++
	return nil
}

func (r *persistentHomePoolRuntime) ActivateWarm(context.Context, *api.ResumablePod, *core.Pod) (bool, error) {
	r.activateCalls++
	return true, nil
}

func TestPersistentHomeNeverUsesOrCreatesSharedWarmPods(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	oldTemplate := w.DeepCopy()
	oldTemplate.UID = "normal-box"
	oldTemplate.Status.SpecHash = fingerprint(oldTemplate)
	warm := executionPod(oldTemplate, "warm-old")
	warm.UID = types.UID("warm-slot")
	warm.Spec.SchedulingGates = nil
	warm.Labels = map[string]string{warmLabel: "true"}
	warm.Annotations = map[string]string{warmSpec: oldTemplate.Status.SpecHash}
	if err := r.Create(ctx, warm); err != nil {
		t.Fatal(err)
	}

	w.Spec.PersistentHome = true
	w.Spec.DesiredState = "Running"
	w.Status.Phase = "Suspended"
	w.Status.Snapshot = "checkpoint-1"
	w.Status.SpecHash = fingerprint(w)
	if err := r.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := r.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	poolRuntime := &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{"warm-slot": time.Now().Add(time.Minute)}}
	backend := &persistentHomePoolRuntime{poolRuntime: poolRuntime}
	r.Runtime = backend
	pool := &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 2}
	r.WarmPool = pool

	if pod, err := pool.acquire(ctx, w); err != nil || pod != nil {
		t.Fatalf("persistent-home restore adopted a shared warm Pod: pod=%v err=%v", pod, err)
	}
	if err := pool.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	var pods core.PodList
	if err := r.List(ctx, &pods, client.MatchingLabels{warmLabel: "true"}); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("persistent-home-only pool retained or created warm Pods: %s", fmt.Sprint(pods.Items))
	}
	if backend.registerCalls != 0 || backend.activateCalls != 0 {
		t.Fatalf("persistent HOME touched warm runtime: register=%d activate=%d", backend.registerCalls, backend.activateCalls)
	}
	if f.cleanups != 1 {
		t.Fatalf("stale shared warm Pod cleanup count = %d, want 1", f.cleanups)
	}
}
