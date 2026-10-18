// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/warmtemplates"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPersistentHomeWarmMissKeepsColdMount(t *testing.T) {
	for _, size := range []int{0, 2} {
		t.Run(fmt.Sprintf("pool-size-%d", size), func(t *testing.T) {
			ctx := context.Background()
			r, f, w := fixture(t)
			w.Spec.PersistentHome = true
			if err := r.Update(ctx, w); err != nil {
				t.Fatal(err)
			}
			w.Status.SpecHash = fingerprint(w)
			w.Status.Snapshot = "checkpoint-1"
			w.Status.Phase = "Suspended"
			if err := r.Status().Update(ctx, w); err != nil {
				t.Fatal(err)
			}
			backend := &persistentHomePoolRuntime{poolRuntime: &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{}}}
			r.Runtime = backend
			r.WarmPool = &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: size, ReadDesired: func(context.Context) ([]warmtemplates.Template, string, error) {
				hash, err := warmtemplates.HashTemplates(nil)
				return nil, hash, err
			}}
			if _, err := r.begin(ctx, w); err != nil {
				t.Fatal(err)
			}
			step(t, r, w)
			p := &core.Pod{}
			if err := r.Get(ctx, client.ObjectKey{Namespace: w.Namespace, Name: w.Status.PodName}, p); err != nil {
				t.Fatal(err)
			}
			if p.Labels[warmLabel] == "true" || backend.activateCalls != 0 || !admittedDebugMount(w, p) {
				t.Fatal("pool miss did not use the ordinary cold Pod")
			}
			found := false
			for _, v := range p.Spec.Volumes {
				if v.Name == "agent-home" {
					found = true
					if v.HostPath == nil || v.HostPath.Path != "/var/lib/cellbox/homes/"+string(w.UID)+"/data" {
						t.Fatal("cold fallback mounted another HOME")
					}
				}
			}
			if !found {
				t.Fatal("cold fallback lost its HOME mount")
			}
		})
	}
}

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

func TestPersistentHomeWarmPodHasNoTemplateHomeAndCanBeClaimed(t *testing.T) {
	ctx := context.Background()
	r, f, w := fixture(t)
	w.Spec.PersistentHome = true
	w.Spec.DesiredState = "Running"
	template := warmtemplates.Template{ID: "template-a", Slots: 1, Spec: w.Spec}
	templateHash, err := warmtemplates.HashTemplates([]warmtemplates.Template{template})
	if err != nil {
		t.Fatal(err)
	}
	oldTemplate := w.DeepCopy()
	oldTemplate.UID = "normal-box"
	oldTemplate.Status.SpecHash = fingerprint(oldTemplate)
	warm := warmExecutionPod(oldTemplate, "warm-old")
	warm.UID = types.UID("warm-slot")
	warm.Spec.SchedulingGates = nil
	warm.Labels = map[string]string{warmLabel: "true"}
	warm.Annotations = map[string]string{warmSpec: oldTemplate.Status.SpecHash}
	if err := r.Create(ctx, warm); err != nil {
		t.Fatal(err)
	}
	if err := r.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase = "Suspended"
	w.Status.Snapshot = "checkpoint-1"
	w.Status.SpecHash = fingerprint(w)
	if err := r.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	poolRuntime := &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{"warm-slot": time.Now().Add(time.Minute)}}
	backend := &persistentHomePoolRuntime{poolRuntime: poolRuntime}
	r.Runtime = backend
	pool := &WarmPool{Reconciler: r, Namespace: w.Namespace, Size: 2, ReadDesired: func(context.Context) ([]warmtemplates.Template, string, error) {
		return []warmtemplates.Template{template}, templateHash, nil
	}, PublishObservation: func(warmtemplates.Snapshot) {}}
	r.WarmPool = pool

	if pod, err := pool.acquire(ctx, w); err != nil || pod == nil {
		t.Fatalf("persistent-home restore should reuse its template slot without mounting HOME in the pooled Pod: pod=%v err=%v", pod, err)
	}
	if err := pool.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	var pods core.PodList
	if err := r.List(ctx, &pods, client.MatchingLabels{warmLabel: "true"}); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 2 {
		t.Fatalf("persistent-home claim should leave its shared template capacity replenished: got %d Pods", len(pods.Items))
	}
	if backend.activateCalls != 0 {
		t.Fatalf("pool maintenance activated a workload before Box restore: activations=%d", backend.activateCalls)
	}
	if f.cleanups != 0 {
		t.Fatalf("a valid template-level slot should remain available to compatible Boxes: cleanup count = %d", f.cleanups)
	}
	var prepared *core.Pod
	for i := range pods.Items {
		candidate := &pods.Items[i]
		if !admittedDebugMount(w, candidate) {
			t.Fatal("template Pod has unexpected mounts")
		}
		for _, v := range candidate.Spec.Volumes {
			if v.Name == "agent-home" {
				t.Fatal("warm Pod exposed a Box HOME")
			}
		}
		if owned(w, candidate) {
			prepared = candidate
		}
	}
	if prepared == nil {
		t.Fatal("persistent-home Box did not claim its compatible warm Pod")
	}
	if _, err := r.begin(ctx, w); err != nil {
		t.Fatal(err)
	}
	claimed := &core.Pod{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: w.Namespace, Name: w.Status.PodName}, claimed); err != nil {
		t.Fatal(err)
	}
	if claimed.UID != prepared.UID || w.Status.Phase != "Restoring" || backend.activateCalls != 1 {
		t.Fatalf("persistent HOME did not activate warm restore: uid=%s phase=%s activations=%d", claimed.UID, w.Status.Phase, backend.activateCalls)
	}
	if !admittedDebugMount(w, claimed) {
		t.Fatal("claimed warm Pod must retain its HOME-free Pod spec")
	}
	tampered := claimed.DeepCopy()
	tampered.Spec.Volumes = hostVolumes(w)
	tampered.Spec.Containers[0].VolumeMounts = hostMounts(w)
	if admittedDebugMount(w, tampered) {
		t.Fatal("warm Pod containing a template HOME was admitted")
	}
}
