package v1alpha1

import (
	"testing"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRuntimeCompletionFencesDelayedPodStatus(t *testing.T) {
	w := &ResumablePod{Spec: Spec{Container: core.Container{Name: "guest"}}, Status: Status{PodUID: "pod", Execution: &Execution{PodUID: "pod", ContainerID: "container", PodIP: "10.42.0.2"}}}
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod"}, Status: core.PodStatus{Phase: core.PodPending}}
	if !AvailableExecution(w, p) {
		t.Fatal("confirmed CRI execution waits for kubelet")
	}
	p.Status = core.PodStatus{Phase: core.PodRunning, PodIP: "10.42.0.2", ContainerStatuses: []core.ContainerStatus{{Name: "guest", ContainerID: "containerd://container", State: core.ContainerState{Running: &core.ContainerStateRunning{}}}}}
	if !AvailableExecution(w, p) {
		t.Fatal("matching kubelet observation rejected")
	}
	for name, mutate := range map[string]func(*core.Pod){
		"different UID":       func(p *core.Pod) { p.UID = "replacement" },
		"different IP":        func(p *core.Pod) { p.Status.PodIP = "10.42.0.3" },
		"different container": func(p *core.Pod) { p.Status.ContainerStatuses[0].ContainerID = "containerd://replacement" },
		"restart":             func(p *core.Pod) { p.Status.ContainerStatuses[0].RestartCount = 1 },
		"terminated":          func(p *core.Pod) { p.Status.ContainerStatuses[0].State.Terminated = &core.ContainerStateTerminated{} },
		"failed Pod":          func(p *core.Pod) { p.Status.Phase = core.PodFailed },
		"deleted Pod":         func(p *core.Pod) { now := meta.Now(); p.DeletionTimestamp = &now },
	} {
		t.Run(name, func(t *testing.T) {
			changed := p.DeepCopy()
			mutate(changed)
			if AvailableExecution(w, changed) {
				t.Fatal("contradictory observation accepted")
			}
		})
	}
}
