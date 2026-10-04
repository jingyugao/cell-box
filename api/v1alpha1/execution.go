package v1alpha1

import (
	"net"
	"strings"

	core "k8s.io/api/core/v1"
)

// Execution is a node-confirmed CRI execution, bound to one immutable Pod UID.
type Execution struct {
	PodUID      string `json:"podUID"`
	ContainerID string `json:"containerID"`
	PodIP       string `json:"podIP"`
}

func AvailableExecution(w *ResumablePod, p *core.Pod) bool {
	if w.Status.Execution != nil {
		return ConfirmedExecution(w, p)
	}
	return ExecutionStarted(p, w.Spec.Container.Name)
}

// ConfirmedExecution permits the CRI completion path before kubelet publishes
// Pod status. Any contradictory kubelet observation immediately closes it.
func ConfirmedExecution(w *ResumablePod, p *core.Pod) bool {
	e := w.Status.Execution
	if e == nil || e.PodUID != w.Status.PodUID || e.PodUID != string(p.UID) || e.ContainerID == "" || net.ParseIP(e.PodIP) == nil || p.DeletionTimestamp != nil || p.Status.Phase == core.PodFailed || p.Status.Phase == core.PodSucceeded {
		return false
	}
	if p.Status.PodIP != "" && p.Status.PodIP != e.PodIP {
		return false
	}
	for _, s := range p.Status.ContainerStatuses {
		if s.Name == w.Spec.Container.Name && (s.RestartCount != 0 || s.State.Terminated != nil || (s.ContainerID != "" && strings.TrimPrefix(s.ContainerID, "containerd://") != e.ContainerID)) {
			return false
		}
	}
	return true
}

// ExecutionStarted separates container execution from asynchronous Pod probes.
// Callers must also fence the Pod UID/owner and checkpoint invalidation state.
func ExecutionStarted(p *core.Pod, container string) bool {
	if p == nil || p.DeletionTimestamp != nil || p.Status.Phase != core.PodRunning || net.ParseIP(p.Status.PodIP) == nil {
		return false
	}
	for _, s := range p.Status.ContainerStatuses {
		if s.Name == container {
			return s.ContainerID != "" && s.RestartCount == 0 && s.State.Running != nil && s.State.Waiting == nil && s.State.Terminated == nil
		}
	}
	return false
}
