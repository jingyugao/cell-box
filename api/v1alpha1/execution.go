package v1alpha1

import (
	"net"

	core "k8s.io/api/core/v1"
)

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
