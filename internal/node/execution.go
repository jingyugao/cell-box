package node

import (
	"context"
	"fmt"
	"net"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	core "k8s.io/api/core/v1"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// WaitWarmStarted reads the local CRI socket, avoiding kubelet's asynchronous
// Pod-status publication. Only actively restoring slots are polled, bounded
// to one second; slower restores are retried by normal reconciliation.
func (b *Backend) WaitWarmStarted(ctx context.Context, w *api.ResumablePod, p *core.Pod) (*api.Execution, error) {
	lease, err := b.WarmStatus(ctx, p)
	if err != nil {
		return nil, err
	}
	if lease.Ticket.OwnerUID != string(w.UID) || lease.Ticket.PodUID != string(p.UID) || lease.Ticket.Snapshot != w.Status.Snapshot || lease.Ticket.SpecHash != w.Status.SpecHash || lease.SID == "" || (lease.Phase != "claimed" && lease.Phase != "restoring") {
		return nil, fmt.Errorf("warm execution identity changed")
	}
	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		e, err := b.warmExecution(waitCtx, w, p, lease.SID)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if waitCtx.Err() != nil {
			return nil, nil
		}
		if err != nil || e != nil {
			return e, err
		}
		select {
		case <-waitCtx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *Backend) warmExecution(ctx context.Context, w *api.ResumablePod, p *core.Pod, sid string) (*api.Execution, error) {
	containers, err := b.Runtime.ListContainers(ctx, &cri.ListContainersRequest{Filter: &cri.ContainerFilter{PodSandboxId: sid}})
	if err != nil {
		return nil, err
	}
	if len(containers.Containers) == 0 {
		return nil, nil
	}
	if len(containers.Containers) != 1 {
		return nil, fmt.Errorf("ambiguous warm execution containers")
	}
	c := containers.Containers[0]
	if c.Metadata == nil || c.Metadata.Name != w.Spec.Container.Name || c.Metadata.Attempt != 0 || c.PodSandboxId != sid || c.Id == "" || c.Labels["io.kubernetes.pod.uid"] != string(p.UID) {
		return nil, fmt.Errorf("warm container identity changed")
	}
	if c.State != cri.ContainerState_CONTAINER_RUNNING {
		return nil, nil
	}
	sandbox, err := b.Runtime.PodSandboxStatus(ctx, &cri.PodSandboxStatusRequest{PodSandboxId: sid})
	if err != nil {
		return nil, err
	}
	s := sandbox.Status
	if s == nil || s.Id != sid || s.Metadata == nil || s.Metadata.Uid != string(p.UID) || s.Metadata.Name != p.Name || s.Metadata.Namespace != p.Namespace || s.Metadata.Attempt != 0 || s.RuntimeHandler != api.RuntimeClass {
		return nil, fmt.Errorf("warm sandbox identity changed")
	}
	if s.State != cri.PodSandboxState_SANDBOX_READY || s.Network == nil || net.ParseIP(s.Network.Ip) == nil {
		return nil, nil
	}
	return &api.Execution{PodUID: string(p.UID), ContainerID: c.Id, PodIP: s.Network.Ip}, nil
}
