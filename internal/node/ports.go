package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/guestapi"
	"github.com/jingyugao/cell-box/internal/portwait"
	core "k8s.io/api/core/v1"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var ErrExecutionLost = errors.New("execution exited or was replaced")
var ErrPortsNotListening = errors.New("required ports are not listening")

func requestedPorts(w *api.ResumablePod) []int {
	if len(w.Spec.WaitPorts) == 0 {
		return []int{guestapi.Port}
	}
	return w.Spec.WaitPorts
}

func (b *Backend) portMonitor() (*portwait.Monitor, error) {
	b.portsMu.Lock()
	defer b.portsMu.Unlock()
	if b.closed {
		return nil, errors.New("node backend is closed")
	}
	if b.ports == nil {
		path := "/sys/fs/cgroup"
		if b.HostMountNamespace {
			path = "/proc/1/root/sys/fs/cgroup"
		}
		monitor, err := portwait.New(path)
		if err != nil {
			return nil, fmt.Errorf("attach listen observer: %w", err)
		}
		b.ports = monitor
	}
	return b.ports, nil
}

// portExecution fences the socket namespace against CRI identity. A restored
// listener may be usable while CRI still reports the container as CREATED.
func (b *Backend) portExecution(ctx context.Context, w *api.ResumablePod, p *core.Pod) (*api.Execution, *os.File, error) {
	if w == nil || p == nil || w.Status.PodUID == "" || w.Status.PodUID != string(p.UID) || p.DeletionTimestamp != nil {
		return nil, nil, ErrExecutionLost
	}
	sandboxes, err := b.sandboxes(ctx, p)
	if err != nil {
		return nil, nil, err
	}
	if len(sandboxes) == 0 {
		if w.Status.Phase == "Running" {
			return nil, nil, ErrExecutionLost
		}
		return nil, nil, nil
	}
	if len(sandboxes) != 1 {
		return nil, nil, ErrExecutionLost
	}
	sid := sandboxes[0].Id
	if p.Labels["cellbox.jingyugao.github.io/warm-pool"] == "true" {
		lease, err := b.WarmStatus(ctx, p)
		if err != nil {
			return nil, nil, err
		}
		if lease.SID != sid || lease.Ticket.OwnerUID != string(w.UID) || lease.Ticket.PodUID != string(p.UID) ||
			lease.Ticket.SpecHash != w.Status.SpecHash || (w.Status.Snapshot != "" && lease.Ticket.Snapshot != w.Status.Snapshot) ||
			(lease.Phase != "claimed" && lease.Phase != "restoring") {
			return nil, nil, ErrExecutionLost
		}
	}
	execution, err := b.inspectExecution(ctx, w, p, sid, false)
	if err != nil || execution == nil {
		return nil, nil, err
	}
	if w.Status.Execution != nil && *w.Status.Execution != *execution {
		return nil, nil, ErrExecutionLost
	}
	status, err := b.Runtime.PodSandboxStatus(ctx, &cri.PodSandboxStatusRequest{PodSandboxId: sid, Verbose: true})
	if err != nil {
		return nil, nil, err
	}
	if status.Status == nil || status.Status.Id != sid || status.Status.Metadata == nil || status.Status.Metadata.Uid != string(p.UID) {
		return nil, nil, ErrExecutionLost
	}
	var info struct {
		PID int `json:"pid"`
	}
	if err := json.Unmarshal([]byte(status.Info["info"]), &info); err != nil || info.PID <= 0 {
		return nil, nil, fmt.Errorf("CRI did not provide sandbox namespace PID")
	}
	// Prevent a recycled PID from binding this operation to an unrelated netns.
	group, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", info.PID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, ErrExecutionLost
		}
		return nil, nil, err
	}
	if !strings.Contains(string(group), sid) {
		return nil, nil, ErrExecutionLost
	}
	ns, err := os.Open(fmt.Sprintf("/proc/%d/ns/net", info.PID))
	if err != nil {
		return nil, nil, err
	}
	group, err = os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", info.PID))
	if err != nil || !strings.Contains(string(group), sid) {
		ns.Close()
		return nil, nil, ErrExecutionLost
	}
	return execution, ns, nil
}

// WaitPortsListening waits for the current execution's actual TCP listeners.
// Its success does not depend on kubelet probes or a Guest admission flag.
func (b *Backend) WaitPortsListening(ctx context.Context, w *api.ResumablePod, p *core.Pod) (*api.Execution, error) {
	monitor, err := b.portMonitor()
	if err != nil {
		return nil, err
	}
	execution, ns, err := b.portExecution(ctx, w, p)
	if err != nil || execution == nil {
		return nil, err
	}
	defer ns.Close()
	if err = monitor.WaitPortsListening(ctx, fmt.Sprintf("/proc/self/fd/%d", ns.Fd()), requestedPorts(w)); err != nil {
		return nil, err
	}
	// A namespace can outlive its execution. Reconfirm identity after the wait.
	actual, currentNS, err := b.portExecution(ctx, w, p)
	if currentNS != nil {
		defer currentNS.Close()
	}
	if err != nil {
		return nil, err
	}
	if actual == nil || *actual != *execution {
		return nil, ErrExecutionLost
	}
	before, err := ns.Stat()
	if err != nil {
		return nil, err
	}
	after, err := currentNS.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, after) {
		return nil, ErrExecutionLost
	}
	return execution, nil
}

// CheckPortsListening is a single snapshot for reconciliation and independent
// health checks. It uses the same socket predicate as WaitPortsListening.
func (b *Backend) CheckPortsListening(ctx context.Context, w *api.ResumablePod, p *core.Pod) (*api.Execution, error) {
	execution, ns, err := b.portExecution(ctx, w, p)
	if err != nil || execution == nil {
		return nil, err
	}
	defer ns.Close()
	listening, err := portwait.CheckPortsListening(ctx, fmt.Sprintf("/proc/self/fd/%d", ns.Fd()), requestedPorts(w))
	if err != nil || !listening {
		return nil, err
	}
	return execution, nil
}

// CheckHealth observes an existing execution without changing it.
func (b *Backend) CheckHealth(ctx context.Context, w *api.ResumablePod, p *core.Pod) error {
	execution, err := b.CheckPortsListening(ctx, w, p)
	if err != nil {
		return err
	}
	if execution == nil {
		return ErrPortsNotListening
	}
	return nil
}
