// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/guestapi"
	"github.com/jingyugao/cell-box/internal/telemetry"
	core "k8s.io/api/core/v1"
)

const resumeGuestTimeout = 3 * time.Second

// ResumeGuest unquiesces the guest belonging to the current local CRI
// execution. It does not look up Kubernetes state: callers pass their current
// Pod observation and the node independently fences the request against CRI.
func (b *Backend) ResumeGuest(ctx context.Context, w *api.ResumablePod, p *core.Pod) (resultErr error) {
	ctx, span := telemetry.Start(ctx, "node.resume_guest")
	defer func() { telemetry.End(span, resultErr) }()

	actual, err := b.ConfirmExecution(ctx, w, p)
	if err != nil {
		return err
	}

	// The guest endpoint is addressed through the exact Pod IP just confirmed
	// by CRI. Disable proxy discovery and redirects so credentials or commands
	// can never be routed to another endpoint.
	endpoint, err := resumeGuestURL(actual.PodIP)
	if err != nil {
		return err
	}
	requestCtx, cancel := context.WithTimeout(ctx, resumeGuestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, nil)
	if err != nil {
		return fmt.Errorf("create guest resume request")
	}
	instanceID := w.Annotations[api.GuestIdentityAnnotation]
	if instanceID == "" { // Boxes created before the identity left the command line.
		var err error
		if instanceID, err = guestapi.InstanceIDFromArgs(w.Spec.Container.Args); err != nil {
			return err
		}
	}
	if instanceID != "" {
		req.Header.Set(guestapi.InstanceIDHeader, instanceID)
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: resumeGuestTimeout, KeepAlive: 30 * time.Second}).DialContext}
	client := &http.Client{
		Transport: transport,
		Timeout:   resumeGuestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	defer transport.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("guest resume request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("guest resume request returned HTTP %d", resp.StatusCode)
	}
	w.Status.Execution = actual
	return nil
}

// ConfirmExecution verifies the supplied Pod execution against local CRI state
// without making a request to the guest.
func (b *Backend) ConfirmExecution(ctx context.Context, w *api.ResumablePod, p *core.Pod) (execution *api.Execution, resultErr error) {
	ctx, span := telemetry.Start(ctx, "node.execution.confirm")
	defer func() { telemetry.End(span, resultErr) }()
	if w == nil || p == nil || b == nil || b.Runtime == nil {
		return nil, fmt.Errorf("execution confirmation requires workload, Pod, and CRI runtime")
	}
	expected, err := resumeGuestExpectedExecution(w, p)
	if err != nil {
		return nil, err
	}

	// Reconfirm the supplied execution using only the node's CRI. A persisted
	// warm execution is useful as a fence, but never authorizes a replaced
	// container by itself.
	criCtx, cancelCRI := context.WithTimeout(ctx, 2*time.Second)
	defer cancelCRI()
	sandboxes, err := b.sandboxes(criCtx, p)
	if err != nil {
		return nil, fmt.Errorf("inspect local guest execution")
	}
	if len(sandboxes) != 1 || sandboxes[0] == nil || sandboxes[0].Id == "" {
		return nil, fmt.Errorf("local guest execution is missing or ambiguous")
	}
	actual, err := b.inspectExecution(criCtx, w, p, sandboxes[0].Id, false)
	if err != nil {
		return nil, fmt.Errorf("inspect local guest execution")
	}
	if actual == nil || actual.PodUID != expected.PodUID || actual.ContainerID != expected.ContainerID || actual.PodIP != expected.PodIP {
		return nil, fmt.Errorf("local guest execution identity changed")
	}
	return actual, nil
}

func resumeGuestExpectedExecution(w *api.ResumablePod, p *core.Pod) (*api.Execution, error) {
	if w.Status.PodUID == "" || w.Status.PodUID != string(p.UID) {
		return nil, fmt.Errorf("workload Pod identity changed")
	}
	if w.Status.Execution != nil {
		if !api.ConfirmedExecution(w, p) {
			return nil, fmt.Errorf("confirmed guest execution identity changed")
		}
		return w.Status.Execution, nil
	}
	if !api.ExecutionStarted(p, w.Spec.Container.Name) {
		return nil, fmt.Errorf("guest container is not running")
	}
	for i := range p.Status.ContainerStatuses {
		status := &p.Status.ContainerStatuses[i]
		if status.Name == w.Spec.Container.Name {
			return &api.Execution{
				PodUID:      string(p.UID),
				ContainerID: strings.TrimPrefix(status.ContainerID, "containerd://"),
				PodIP:       p.Status.PodIP,
			}, nil
		}
	}
	return nil, fmt.Errorf("guest container status is missing")
}

// Keep URL construction local and strict if this endpoint is ever moved to a
// separately configured guest port.
func resumeGuestURL(ip string) (string, error) {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return "", fmt.Errorf("guest IP is invalid")
	}
	return "http://" + net.JoinHostPort(parsed.String(), strconv.Itoa(guestapi.Port)) + "/v1/unquiesce", nil
}
