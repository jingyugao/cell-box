package runtimecontrol

import (
	"net"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

// GuestEndpoint is the address of a locally committed execution. It is a
// routing snapshot, not a health check or an authentication credential.
type GuestEndpoint struct {
	OwnerUID   string
	BoxID      string
	NodeName   string
	PodUID     string
	IP         string
	Generation int64
}

func (c *Client) GuestEndpoint(key types.NamespacedName) (GuestEndpoint, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	endpoint, ok := c.endpoints[key]
	return endpoint, ok
}

// Called under mu, before notifying subscribers that an execution is ready.
func (c *Client) projectEndpoint(key types.NamespacedName, w *api.ResumablePod, deleted bool) {
	if w == nil || deleted || w.DeletionTimestamp != nil || w.Spec.DesiredState != "Running" || w.Status.Phase != "Running" ||
		w.Status.ObservedGeneration != w.Generation || w.Status.ObservedTransitionID != w.Spec.TransitionID ||
		w.Status.Execution == nil || w.Status.Execution.PodUID != w.Status.PodUID || w.Status.PodUID == "" ||
		w.Status.Execution.ContainerID == "" || net.ParseIP(w.Status.Execution.PodIP) == nil {
		if current, ok := c.endpoints[key]; ok && (!deleted || w == nil || current.OwnerUID == string(w.UID)) {
			delete(c.endpoints, key)
		}
		return
	}
	if c.endpoints == nil {
		c.endpoints = make(map[types.NamespacedName]GuestEndpoint)
	}
	c.endpoints[key] = GuestEndpoint{
		OwnerUID: string(w.UID), BoxID: w.Labels["cellbox.jingyugao.github.io/box-id"], NodeName: w.Spec.NodeName,
		PodUID: w.Status.PodUID, IP: w.Status.Execution.PodIP, Generation: w.Status.Cycle,
	}
}
