// Package boxprovider is the private boundary between Cellbox and runtimes.
package boxprovider

import (
	"cellbox.local/cellbox/internal/guestapi"
	"context"
	"errors"
)

var ErrNotFound = errors.New("runtime not found")
var ErrUnsupported = errors.New("unsupported runtime operation")

type Spec struct {
	BoxID                  string
	Image                  string
	Config                 guestapi.Config
	CPU                    float64
	MemoryMiB              int64
	NodeName               string
	Namespace              string
	DebugReadOnlyHostPath  string
	DebugReadWriteHostPath string
	Staged                 bool // Start guest control, but hold the configured workload until activation.
}

type Handle struct {
	Provider  string `json:"provider"`
	ID        string `json:"id"`
	Name      string `json:"name"`
	ImageID   string `json:"imageId,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	NodeName  string `json:"nodeName,omitempty"`
}

type Observation struct {
	State       string // provisioning, ready, frozen, suspended, resuming, failed, deleted
	ExecutionID string // immutable runtime identity; changes after Pod replacement
	Message     string
}

type Connection struct {
	URL   string
	Token string
}

type Provider interface {
	Name() string
	Create(context.Context, Spec) (Handle, error)
	Inspect(context.Context, Handle) (Observation, error)
	Action(context.Context, Handle, string) error // freeze, unfreeze, suspend, resume
	Destroy(context.Context, Handle) error
	Guest(context.Context, Handle) (Connection, error)
}
