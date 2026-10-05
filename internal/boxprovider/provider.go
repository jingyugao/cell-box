// Package boxprovider is the private boundary between Cellbox and runtimes.
package boxprovider

import (
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/inventory"
	"context"
	"encoding/json"
	"errors"
)

var ErrNotFound = errors.New("runtime not found")
var ErrUnsupported = errors.New("unsupported runtime operation")
var ErrStaleExecution = errors.New("runtime execution changed")
var ErrNotReady = errors.New("current runtime is not ready")

type Spec struct {
	Inventory              json.RawMessage
	BoxID                  string
	Image                  string
	Config                 guestapi.Config
	CPU                    float64
	MemoryMiB              int64
	NodeName               string
	Namespace              string
	SharedReadOnlyHostPath string
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
	Generation  uint64 // Provider-owned durable execution cycle, when available.
	Phase       string // creating, running, freezing, frozen, unfreezing, checkpointing, suspending, suspended, resuming, restoring, staged, deleting, deleted, failed
	ExecutionID string // immutable runtime identity; changes after Pod replacement
	Message     string
}

type Connection struct {
	URL string
}

type Provider interface {
	Name() string
	Create(context.Context, Spec) (Handle, error)
	Inspect(context.Context, Handle) (Observation, error)
	Action(context.Context, Handle, string) error // freeze, unfreeze, suspend, resume
	Destroy(context.Context, Handle) error
	Guest(context.Context, Handle) (Connection, error)
}

// ImageCacheProvider warms the runtime image store without creating a workload.
type ImageCacheProvider interface {
	CacheImage(context.Context, string, string, string) error // image, node, namespace
}

// FencedGuestProvider validates the execution while resolving its
// connection, avoiding a second, independent lifecycle inspection.
type FencedGuestProvider interface {
	GuestForExecution(context.Context, Handle, string) (Connection, error)
}

// ChangeProvider wakes lifecycle waiters when the runtime resource changes.
type ChangeProvider interface {
	WatchChanges(context.Context, Handle) (<-chan struct{}, error)
}

// InventoryProvider enumerates workloads from the runtime, without the service ledger.
type InventoryProvider interface {
	List(context.Context, string, string) ([]inventory.Record, error)
}

// ResourceInventoryProvider reads lifecycle state from resources without checking Pods.
// It is intended for display-only listings that accept eventual consistency.
type ResourceInventoryProvider interface {
	ListResources(context.Context, string, string) ([]inventory.Record, error)
}
