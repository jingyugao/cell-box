// Package cellbox provides a Go client for managing Cellbox templates, sandbox
// lifecycles, workspace files and bounded commands. Context cancellation stops
// waiting; it does not cancel accepted server operations. Use operation IDs to
// continue waiting for accepted work. Command and template build submissions
// are sent once without automatic retry because their scripts may have side
// effects that cannot be safely deduplicated.
package cellbox

import (
	"fmt"
	"time"
)

// CreateOptions selects a fixed revision, or the current stable revision of a template.
type CreateOptions struct {
	ID           string  `json:"id,omitempty"`
	RevisionID   string  `json:"revisionId,omitempty"`
	TemplateID   string  `json:"templateId,omitempty"`
	OwnerKey     string  `json:"ownerKey"`
	HomeVolumeID string  `json:"homeVolumeId,omitempty"`
	Mounts       []Mount `json:"mounts,omitempty"`
	// Staged holds the configured workload until explicitly activated via the API.
	Staged bool `json:"staged,omitempty"`
}

// SandboxInfo is a lifecycle snapshot. Running does not imply application health.
type SandboxInfo struct {
	ID              string       `json:"id"`
	OwnerKey        string       `json:"ownerKey"`
	HomeVolumeID    string       `json:"homeVolumeId,omitempty"`
	Mounts          []Mount      `json:"mounts,omitempty"`
	TemplateID      string       `json:"templateId"`
	RevisionID      string       `json:"revisionId"`
	TemplateName    string       `json:"templateName"`
	TemplateVersion string       `json:"templateVersion"`
	ProfileID       string       `json:"profileId"`
	Phase           string       `json:"phase"`
	Generation      uint64       `json:"generation"`
	ExecutionID     string       `json:"executionId,omitempty"`
	ResourceVersion uint64       `json:"resourceVersion"`
	Image           string       `json:"image"`
	ImageID         string       `json:"imageId,omitempty"`
	Workspace       string       `json:"workspace"`
	Capabilities    Capabilities `json:"capabilities"`
	OperationID     string       `json:"operationId,omitempty"`
	Error           *APIError    `json:"error,omitempty"`
	CreatedAt       time.Time    `json:"createdAt"`
	ObservedAt      *time.Time   `json:"observedAt,omitempty"`
}

// Mount attaches a persistent volume at a fixed absolute path in a sandbox.
type Mount struct {
	VolumeID  string `json:"volumeId"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

// CreateVolumeOptions creates a persistent volume for later mounting.
type CreateVolumeOptions struct {
	ID        string `json:"id,omitempty"`
	OwnerKey  string `json:"ownerKey"`
	ProfileID string `json:"profileId"`
}

// Volume describes an independently managed persistent volume.
type Volume struct {
	ID              string    `json:"id"`
	OwnerKey        string    `json:"ownerKey"`
	ProfileID       string    `json:"profileId"`
	NodeName        string    `json:"nodeName"`
	Phase           string    `json:"phase"`
	ResourceVersion uint64    `json:"resourceVersion"`
	CreatedAt       time.Time `json:"createdAt"`
	OperationID     string    `json:"operationId,omitempty"`
	Error           *APIError `json:"error,omitempty"`
}

type Capabilities struct {
	Exec               bool   `json:"exec"`
	Files              bool   `json:"files"`
	HTTP               bool   `json:"http"`
	WebSocket          bool   `json:"websocket"`
	PTY                bool   `json:"pty"`
	ReconnectExec      bool   `json:"reconnectExec"`
	Suspend            string `json:"suspend"`
	Archives           string `json:"archives"`
	ProtectedTools     bool   `json:"protectedTools"`
	CredentialBatch    bool   `json:"credentialBatch,omitempty"`
	InternalServices   bool   `json:"internalServices,omitempty"`
	RootDebug          bool   `json:"rootDebug,omitempty"`
	SharedDirectory    bool   `json:"sharedDirectory,omitempty"`
	MountedToolRuntime bool   `json:"mountedToolRuntime,omitempty"`
	MountedDebugHome   bool   `json:"mountedDebugHome,omitempty"`
	PersistentHome     bool   `json:"persistentHome,omitempty"`
}

type Profile struct {
	ID           string       `json:"id"`
	Provider     string       `json:"provider"`
	Kind         string       `json:"kind"`
	Image        string       `json:"image"`
	Workspace    string       `json:"workspace"`
	CPU          float64      `json:"cpu"`
	MemoryMiB    int64        `json:"memoryMiB"`
	Capabilities Capabilities `json:"capabilities"`
}

// Template is a family of immutable revisions.
type Template struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	Description      string    `json:"description,omitempty"`
	DefaultWarmSlots int       `json:"defaultWarmSlots"`
	StableRevisionID string    `json:"stableRevisionId,omitempty"`
	Generation       uint64    `json:"generation"`
	CreatedAt        time.Time `json:"createdAt"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

// CreateTemplateOptions defines a template family.
type CreateTemplateOptions struct {
	Name             string `json:"name"`
	Description      string `json:"description,omitempty"`
	DefaultWarmSlots int    `json:"defaultWarmSlots,omitempty"`
}

type UpdateTemplateOptions struct {
	Description        *string `json:"description,omitempty"`
	DefaultWarmSlots   *int    `json:"defaultWarmSlots,omitempty"`
	StableRevisionID   *string `json:"stableRevisionId,omitempty"`
	ExpectedGeneration uint64  `json:"expectedGeneration"`
}

type CreateRevisionOptions struct {
	UserVersion  string `json:"userVersion"`
	BaseImage    string `json:"baseImage"`
	ProfileID    string `json:"profileId"`
	BuildCommand string `json:"buildCommand,omitempty"`
	RunCommand   string `json:"runCommand,omitempty"`
	Platform     string `json:"platform,omitempty"`
	WarmSlots    *int   `json:"warmSlots,omitempty"`
}

type TemplateRevision struct {
	ID                string    `json:"id"`
	TemplateID        string    `json:"templateId"`
	TemplateName      string    `json:"templateName"`
	Description       string    `json:"description,omitempty"`
	UserVersion       string    `json:"userVersion"`
	Revision          int       `json:"revision"`
	Version           string    `json:"version"`
	ProfileID         string    `json:"profileId"`
	BaseImage         string    `json:"baseImage"`
	ResolvedBaseImage string    `json:"resolvedBaseImage"`
	Image             string    `json:"image"`
	BuildCommand      string    `json:"buildCommand,omitempty"`
	RunCommand        string    `json:"runCommand,omitempty"`
	Platform          string    `json:"platform,omitempty"`
	CPU               float64   `json:"cpu"`
	MemoryMiB         int64     `json:"memoryMiB"`
	WarmSlots         int       `json:"warmSlots"`
	Status            string    `json:"status"`
	Generation        uint64    `json:"generation"`
	BuildOperationID  string    `json:"buildOperationId,omitempty"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

// WarmPoolResources reports configured requests and limits for allocated idle
// warm Pods. These values are not measured CPU or memory usage.
type WarmPoolResources struct {
	CPURequestMilli    int64 `json:"cpuRequestMilli"`
	CPULimitMilli      int64 `json:"cpuLimitMilli"`
	MemoryRequestBytes int64 `json:"memoryRequestBytes"`
	MemoryLimitBytes   int64 `json:"memoryLimitBytes"`
}

// WarmPoolStatus is an observed snapshot. Nil Pool means no pool is currently
// associated with the revision; stale snapshots must not be treated as current.
type WarmPoolStatus struct {
	PoolID         string            `json:"poolId"`
	RequestedSlots int               `json:"requestedSlots"`
	AllocatedSlots int               `json:"allocatedSlots"`
	ReadySlots     int               `json:"readySlots"`
	PreparingSlots int               `json:"preparingSlots"`
	FailedSlots    int               `json:"failedSlots"`
	Resources      WarmPoolResources `json:"resources"`
	ObservedAt     time.Time         `json:"observedAt"`
	Stale          bool              `json:"stale"`
}

type RevisionWarmStatus struct {
	RevisionID         string          `json:"revisionId"`
	Generation         uint64          `json:"generation"`
	ObservedGeneration uint64          `json:"observedGeneration"`
	DesiredSlots       int             `json:"desiredSlots"`
	SyncStatus         string          `json:"syncStatus"`
	Error              string          `json:"error,omitempty"`
	ObservedAt         *time.Time      `json:"observedAt,omitempty"`
	Stale              bool            `json:"stale"`
	Pool               *WarmPoolStatus `json:"pool"`
}

type RevisionRuntimeStatus struct {
	RevisionID   string             `json:"revisionId"`
	SandboxCount *int               `json:"sandboxCount"`
	Phases       map[string]int     `json:"phases"`
	ObservedAt   *time.Time         `json:"observedAt,omitempty"`
	Stale        bool               `json:"stale"`
	Error        string             `json:"error,omitempty"`
	WarmPool     RevisionWarmStatus `json:"warmPool"`
}

type TemplateRuntimeStatus struct {
	TemplateID string                  `json:"templateId"`
	Revisions  []RevisionRuntimeStatus `json:"revisions"`
}

type UpdateRevisionOptions struct {
	Description        *string `json:"description,omitempty"`
	Status             *string `json:"status,omitempty"`
	WarmSlots          *int    `json:"warmSlots,omitempty"`
	ExpectedGeneration uint64  `json:"expectedGeneration"`
}

type RevisionListOptions struct {
	UserVersion string
	Limit       int
	Offset      int
}

type TemplateListOptions struct {
	Name   string
	Limit  int
	Offset int
}

type OperationListOptions struct {
	TemplateID string
	Status     string
	Limit      int
	Offset     int
}

type OperationLogs struct {
	Text       string `json:"text"`
	NextOffset int64  `json:"nextOffset"`
	EOF        bool   `json:"eof"`
}

// Operation records server work. After a server restart, an interrupted
// operation may have an unknown outcome; Cellbox does not replay it.
type Operation struct {
	ID         string                 `json:"id"`
	Kind       string                 `json:"kind"`
	TargetID   string                 `json:"targetId"`
	Status     string                 `json:"status"`
	Stage      string                 `json:"stage,omitempty"`
	Version    uint64                 `json:"version"`
	CreatedAt  time.Time              `json:"createdAt"`
	StartedAt  *time.Time             `json:"startedAt,omitempty"`
	FinishedAt *time.Time             `json:"finishedAt,omitempty"`
	UpdatedAt  time.Time              `json:"updatedAt"`
	Input      *CreateRevisionOptions `json:"input,omitempty"`
	Result     map[string]string      `json:"result,omitempty"`
	Box        *SandboxInfo           `json:"box,omitempty"`
	Error      *APIError              `json:"error,omitempty"`
}

type Execution struct {
	ID          string         `json:"id"`
	BoxID       string         `json:"boxId"`
	OperationID string         `json:"operationId"`
	State       string         `json:"state"`
	Result      *CommandResult `json:"result,omitempty"`
}

type CommandResult struct {
	Stdout    string `json:"stdout"`
	Stderr    string `json:"stderr"`
	ExitCode  int    `json:"exitCode"`
	Truncated bool   `json:"truncated,omitempty"`
}

// APIError represents an HTTP API error or an operation's reported failure.
// StatusCode is zero for errors embedded in an operation.
type APIError struct {
	StatusCode int    `json:"-"`
	Code       string `json:"code"`
	Message    string `json:"message"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("cellbox: %s: %s", e.Code, e.Message)
}

// RequestError reports a submission failure. The server may have accepted the
// request if the response was lost; callers should inspect state before retrying.
type RequestError struct {
	Err error
}

func (e *RequestError) Error() string { return e.Err.Error() }
func (e *RequestError) Unwrap() error { return e.Err }

// OperationError retains the last known operation for inspection.
// errors.Is/As also work with its underlying context error or APIError.
type OperationError struct {
	Operation *Operation
	Err       error
}

func (e *OperationError) Error() string {
	return fmt.Sprintf("cellbox: operation %s: %v", e.Operation.ID, e.Err)
}
func (e *OperationError) Unwrap() error { return e.Err }

// CommandExitError reports a completed command's nonzero exit status, including
// server command timeouts (normally exit code 124). Its output remains available.
type CommandExitError struct {
	Result *CommandResult
}

func (e *CommandExitError) Error() string {
	return fmt.Sprintf("cellbox: command exited with code %d", e.Result.ExitCode)
}
