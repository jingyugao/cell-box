// Package cellbox provides a Go client for creating Cellbox sandboxes and
// executing bounded commands. Context cancellation stops waiting; it does not
// cancel accepted server operations. Use operation IDs or the same idempotency
// key and identical input to recover their results without repeating work.
package cellbox

import (
	"fmt"
	"time"
)

// CreateOptions selects a configured profile and an application-owned label.
type CreateOptions struct {
	ProfileID       string `json:"profileId"`
	OwnerKey        string `json:"ownerKey"`
	ImportedImageID string `json:"importedImageId,omitempty"`
	// Staged holds the configured workload until explicitly activated via the API.
	Staged         bool   `json:"staged,omitempty"`
	IdempotencyKey string `json:"-"`
}

// SandboxInfo is a lifecycle snapshot. Running does not imply application health.
type SandboxInfo struct {
	ID              string       `json:"id"`
	OwnerKey        string       `json:"ownerKey"`
	ProfileID       string       `json:"profileId"`
	Phase           string       `json:"phase"`
	Generation      uint64       `json:"generation"`
	ExecutionID     string       `json:"executionId,omitempty"`
	ResourceVersion uint64       `json:"resourceVersion"`
	Image           string       `json:"image"`
	ImageID         string       `json:"imageId,omitempty"`
	ImportedImageID string       `json:"importedImageId,omitempty"`
	Workspace       string       `json:"workspace"`
	Capabilities    Capabilities `json:"capabilities"`
	OperationID     string       `json:"operationId,omitempty"`
	Error           *APIError    `json:"error,omitempty"`
	CreatedAt       time.Time    `json:"createdAt"`
	ObservedAt      *time.Time   `json:"observedAt,omitempty"`
}

type Capabilities struct {
	Exec          bool   `json:"exec"`
	Files         bool   `json:"files"`
	HTTP          bool   `json:"http"`
	WebSocket     bool   `json:"websocket"`
	PTY           bool   `json:"pty"`
	ReconnectExec bool   `json:"reconnectExec"`
	Freeze        bool   `json:"freeze"`
	Suspend       string `json:"suspend"`
	Archives      string `json:"archives"`
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

// Operation records durable server work. IdempotencyKey is populated locally
// on submission, and is not returned by the server's GET operation endpoint.
type Operation struct {
	ID             string            `json:"id"`
	Kind           string            `json:"kind"`
	TargetID       string            `json:"targetId"`
	Status         string            `json:"status"`
	Version        uint64            `json:"version"`
	CreatedAt      time.Time         `json:"createdAt"`
	FinishedAt     *time.Time        `json:"finishedAt,omitempty"`
	Result         map[string]string `json:"result,omitempty"`
	Box            *SandboxInfo      `json:"box,omitempty"`
	Error          *APIError         `json:"error,omitempty"`
	IdempotencyKey string            `json:"-"`
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

// RequestError preserves the submission key, including when a connection fails
// before the operation ID is received. Retry only with this key and identical input.
type RequestError struct {
	IdempotencyKey string
	Err            error
}

func (e *RequestError) Error() string { return e.Err.Error() }
func (e *RequestError) Unwrap() error { return e.Err }

// OperationError retains the last known operation for recovery or inspection.
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
