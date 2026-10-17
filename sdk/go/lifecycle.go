package cellbox

import (
	"context"
	"fmt"
)

// LifecycleOptions controls submission of a lifecycle transition. Reuse the
// same key and original handle after an ambiguous submission to recover work.
type LifecycleOptions struct {
	IdempotencyKey string
}

func (s *Sandbox) action(ctx context.Context, action string, opts *LifecycleOptions) (*Operation, error) {
	if s.Generation == 0 && action != "destroy" {
		return nil, fmt.Errorf("cellbox: sandbox has no runtime generation; connect after provisioning completes")
	}
	path, err := resourcePath("/v1/boxes/", s.ID)
	if err != nil {
		return nil, err
	}
	key := ""
	if opts != nil {
		key = opts.IdempotencyKey
	}
	body := struct {
		ExpectedGeneration uint64 `json:"expectedGeneration,omitempty"`
	}{s.Generation}
	op, err := s.client.submit(ctx, path+":"+action, key, body)
	if err != nil {
		return nil, err
	}
	return s.client.wait(ctx, op)
}

func (s *Sandbox) runningAction(ctx context.Context, action string, opts *LifecycleOptions) (*Sandbox, error) {
	op, err := s.action(ctx, action, opts)
	if err != nil {
		return nil, err
	}
	if op.Box != nil {
		if op.Box.ID != s.ID || op.Box.Generation == 0 {
			return nil, &OperationError{Operation: op, Err: fmt.Errorf("invalid sandbox snapshot in lifecycle result")}
		}
		return s.client.sandbox(*op.Box), nil
	}
	current, err := s.client.Connect(ctx, s.ID)
	if err != nil {
		return nil, &OperationError{Operation: op, Err: err}
	}
	return current, nil
}

// Suspend checkpoints a running Kubernetes sandbox on its current node. It
// waits for completion; cancelling ctx only stops the local wait.
func (s *Sandbox) Suspend(ctx context.Context, opts *LifecycleOptions) error {
	_, err := s.action(ctx, "suspend", opts)
	return err
}

// Resume restores a suspended Kubernetes sandbox and returns a fresh handle.
// Use the returned handle for subsequent commands and lifecycle transitions.
// The original handle is not modified. Runtime readiness is not app health.
func (s *Sandbox) Resume(ctx context.Context, opts *LifecycleOptions) (*Sandbox, error) {
	return s.runningAction(ctx, "resume", opts)
}

// EnsureRunning returns a fresh handle for a running sandbox, resuming it when
// suspended. It does not recover failed boxes or unfreeze Docker containers.
func (s *Sandbox) EnsureRunning(ctx context.Context, opts *LifecycleOptions) (*Sandbox, error) {
	return s.runningAction(ctx, "ensure-running", opts)
}

// Activate starts the configured workload in a staged sandbox and returns a
// fresh handle. Creating with Staged=true allows preparation before activation.
func (s *Sandbox) Activate(ctx context.Context, opts *LifecycleOptions) (*Sandbox, error) {
	return s.runningAction(ctx, "activate", opts)
}

// Freeze pauses a Docker sandbox without a checkpoint. Its generation is unchanged.
func (s *Sandbox) Freeze(ctx context.Context, opts *LifecycleOptions) error {
	_, err := s.action(ctx, "freeze", opts)
	return err
}

// Unfreeze resumes execution in a frozen Docker sandbox. Its generation is unchanged.
func (s *Sandbox) Unfreeze(ctx context.Context, opts *LifecycleOptions) error {
	_, err := s.action(ctx, "unfreeze", opts)
	return err
}
