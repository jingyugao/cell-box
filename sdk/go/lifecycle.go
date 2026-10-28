package cellbox

import (
	"context"
	"fmt"
)

func (s *Sandbox) action(ctx context.Context, action string, resumeOptions *ResumeOptions) (*Operation, error) {
	if s.Generation == 0 && action != "destroy" {
		return nil, fmt.Errorf("cellbox: sandbox has no runtime generation; connect after provisioning completes")
	}
	path, err := resourcePath("/v1/boxes/", s.ID)
	if err != nil {
		return nil, err
	}
	body := struct {
		ExpectedGeneration uint64 `json:"expectedGeneration,omitempty"`
		WaitPorts          *[]int `json:"waitPorts,omitempty"`
	}{ExpectedGeneration: s.Generation}
	if resumeOptions != nil {
		if resumeOptions.WaitPorts != nil && len(resumeOptions.WaitPorts) == 0 {
			return nil, fmt.Errorf("cellbox: waitPorts must not be empty")
		}
		if resumeOptions.WaitPorts != nil {
			ports := append([]int(nil), resumeOptions.WaitPorts...)
			body.WaitPorts = &ports
		}
	}
	op, err := s.client.submit(ctx, path+":"+action, body)
	if err != nil {
		return nil, err
	}
	return s.client.wait(ctx, op)
}

func (s *Sandbox) runningAction(ctx context.Context, action string, resumeOptions *ResumeOptions) (*Sandbox, error) {
	op, err := s.action(ctx, action, resumeOptions)
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
func (s *Sandbox) Suspend(ctx context.Context) error {
	_, err := s.action(ctx, "suspend", nil)
	return err
}

// Resume restores a suspended Kubernetes sandbox and returns a fresh handle.
// Use the returned handle for subsequent commands and lifecycle transitions.
// The original handle is not modified. Runtime readiness is not app health.
func (s *Sandbox) Resume(ctx context.Context, options ...ResumeOptions) (*Sandbox, error) {
	if len(options) > 1 {
		return nil, fmt.Errorf("cellbox: Resume accepts at most one options value")
	}
	var selected *ResumeOptions
	if len(options) == 1 {
		selected = &options[0]
	}
	return s.runningAction(ctx, "resume", selected)
}

// Activate starts the workload of a staged Kubernetes sandbox and returns a
// fresh handle.
func (s *Sandbox) Activate(ctx context.Context) (*Sandbox, error) {
	return s.runningAction(ctx, "activate", nil)
}
