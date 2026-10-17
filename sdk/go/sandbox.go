package cellbox

import (
	"context"
	"fmt"
	"net/http"
)

// Sandbox is a handle pinned to the generation observed at Create/Connect.
// Reconnect after another client changes the runtime generation. Fields should
// be treated as read-only; methods do not mutate the handle and may run concurrently.
type Sandbox struct {
	ID         string
	Generation uint64
	Commands   *Commands
	client     *Client
}

// Create waits for provisioning to complete. An application may still need its
// own health check. On cancellation, inspect OperationError to recover the sandbox.
func (c *Client) Create(ctx context.Context, opts CreateOptions) (*Sandbox, error) {
	op, err := c.CreateAsync(ctx, opts)
	if err != nil {
		return nil, err
	}
	op, err = c.wait(ctx, op)
	if err != nil {
		return nil, err
	}
	if op.Box != nil {
		return c.sandbox(*op.Box), nil
	}
	sandbox, err := c.Connect(ctx, op.TargetID)
	if err != nil {
		return nil, &OperationError{Operation: op, Err: err}
	}
	return sandbox, nil
}

// CreateAsync submits provisioning. Use WaitOperation then Connect to attach.
func (c *Client) CreateAsync(ctx context.Context, opts CreateOptions) (*Operation, error) {
	if (opts.TemplateID == "") == (opts.RevisionID == "") || opts.OwnerKey == "" || len(opts.OwnerKey) > 256 {
		return nil, fmt.Errorf("cellbox: exactly one of template ID or revision ID and an owner key (1..256 bytes) are required")
	}
	return c.submit(ctx, "/v1/boxes", opts.IdempotencyKey, opts)
}

// Connect attaches to an existing sandbox without resuming it or changing state.
func (c *Client) Connect(ctx context.Context, id string) (*Sandbox, error) {
	info, err := c.GetSandbox(ctx, id)
	if err != nil {
		return nil, err
	}
	return c.sandbox(*info), nil
}

func (c *Client) sandbox(info SandboxInfo) *Sandbox {
	s := &Sandbox{ID: info.ID, Generation: info.Generation, client: c}
	s.Commands = &Commands{sandbox: s}
	return s
}

func (c *Client) GetSandbox(ctx context.Context, id string) (*SandboxInfo, error) {
	path, err := resourcePath("/v1/boxes/", id)
	if err != nil {
		return nil, err
	}
	var info SandboxInfo
	if err := c.do(ctx, http.MethodGet, path, "", nil, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// Info retrieves current lifecycle state without changing the handle's generation.
func (s *Sandbox) Info(ctx context.Context) (*SandboxInfo, error) {
	return s.client.GetSandbox(ctx, s.ID)
}

type DestroyOptions struct {
	IdempotencyKey string
}

// Destroy waits for resource destruction. Active commands or leases may cause
// a BUSY error. Destroy does not implicitly cancel commands.
func (s *Sandbox) Destroy(ctx context.Context, opts *DestroyOptions) error {
	path, err := resourcePath("/v1/boxes/", s.ID)
	if err != nil {
		return err
	}
	key := ""
	if opts != nil {
		key = opts.IdempotencyKey
	}
	body := struct {
		ExpectedGeneration uint64 `json:"expectedGeneration,omitempty"`
	}{s.Generation}
	op, err := s.client.submit(ctx, path+":destroy", key, body)
	if err != nil {
		return err
	}
	_, err = s.client.wait(ctx, op)
	return err
}
