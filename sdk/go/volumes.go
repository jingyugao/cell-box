package cellbox

import (
	"context"
	"fmt"
	"net/http"
)

// CreateVolumeAsync submits creation of an independently managed persistent volume.
// A supplied ID is reserved for this volume and is never reused after deletion.
func (c *Client) CreateVolumeAsync(ctx context.Context, opts CreateVolumeOptions) (*Operation, error) {
	if opts.OwnerKey == "" || len(opts.OwnerKey) > 256 || opts.ProfileID == "" {
		return nil, fmt.Errorf("cellbox: owner key (1..256 bytes) and profile ID are required")
	}
	return c.submit(ctx, "/v1/volumes", opts)
}

// CreateVolume waits for provisioning and returns the resulting volume.
func (c *Client) CreateVolume(ctx context.Context, opts CreateVolumeOptions) (*Volume, error) {
	op, err := c.CreateVolumeAsync(ctx, opts)
	if err != nil {
		return nil, err
	}
	op, err = c.wait(ctx, op)
	if err != nil {
		return nil, err
	}
	id := op.TargetID
	if id == "" && op.Result != nil {
		id = op.Result["volumeId"]
	}
	if id == "" {
		return nil, &OperationError{Operation: op, Err: fmt.Errorf("volume create result missing volume ID")}
	}
	volume, err := c.GetVolume(ctx, id)
	if err != nil {
		return nil, &OperationError{Operation: op, Err: err}
	}
	return volume, nil
}

// GetVolume reads a volume in the client's namespace.
func (c *Client) GetVolume(ctx context.Context, id string) (*Volume, error) {
	path, err := resourcePath("/v1/volumes/", id)
	if err != nil {
		return nil, err
	}
	var volume Volume
	if err := c.do(ctx, http.MethodGet, path, nil, &volume); err != nil {
		return nil, err
	}
	return &volume, nil
}

// ListVolumes lists volumes in the client's namespace, including deleting and deleted records.
func (c *Client) ListVolumes(ctx context.Context) ([]Volume, error) {
	var volumes []Volume
	err := c.do(ctx, http.MethodGet, "/v1/volumes", nil, &volumes)
	return volumes, err
}

// DeleteVolumeAsync requests explicit deletion of a volume and its data.
func (c *Client) DeleteVolumeAsync(ctx context.Context, id string) (*Operation, error) {
	path, err := resourcePath("/v1/volumes/", id)
	if err != nil {
		return nil, err
	}
	var op Operation
	if err := c.do(ctx, http.MethodDelete, path, nil, &op); err != nil {
		return nil, &RequestError{Err: err}
	}
	if op.ID == "" {
		return nil, &RequestError{Err: fmt.Errorf("cellbox: response missing operation ID")}
	}
	return &op, nil
}

// DeleteVolume waits until volume deletion completes and returns its operation.
func (c *Client) DeleteVolume(ctx context.Context, id string) (*Operation, error) {
	op, err := c.DeleteVolumeAsync(ctx, id)
	if err != nil {
		return nil, err
	}
	return c.wait(ctx, op)
}
