package cellbox

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
)

var volumeIDPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,53}[a-z0-9])?$`)
var volumeBackupIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// GetBackupPolicy returns the server's independent-volume backup policy.
func (c *Client) GetBackupPolicy(ctx context.Context) (*BackupPolicy, error) {
	var policy BackupPolicy
	if err := c.do(ctx, http.MethodGet, "/v1/backup-policy", nil, &policy); err != nil {
		return nil, err
	}
	return &policy, nil
}

// CreateVolumeBackupAsync submits one backup request. The mutation is sent
// once; callers can use the returned operation ID if waiting is interrupted.
func (c *Client) CreateVolumeBackupAsync(ctx context.Context, volumeID string) (*Operation, error) {
	if !volumeIDPattern.MatchString(volumeID) {
		return nil, fmt.Errorf("cellbox: invalid volume ID")
	}
	path, err := resourcePath("/v1/volumes/", volumeID)
	if err != nil {
		return nil, err
	}
	return c.submit(ctx, path+"/backups", struct{}{})
}

// CreateVolumeBackup waits for the submitted backup operation to finish.
func (c *Client) CreateVolumeBackup(ctx context.Context, volumeID string) (*Operation, error) {
	op, err := c.CreateVolumeBackupAsync(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	return c.wait(ctx, op)
}

// ListVolumeBackups lists the snapshots retained for a Volume.
func (c *Client) ListVolumeBackups(ctx context.Context, volumeID string) ([]VolumeBackup, error) {
	if !volumeIDPattern.MatchString(volumeID) {
		return nil, fmt.Errorf("cellbox: invalid volume ID")
	}
	path, err := resourcePath("/v1/volumes/", volumeID)
	if err != nil {
		return nil, err
	}
	var backups []VolumeBackup
	err = c.do(ctx, http.MethodGet, path+"/backups", nil, &backups)
	return backups, err
}

// GetVolumeBackup returns one retained snapshot.
func (c *Client) GetVolumeBackup(ctx context.Context, volumeID, backupID string) (*VolumeBackup, error) {
	if !volumeIDPattern.MatchString(volumeID) {
		return nil, fmt.Errorf("cellbox: invalid volume ID")
	}
	if !volumeBackupIDPattern.MatchString(backupID) {
		return nil, fmt.Errorf("cellbox: a full 64-character backup ID is required")
	}
	path, err := resourcePath("/v1/volumes/", volumeID)
	if err != nil {
		return nil, err
	}
	backupPath, err := resourcePath(path+"/backups/", backupID)
	if err != nil {
		return nil, err
	}
	var backup VolumeBackup
	if err := c.do(ctx, http.MethodGet, backupPath, nil, &backup); err != nil {
		return nil, err
	}
	return &backup, nil
}

// RestoreVolumeAsync submits a restore that always creates a new Volume.
func (c *Client) RestoreVolumeAsync(ctx context.Context, opts RestoreVolumeOptions) (*Operation, error) {
	if !volumeIDPattern.MatchString(opts.SourceVolumeID) || !volumeBackupIDPattern.MatchString(opts.BackupID) || opts.OwnerKey == "" || len(opts.OwnerKey) > 256 {
		return nil, fmt.Errorf("cellbox: valid source volume ID, full backup ID, and owner key (1..256 bytes) are required")
	}
	if opts.ID != "" && !volumeIDPattern.MatchString(opts.ID) {
		return nil, fmt.Errorf("cellbox: invalid target volume ID")
	}
	return c.submit(ctx, "/v1/volumes:restore", opts)
}

// RestoreVolume waits for the new Volume to be restored and returns the operation result.
func (c *Client) RestoreVolume(ctx context.Context, opts RestoreVolumeOptions) (*Operation, error) {
	op, err := c.RestoreVolumeAsync(ctx, opts)
	if err != nil {
		return nil, err
	}
	return c.wait(ctx, op)
}
