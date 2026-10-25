package cellbox

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"unicode"
)

var volumeExportIDPattern = regexp.MustCompile(`^exp-[a-f0-9]{32}$`)

func validateVolumeExportPath(value string) (string, error) {
	if value == "" {
		return ".", nil
	}
	if len(value) > 4096 || strings.HasPrefix(value, "/") || strings.ContainsRune(value, '\\') {
		return "", fmt.Errorf("cellbox: invalid Volume-relative export path")
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return "", fmt.Errorf("cellbox: export path cannot contain control characters")
		}
	}
	if value == "." {
		return value, nil
	}
	parts := strings.Split(value, "/")
	if len(parts) > 64 {
		return "", fmt.Errorf("cellbox: export path cannot exceed 64 segments")
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("cellbox: export path must be a clean relative path without dot or parent segments")
		}
	}
	return value, nil
}

func (c *Client) volumeExportPath(volumeID, exportID string) (string, error) {
	if !volumeIDPattern.MatchString(volumeID) {
		return "", fmt.Errorf("cellbox: invalid volume ID")
	}
	if !volumeExportIDPattern.MatchString(exportID) {
		return "", fmt.Errorf("cellbox: invalid Volume export ID")
	}
	path, err := resourcePath("/v1/volumes/", volumeID)
	if err != nil {
		return "", err
	}
	return resourcePath(path+"/exports/", exportID)
}

// CreateVolumeExportAsync submits a temporary archive request once. The Volume
// must be detached or referenced only by suspended Boxes. An empty path selects
// the Volume root. If waiting is interrupted, use the returned operation ID.
func (c *Client) CreateVolumeExportAsync(ctx context.Context, volumeID, directory string) (*Operation, error) {
	if !volumeIDPattern.MatchString(volumeID) {
		return nil, fmt.Errorf("cellbox: invalid volume ID")
	}
	cleanPath, err := validateVolumeExportPath(directory)
	if err != nil {
		return nil, err
	}
	path, err := resourcePath("/v1/volumes/", volumeID)
	if err != nil {
		return nil, err
	}
	return c.submit(ctx, path+"/exports", struct {
		Path string `json:"path"`
	}{Path: cleanPath})
}

// CreateVolumeExport waits for archive creation and returns its metadata.
func (c *Client) CreateVolumeExport(ctx context.Context, volumeID, directory string) (*VolumeExport, error) {
	op, err := c.CreateVolumeExportAsync(ctx, volumeID, directory)
	if err != nil {
		return nil, err
	}
	op, err = c.wait(ctx, op)
	if err != nil {
		return nil, err
	}
	exportID := op.Result["exportId"]
	if !volumeExportIDPattern.MatchString(exportID) {
		return nil, fmt.Errorf("cellbox: completed export operation is missing a valid export ID")
	}
	return c.GetVolumeExport(ctx, volumeID, exportID)
}

// GetVolumeExport returns metadata for a temporary Volume archive.
func (c *Client) GetVolumeExport(ctx context.Context, volumeID, exportID string) (*VolumeExport, error) {
	path, err := c.volumeExportPath(volumeID, exportID)
	if err != nil {
		return nil, err
	}
	var export VolumeExport
	if err := c.do(ctx, http.MethodGet, path, nil, &export); err != nil {
		return nil, err
	}
	return &export, nil
}

// OpenVolumeExport opens a streaming download. The caller must close the body.
// Large exports are subject to the configured HTTP timeout; provide a custom
// Config.HTTPClient when the default timeout is too short for the download.
func (c *Client) OpenVolumeExport(ctx context.Context, volumeID, exportID string) (io.ReadCloser, error) {
	path, err := c.volumeExportPath(volumeID, exportID)
	if err != nil {
		return nil, err
	}
	res, err := c.request(ctx, http.MethodGet, path+"/content", nil, "")
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

// DeleteVolumeExport removes a temporary archive. Deleting an already absent
// export is idempotent.
func (c *Client) DeleteVolumeExport(ctx context.Context, volumeID, exportID string) error {
	path, err := c.volumeExportPath(volumeID, exportID)
	if err != nil {
		return err
	}
	res, err := c.request(ctx, http.MethodDelete, path, nil, "")
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, err = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
	return err
}
