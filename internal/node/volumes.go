package node

import (
	"fmt"
	"strings"

	"cellbox.local/cellbox/internal/homevolume"
)

// CreateVolume initializes an independently managed HOME volume.
func (b *Backend) CreateVolume(uid, node string) error {
	if strings.TrimSpace(node) == "" {
		return fmt.Errorf("node name is required")
	}
	if _, err := homevolume.Prepare(b.Base, uid, node, homevolume.VolumeSpecHash, true); err != nil {
		return fmt.Errorf("create HOME volume: %w", err)
	}
	return nil
}

// DeleteVolume removes an independently managed volume after identity checks.
func (b *Backend) DeleteVolume(uid, node string) error {
	if strings.TrimSpace(node) == "" {
		return fmt.Errorf("node name is required")
	}
	return homevolume.RemoveVolume(b.Base, uid, node)
}
