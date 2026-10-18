package node

import (
	"fmt"
	"os"
	"strings"
	"syscall"

	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/homevolume"
)

// CreateVolume initializes an independently managed HOME volume.
func (b *Backend) CreateVolume(uid, node string, agent guestapi.Identity) error {
	if strings.TrimSpace(node) == "" {
		return fmt.Errorf("node name is required")
	}
	if _, err := homevolume.Prepare(b.Base, uid, node, homevolume.VolumeSpecHash, true); err != nil {
		return fmt.Errorf("create HOME volume: %w", err)
	}
	// Every managed mount must be writable by its configured guest identity,
	// including destinations that the HOME/workspace bootstrap does not visit.
	path, err := homevolume.Path(b.Base, uid)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Chown(int(agent.UID), int(agent.GID)); err != nil {
		return fmt.Errorf("set volume guest ownership: %w", err)
	}
	if err := f.Chmod(0700); err != nil {
		return fmt.Errorf("set volume directory permissions: %w", err)
	}
	return f.Sync()
}

// DeleteVolume removes an independently managed volume after identity checks.
func (b *Backend) DeleteVolume(uid, node string) error {
	if strings.TrimSpace(node) == "" {
		return fmt.Errorf("node name is required")
	}
	return homevolume.RemoveVolume(b.Base, uid, node)
}
