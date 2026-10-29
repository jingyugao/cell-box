package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/guestapi"
	"github.com/jingyugao/cell-box/internal/homevolume"
)

// WithStoppedVolume exposes an independent volume's data directory while
// validating its on-disk identity and holding its per-volume lifecycle lock.
func (b *Backend) WithStoppedVolume(ctx context.Context, uid, node string, work func(string) error) error {
	if strings.TrimSpace(node) == "" {
		return fmt.Errorf("node name is required")
	}
	if b == nil || b.Base == "" {
		return fmt.Errorf("node volume storage is unavailable")
	}
	err := homevolume.WithVolume(ctx, b.Base, uid, node, work)
	if errors.Is(err, homevolume.ErrStaleVolume) {
		return fmt.Errorf("%w: persistent volume identity is stale", boxprovider.ErrStaleVolume)
	}
	return err
}

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
