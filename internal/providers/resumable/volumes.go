package resumable

import (
	"context"
	"errors"

	"cellbox.local/cellbox/internal/boxprovider"
)

func (p *Provider) CreateVolume(ctx context.Context, spec boxprovider.VolumeSpec) error {
	if spec.UID == "" || spec.NodeName == "" || spec.Namespace == "" {
		return errors.New("volume UID, node, and namespace are required")
	}
	if p == nil || p.CreateVolumeLocal == nil {
		return errors.New("local volume creation is unavailable")
	}
	return p.CreateVolumeLocal(ctx, spec)
}

func (p *Provider) DeleteVolume(ctx context.Context, spec boxprovider.VolumeSpec) error {
	if spec.UID == "" || spec.NodeName == "" || spec.Namespace == "" {
		return errors.New("volume UID, node, and namespace are required")
	}
	if p == nil || p.DeleteVolumeLocal == nil {
		return errors.New("local volume deletion is unavailable")
	}
	return p.DeleteVolumeLocal(ctx, spec)
}

var _ boxprovider.VolumeProvider = (*Provider)(nil)
