package resumable

import (
	"context"
	"errors"
	"fmt"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/boxprovider"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// WithStoppedVolume checks every Box CR in the namespace that references this
// independent volume, then delegates to the node-local identity and lock
// checks. A volume with no remaining Box CRs is still directly accessible.
func (p *Provider) WithStoppedVolume(ctx context.Context, spec boxprovider.VolumeSpec, work func(string) error) error {
	if spec.UID == "" || spec.NodeName == "" || spec.Namespace == "" {
		return errors.New("volume UID, node, and namespace are required")
	}
	if work == nil {
		return errors.New("volume callback is required")
	}
	if p == nil || p.Client == nil {
		return errors.New("Kubernetes client is required")
	}
	if p.WithStoppedVolumeLocal == nil {
		return errors.New("local stopped-volume access is unavailable")
	}
	if err := p.requireStoppedVolumeAttachments(ctx, spec); err != nil {
		return err
	}
	return p.WithStoppedVolumeLocal(ctx, spec, func(root string) error {
		// Recheck after taking the node volume lock to narrow the transition race.
		if err := p.requireStoppedVolumeAttachments(ctx, spec); err != nil {
			return err
		}
		return work(root)
	})
}

func (p *Provider) requireStoppedVolumeAttachments(ctx context.Context, spec boxprovider.VolumeSpec) error {
	list := &api.ResumablePodList{}
	if err := p.Client.List(ctx, list, client.InNamespace(spec.Namespace)); err != nil {
		return fmt.Errorf("list volume attachments: %w", err)
	}
	for i := range list.Items {
		box := &list.Items[i]
		if !referencesIndependentVolume(box, spec.UID) {
			continue
		}
		if box.Spec.NodeName != spec.NodeName || box.DeletionTimestamp != nil || box.Status.PodName != "" || box.Status.PodUID != "" {
			return boxprovider.ErrNotReady
		}
		suspended := box.Status.Phase == "Suspended" && box.Spec.DesiredState == "Suspended"
		failed := box.Status.Phase == "Failed"
		if !suspended && !failed {
			return boxprovider.ErrNotReady
		}
	}
	return nil
}

func referencesIndependentVolume(box *api.ResumablePod, uid string) bool {
	if box == nil {
		return false
	}
	if box.Spec.HomeVolumeUID == uid {
		return true
	}
	for _, mount := range box.Spec.Mounts {
		if mount.UID == uid {
			return true
		}
	}
	return false
}

var _ boxprovider.VolumeProvider = (*Provider)(nil)
var _ boxprovider.StoppedVolumeProvider = (*Provider)(nil)
