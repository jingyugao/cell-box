package node

import (
	"context"
	"errors"

	api "cellbox.local/cellbox/api/v1alpha1"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func (b *Backend) CacheImage(ctx context.Context, image string) error {
	spec := &cri.ImageSpec{Image: image, RuntimeHandler: api.RuntimeClass}
	status, err := b.Images.ImageStatus(ctx, &cri.ImageStatusRequest{Image: spec})
	if err != nil {
		return err
	}
	if status.Image != nil && status.Image.Id != "" {
		return nil
	}
	result, err := b.Images.PullImage(ctx, &cri.PullImageRequest{Image: spec})
	if err != nil {
		return err
	}
	if result.ImageRef == "" {
		return errors.New("CRI image pull returned no image reference")
	}
	return nil
}
