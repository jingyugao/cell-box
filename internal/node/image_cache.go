package node

import (
	"context"
	"errors"

	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func (b *Backend) CacheImage(ctx context.Context, image string) error {
	spec := &cri.ImageSpec{Image: image, RuntimeHandler: b.runtimeHandler()}
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
