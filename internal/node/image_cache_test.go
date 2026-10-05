package node

import (
	"context"
	"errors"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	"google.golang.org/grpc"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type cachingImages struct {
	cri.ImageServiceClient
	cached  bool
	pulled  string
	handler string
	err     error
}

func (c *cachingImages) ImageStatus(context.Context, *cri.ImageStatusRequest, ...grpc.CallOption) (*cri.ImageStatusResponse, error) {
	result := &cri.ImageStatusResponse{}
	if c.cached {
		result.Image = &cri.Image{Id: "existing"}
	}
	return result, nil
}
func (c *cachingImages) PullImage(_ context.Context, req *cri.PullImageRequest, _ ...grpc.CallOption) (*cri.PullImageResponse, error) {
	c.pulled = req.Image.Image
	c.handler = req.Image.RuntimeHandler
	return &cri.PullImageResponse{ImageRef: "pulled"}, c.err
}
func TestCacheImageUsesRuntimeStoreAndPropagatesPullFailure(t *testing.T) {
	images := &cachingImages{cached: true}
	b := &Backend{Images: images}
	if err := b.CacheImage(context.Background(), "prepared@sha256:digest"); err != nil || images.pulled != "" {
		t.Fatalf("cached image pulled again: %v", err)
	}
	images.cached = false
	if err := b.CacheImage(context.Background(), "prepared@sha256:digest"); err != nil || images.pulled != "prepared@sha256:digest" {
		t.Fatalf("prepared image not pulled: %v", err)
	}
	if images.handler != api.RuntimeClass {
		t.Fatal("image cached for a different runtime")
	}
	images.err = errors.New("registry unavailable")
	if err := b.CacheImage(context.Background(), "prepared@sha256:digest"); !errors.Is(err, images.err) {
		t.Fatalf("pull failure hidden: %v", err)
	}
}
