package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/image"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var guestProtocolCache sync.Map // immutable image digest -> needs legacy unquiesce

// NeedsGuestUnquiesce reports whether an image contains a guest protocol that
// requires the legacy unquiesce request after restore. Images without the
// protocol label are treated as legacy for checkpoint compatibility.
func (b *Backend) NeedsGuestUnquiesce(ctx context.Context, w *api.ResumablePod) (bool, error) {
	if b == nil || b.Images == nil || w == nil {
		return false, errors.New("guest protocol inspection requires workload and CRI image service")
	}
	imageRef := w.Spec.Container.Image
	digestAt := strings.LastIndex(imageRef, "@sha256:")
	if digestAt < 0 || !image.ValidRegistryImage(imageRef) {
		return false, errors.New("guest protocol inspection requires an immutable image digest")
	}
	digest := imageRef[digestAt+1:]
	if cached, ok := guestProtocolCache.Load(digest); ok {
		return cached.(bool), nil
	}

	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	response, err := b.Images.ImageStatus(queryCtx, &cri.ImageStatusRequest{
		Image:   &cri.ImageSpec{Image: imageRef, RuntimeHandler: b.runtimeHandler()},
		Verbose: true,
	})
	if err != nil {
		return false, fmt.Errorf("inspect guest protocol for image %s: %w", digest, err)
	}
	if response == nil || response.Image == nil || response.Image.Id == "" {
		return false, fmt.Errorf("CRI returned no local image for guest protocol inspection")
	}
	info := response.Info["info"]
	if info == "" {
		return false, errors.New("CRI returned no verbose image metadata")
	}
	var metadata struct {
		ImageSpec struct {
			Config struct {
				Labels map[string]string `json:"Labels"`
			} `json:"config"`
		} `json:"imageSpec"`
	}
	if err := json.Unmarshal([]byte(info), &metadata); err != nil {
		return false, fmt.Errorf("decode guest protocol image metadata: %w", err)
	}
	protocol := metadata.ImageSpec.Config.Labels[image.GuestProtocolLabel]
	needsUnquiesce := protocol == ""
	if protocol != "" && protocol != image.GuestProtocolVersion {
		return false, fmt.Errorf("unsupported guest protocol version %q for image %s", protocol, digest)
	}
	guestProtocolCache.Store(digest, needsUnquiesce)
	return needsUnquiesce, nil
}
