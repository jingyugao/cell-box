// SPDX-License-Identifier: Apache-2.0

package resumable

import (
	"context"
	"encoding/json"
	"errors"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/warmdefaults"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

func (p *Provider) SetWarmDefaultImage(ctx context.Context, namespace string, target boxprovider.WarmDefaultImage) error {
	if p == nil || p.Client == nil {
		return errors.New("Kubernetes client is required")
	}
	image := warmdefaults.Image{ProfileID: target.ProfileID, NodeName: target.NodeName, Image: target.Image}
	if err := warmdefaults.Validate(namespace, image.ProfileID, image); err != nil {
		return err
	}
	encoded, err := json.Marshal(image)
	if err != nil {
		return err
	}
	return retry.OnError(retry.DefaultRetry, func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err)
	}, func() error {
		key := types.NamespacedName{Namespace: namespace, Name: warmdefaults.ConfigMapName}
		current := &core.ConfigMap{}
		err := p.Client.Get(ctx, key, current)
		if apierrors.IsNotFound(err) {
			current = &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: warmdefaults.ConfigMapName}, Data: map[string]string{}}
			current.Data[image.ProfileID] = string(encoded)
			if createErr := p.Client.Create(ctx, current); createErr != nil {
				return createErr
			}
			return nil
		}
		if err != nil {
			return err
		}
		if current.Data == nil {
			current.Data = map[string]string{}
		}
		if current.Data[image.ProfileID] == string(encoded) {
			return nil
		}
		current.Data[image.ProfileID] = string(encoded)
		return p.Client.Update(ctx, current)
	})
}

func (p *Provider) GetWarmDefaultImages(ctx context.Context, namespace string) ([]boxprovider.WarmDefaultImage, error) {
	if p == nil || p.Client == nil {
		return nil, errors.New("Kubernetes client is required")
	}
	images, err := warmdefaults.Read(ctx, p.Client, namespace)
	if err != nil {
		return nil, err
	}
	out := make([]boxprovider.WarmDefaultImage, 0, len(images))
	for _, image := range images {
		out = append(out, boxprovider.WarmDefaultImage{ProfileID: image.ProfileID, NodeName: image.NodeName, Image: image.Image})
	}
	return out, nil
}

var _ boxprovider.WarmDefaultImageProvider = (*Provider)(nil)
