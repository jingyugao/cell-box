// SPDX-License-Identifier: Apache-2.0

package resumable

import (
	"context"
	"encoding/json"
	"errors"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/warmtemplates"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

func (p *Provider) SetWarmTemplates(ctx context.Context, namespace string, templates []boxprovider.WarmTemplate) error {
	if p == nil || p.Client == nil {
		return errors.New("Kubernetes client is required")
	}
	data := make(map[string]string, len(templates))
	for _, template := range templates {
		if template.Spec.Namespace != namespace {
			return errors.New("warm template namespace mismatch")
		}
		if template.Slots == 0 {
			continue
		}
		if template.Slots < 0 {
			return errors.New("warm template slots cannot be negative")
		}
		spec := template.Spec
		spec.BoxID = "warm-" + template.ID
		wanted, err := buildWanted(spec)
		if err != nil {
			return err
		}
		record := warmtemplates.Template{ID: template.ID, Slots: template.Slots, Spec: wanted.Spec}
		if err := warmtemplates.Validate(namespace, template.ID, record); err != nil {
			return err
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if _, exists := data[template.ID]; exists {
			return errors.New("duplicate warm template ID")
		}
		data[template.ID] = string(encoded)
	}
	return retry.OnError(retry.DefaultRetry, func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err)
	}, func() error {
		key := types.NamespacedName{Namespace: namespace, Name: warmtemplates.ConfigMapName}
		current := &core.ConfigMap{}
		err := p.Client.Get(ctx, key, current)
		if apierrors.IsNotFound(err) {
			current = &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: warmtemplates.ConfigMapName}, Data: data}
			return p.Client.Create(ctx, current)
		}
		if err != nil {
			return err
		}
		if len(current.Data) == len(data) {
			same := true
			for key, value := range data {
				if current.Data[key] != value {
					same = false
					break
				}
			}
			if same {
				return nil
			}
		}
		current.Data = data
		return p.Client.Update(ctx, current)
	})
}

var _ boxprovider.WarmTemplateProvider = (*Provider)(nil)
