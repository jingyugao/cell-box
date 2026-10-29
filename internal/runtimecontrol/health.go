package runtimecontrol

import (
	"context"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// HealthResources includes locally committed resumes before API publication.
func (c *Client) HealthResources(ctx context.Context, namespace string) ([]*api.ResumablePod, error) {
	list := &api.ResumablePodList{}
	listCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	listErr := c.Client.List(listCtx, list, client.InNamespace(namespace))
	cancel()
	resources := make(map[client.ObjectKey]*api.ResumablePod)
	for i := range list.Items {
		w := &list.Items[i]
		resources[client.ObjectKeyFromObject(w)] = w
	}
	if journal := c.getJournal(); journal != nil {
		pending, err := journal.ListPending(ctx)
		if err != nil {
			return nil, err
		}
		for _, record := range pending {
			if record.Resource.Namespace == namespace {
				resources[client.ObjectKeyFromObject(record.Resource)] = record.Resource
			}
		}
	}
	out := make([]*api.ResumablePod, 0, len(resources))
	for key, w := range resources {
		if record, err := c.LocalRecord(ctx, key); err != nil {
			return nil, err
		} else if record != nil {
			w = record.Resource
		}
		out = append(out, w.DeepCopy())
	}
	// Continue checking locally owned work even while the Kubernetes API is down.
	if len(out) == 0 {
		return nil, listErr
	}
	return out, nil
}
