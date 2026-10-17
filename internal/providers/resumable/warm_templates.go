// SPDX-License-Identifier: Apache-2.0

package resumable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/warmtemplates"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

func (p *Provider) SetWarmTemplates(ctx context.Context, namespace string, templates []boxprovider.WarmTemplate) error {
	if p == nil || (p.Client == nil && p.WarmTemplates == nil) {
		return errors.New("Kubernetes client is required")
	}
	data := make(map[string]string, len(templates))
	records := make([]warmtemplates.Template, 0, len(templates))
	seen := make(map[string]bool, len(templates))
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
		if seen[template.ID] {
			return errors.New("duplicate warm template ID")
		}
		seen[template.ID] = true
		if p.WarmTemplates == nil {
			encoded, err := json.Marshal(record)
			if err != nil {
				return err
			}
			data[template.ID] = string(encoded)
		}
		records = append(records, record)
	}
	if p.WarmTemplates != nil {
		return p.WarmTemplates.Set(namespace, records)
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

// ObserveWarmTemplates returns controller snapshots. A stale snapshot retains
// its timestamp and counters so callers can label it accurately; an absent or
// malformed snapshot is an error and must not be presented as zero usage.
func (p *Provider) ObserveWarmTemplates(ctx context.Context, namespace string, nodeNames []string) ([]boxprovider.WarmPoolObservation, error) {
	if p == nil || (p.Client == nil && p.WarmTemplates == nil) {
		return nil, errors.New("Kubernetes client is required")
	}
	if p.WarmTemplates != nil {
		return p.observeLocalWarmTemplates(namespace, nodeNames)
	}
	desired := &core.ConfigMap{}
	err := p.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: warmtemplates.ConfigMapName}, desired)
	if apierrors.IsNotFound(err) {
		desired.Data = map[string]string{}
		err = nil
	}
	if err != nil {
		return nil, err
	}
	templates, err := warmtemplates.Read(ctx, p.Client, namespace)
	if err != nil {
		return nil, err
	}
	nodes := map[string]bool{}
	for _, node := range nodeNames {
		if node != "" {
			nodes[node] = true
		}
	}
	for _, template := range templates {
		nodes[template.Spec.NodeName] = true
	}
	if len(nodes) == 0 {
		return []boxprovider.WarmPoolObservation{}, nil
	}
	desiredHash := warmtemplates.DesiredHash(desired.Data)
	out := []boxprovider.WarmPoolObservation{}
	now := time.Now()
	for node := range nodes {
		cm := &core.ConfigMap{}
		if err := p.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: warmtemplates.StatusConfigMapName(node)}, cm); err != nil {
			return nil, fmt.Errorf("warm pool observation for node %q is unavailable: %w", node, err)
		}
		var snapshot warmtemplates.Snapshot
		if err := json.Unmarshal([]byte(cm.Data["snapshot.json"]), &snapshot); err != nil {
			return nil, fmt.Errorf("decode warm pool snapshot %q: %w", cm.Name, err)
		}
		if snapshot.NodeName != node {
			return nil, fmt.Errorf("warm pool snapshot %q node identity mismatch", cm.Name)
		}
		observedAt := snapshot.ObservedAt
		if observedAt.IsZero() {
			return nil, fmt.Errorf("warm pool snapshot %q has no observation timestamp", cm.Name)
		}
		stale := snapshot.DesiredHash != desiredHash || snapshot.ObservedGeneration != desired.Generation || now.Sub(observedAt) > 15*time.Second
		if stale && len(snapshot.Pools) == 0 {
			return nil, fmt.Errorf("empty warm pool snapshot for node %q is stale", node)
		}
		for _, pool := range snapshot.Pools {
			pool.Stale = stale
			out = append(out, pool)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PoolID < out[j].PoolID })
	return out, nil
}

func (p *Provider) observeLocalWarmTemplates(namespace string, nodeNames []string) ([]boxprovider.WarmPoolObservation, error) {
	templates, desiredHash, generation, initialized := p.WarmTemplates.Read(namespace)
	if !initialized {
		return nil, fmt.Errorf("warm template configuration for namespace %q is not initialized", namespace)
	}
	nodes := map[string]bool{}
	for _, node := range nodeNames {
		if node != "" {
			nodes[node] = true
		}
	}
	for _, template := range templates {
		nodes[template.Spec.NodeName] = true
	}
	out := []boxprovider.WarmPoolObservation{}
	now := time.Now()
	for node := range nodes {
		snapshot, ok := p.WarmTemplates.Snapshot(namespace, node)
		if !ok {
			return nil, fmt.Errorf("warm pool observation for node %q is unavailable", node)
		}
		if snapshot.NodeName != node || snapshot.ObservedAt.IsZero() {
			return nil, fmt.Errorf("warm pool snapshot for node %q is invalid", node)
		}
		stale := snapshot.DesiredHash != desiredHash || snapshot.ObservedGeneration != generation || now.Sub(snapshot.ObservedAt) > 15*time.Second
		if stale && len(snapshot.Pools) == 0 {
			return nil, fmt.Errorf("empty warm pool snapshot for node %q is stale", node)
		}
		for _, pool := range snapshot.Pools {
			pool.Stale = stale
			out = append(out, pool)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PoolID < out[j].PoolID })
	return out, nil
}

var _ boxprovider.WarmTemplateObserver = (*Provider)(nil)
