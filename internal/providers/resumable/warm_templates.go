// SPDX-License-Identifier: Apache-2.0

package resumable

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/warmtemplates"
)

// BuildWarmTemplates converts catalog revisions into the exact runtime specs
// shared by the controller and provider, and returns a stable configuration hash.
func BuildWarmTemplates(namespace string, templates []boxprovider.WarmTemplate) ([]warmtemplates.Template, string, error) {
	records := make([]warmtemplates.Template, 0, len(templates))
	seen := make(map[string]bool, len(templates))
	for _, template := range templates {
		if template.Spec.Namespace != namespace || template.Slots < 0 || seen[template.ID] {
			return nil, "", errors.New("invalid or duplicate warm template")
		}
		seen[template.ID] = true
		spec := template.Spec
		spec.BoxID = "warm-" + template.ID
		wanted, err := buildWanted(spec)
		if err != nil {
			return nil, "", fmt.Errorf("build warm template %q: %w", template.ID, err)
		}
		record := warmtemplates.Template{ID: template.ID, Slots: template.Slots, Spec: wanted.Spec}
		if err := warmtemplates.Validate(namespace, template.ID, record); err != nil {
			return nil, "", err
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	hash, err := warmtemplates.HashTemplates(records)
	return records, hash, err
}

// SetWarmSnapshot accepts the controller's latest single-node observation.
func (p *Provider) SetWarmSnapshot(snapshot warmtemplates.Snapshot) {
	if p == nil {
		return
	}
	p.warmMu.Lock()
	p.warmSnapshot = warmtemplates.CloneSnapshot(snapshot)
	p.warmMu.Unlock()
}

// ObserveWarmTemplates returns the last controller observation. The desired
// hash is carried in the snapshot so callers can compare it with SQLite state.
func (p *Provider) ObserveWarmTemplates(_ context.Context, _ string, nodeNames []string) (boxprovider.WarmSnapshot, error) {
	if p == nil {
		return boxprovider.WarmSnapshot{}, errors.New("resumable provider is required")
	}
	p.warmMu.RLock()
	snapshot := warmtemplates.CloneSnapshot(p.warmSnapshot)
	p.warmMu.RUnlock()
	if snapshot.NodeName == "" || snapshot.ObservedAt.IsZero() {
		return boxprovider.WarmSnapshot{}, errors.New("warm pool observation is unavailable")
	}
	if len(nodeNames) > 0 {
		found := false
		for _, node := range nodeNames {
			found = found || node == snapshot.NodeName
		}
		if !found {
			return boxprovider.WarmSnapshot{}, fmt.Errorf("warm pool observation for requested nodes is unavailable")
		}
	}
	stale := time.Since(snapshot.ObservedAt) > 15*time.Second
	out := snapshot.Pools
	for i := range out {
		out[i].Stale = stale
	}
	return boxprovider.WarmSnapshot{DesiredHash: snapshot.DesiredHash, ObservedAt: snapshot.ObservedAt, Pools: out}, nil
}

var _ boxprovider.WarmTemplateObserver = (*Provider)(nil)
