package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/catalog"
	"net/http"
	"time"
)

const templateRuntimeMaxAge = 15 * time.Second

type WarmPoolStatus struct {
	PoolID         string                        `json:"poolId"`
	RequestedSlots int                           `json:"requestedSlots"`
	AllocatedSlots int                           `json:"allocatedSlots"`
	ReadySlots     int                           `json:"readySlots"`
	PreparingSlots int                           `json:"preparingSlots"`
	FailedSlots    int                           `json:"failedSlots"`
	Resources      boxprovider.WarmPoolResources `json:"resources"`
	ObservedAt     time.Time                     `json:"observedAt"`
	Stale          bool                          `json:"stale"`
}

type RevisionWarmStatus struct {
	RevisionID         string          `json:"revisionId"`
	Generation         uint64          `json:"generation"`
	ObservedGeneration uint64          `json:"observedGeneration"`
	DesiredSlots       int             `json:"desiredSlots"`
	SyncStatus         string          `json:"syncStatus"`
	Error              string          `json:"error,omitempty"`
	ObservedAt         *time.Time      `json:"observedAt,omitempty"`
	Stale              bool            `json:"stale"`
	Pool               *WarmPoolStatus `json:"pool"`
}

type RevisionRuntimeStatus struct {
	RevisionID   string             `json:"revisionId"`
	SandboxCount *int               `json:"sandboxCount"`
	Phases       map[string]int     `json:"phases"`
	ObservedAt   *time.Time         `json:"observedAt,omitempty"`
	Stale        bool               `json:"stale"`
	Error        string             `json:"error,omitempty"`
	WarmPool     RevisionWarmStatus `json:"warmPool"`
}

type TemplateRuntimeStatus struct {
	TemplateID string                  `json:"templateId"`
	Revisions  []RevisionRuntimeStatus `json:"revisions"`
}

// DesiredWarmTemplates reads SQLite directly; only the controller keeps an observed snapshot.
func (s *Service) DesiredWarmTemplates(ctx context.Context) ([]boxprovider.WarmTemplate, error) {
	records, err := s.catalog.RuntimeRevisions(ctx)
	if err != nil {
		return nil, err
	}
	out := []boxprovider.WarmTemplate{}
	for _, r := range records {
		var p Profile
		if err = json.Unmarshal(r.Profile, &p); err != nil {
			return nil, err
		}
		// Slots no longer create warm-pool demand (it follows checkpoints), but every
		// active revision stays listed so its pool can be reported against it.
		if p.Provider != "resumable-k8s-pod" || r.Revision.Status != "active" {
			continue
		}
		if !s.configuredWarmLocation(p) {
			if effectiveWarmSlots(r.Revision) == 0 {
				continue
			}
			return nil, fmt.Errorf("revision %s targets an unavailable warm-pool location", r.Revision.ID)
		}
		out = append(out, boxprovider.WarmTemplate{ID: r.Revision.ID, Slots: effectiveWarmSlots(r.Revision), Spec: runtimeSpec(boxRecord{Box: Box{ID: r.Revision.ID}, ClientID: r.ClientID, Profile: p})})
	}
	return out, nil
}
func WarmDesiredHash(values []boxprovider.WarmTemplate) string {
	data, _ := json.Marshal(values)
	return hash(string(data))
}
func (s *Service) configuredWarmLocation(p Profile) bool {
	for _, c := range s.config.Profiles {
		if c.Provider == p.Provider && c.Namespace == p.Namespace && c.NodeName == p.NodeName {
			return true
		}
	}
	return false
}
func effectiveWarmSlots(v catalog.Revision) int {
	if v.Status != "active" {
		return 0
	}
	return v.WarmSlots
}

func (s *Service) revisionWarmStatus(ctx context.Context, v catalog.Revision, p Profile, desiredHash string) RevisionWarmStatus {
	state := RevisionWarmStatus{RevisionID: v.ID, Generation: v.Generation, DesiredSlots: effectiveWarmSlots(v), SyncStatus: "pending", Stale: true}
	provider, ok := s.providers[p.Provider].(boxprovider.WarmTemplateObserver)
	if !ok || !s.configuredWarmLocation(p) {
		state.SyncStatus = "unsupported"
		return state
	}
	snapshot, err := provider.ObserveWarmTemplates(ctx, p.Namespace, []string{p.NodeName})
	if err != nil {
		state.Error = err.Error()
		return state
	}
	state.ObservedAt = &snapshot.ObservedAt
	state.Stale = snapshot.DesiredHash != desiredHash || snapshot.ObservedAt.IsZero() || time.Since(snapshot.ObservedAt) > templateRuntimeMaxAge
	for _, pool := range snapshot.Pools {
		for _, id := range pool.TemplateIDs {
			if id == v.ID {
				state.Pool = &WarmPoolStatus{PoolID: pool.PoolID, RequestedSlots: pool.RequestedSlots, AllocatedSlots: pool.AllocatedSlots, ReadySlots: pool.ReadySlots, PreparingSlots: pool.PreparingSlots, FailedSlots: pool.FailedSlots, Resources: pool.Resources, ObservedAt: pool.ObservedAt, Stale: state.Stale || pool.Stale}
			}
		}
	}
	if !state.Stale {
		state.SyncStatus = "applied"
		state.ObservedGeneration = v.Generation
	}
	return state
}

func (s *Service) registerTemplateRuntime(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/template-revisions/{id}/warm-pool", func(w http.ResponseWriter, r *http.Request) {
		v, raw, err := s.catalog.RevisionSnapshot(r.Context(), clientID(r), r.PathValue("id"))
		if err != nil {
			fail(w, catalogAPIError(err))
			return
		}
		var p Profile
		if err = json.Unmarshal(raw, &p); err != nil {
			fail(w, err)
			return
		}
		desired, err := s.DesiredWarmTemplates(r.Context())
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, s.revisionWarmStatus(r.Context(), v, p, WarmDesiredHash(desired)))
	})
	mux.HandleFunc("GET /v1/templates/{id}/runtime", s.templateRuntimeStatus)
}
func (s *Service) templateRuntimeStatus(w http.ResponseWriter, r *http.Request) {
	client, id := clientID(r), r.PathValue("id")
	if _, err := s.catalog.TemplateSnapshot(r.Context(), client, id); err != nil {
		fail(w, catalogAPIError(err))
		return
	}
	records, err := s.catalog.TemplateRuntimeRevisions(r.Context(), client, id)
	if err != nil {
		fail(w, err)
		return
	}
	desired, err := s.DesiredWarmTemplates(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	desiredHash := WarmDesiredHash(desired)
	// One inventory read per provider/namespace for this request; no background counts cache.
	type inventoryResult struct {
		boxes []Box
		err   error
	}
	groups := map[string]inventoryResult{}
	out := TemplateRuntimeStatus{TemplateID: id, Revisions: []RevisionRuntimeStatus{}}
	for _, record := range records {
		var p Profile
		if err = json.Unmarshal(record.Profile, &p); err != nil {
			fail(w, err)
			return
		}
		key := p.Provider + "/" + p.Namespace
		group, exists := groups[key]
		if !exists {
			if provider, ok := s.providers[p.Provider].(boxprovider.ResourceInventoryProvider); ok {
				attempt, cancel := context.WithTimeout(r.Context(), 10*time.Second)
				rows, e := provider.ListResources(attempt, p.Namespace, client)
				cancel()
				group.err = e
				for _, row := range rows {
					var b Box
					if e = json.Unmarshal(row.Box, &b); e != nil {
						group.err = e
						break
					}
					group.boxes = append(group.boxes, b)
				}
			} else if p.Provider == "docker" {
				rows, e := s.store.BoxRecords(client, nil)
				group.err = e
				for _, b := range rows {
					if b.Profile.Provider == p.Provider {
						group.boxes = append(group.boxes, b.Box)
					}
				}
			} else {
				group.err = fmt.Errorf("runtime inventory unavailable")
			}
			groups[key] = group
		}
		state := RevisionRuntimeStatus{RevisionID: record.Revision.ID, WarmPool: s.revisionWarmStatus(r.Context(), record.Revision, p, desiredHash)}
		if group.err != nil {
			state.Stale = true
			state.Error = "Runtime inventory observation failed"
		} else {
			count := 0
			state.Phases = map[string]int{}
			for _, b := range group.boxes {
				if b.RevisionID == record.Revision.ID && b.Phase != "deleted" {
					count++
					state.Phases[b.Phase]++
				}
			}
			now := time.Now().UTC()
			state.ObservedAt = &now
			state.SandboxCount = &count
		}
		out.Revisions = append(out.Revisions, state)
	}
	writeJSON(w, 200, out)
}
