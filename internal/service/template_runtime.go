package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/catalog"
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

type templateRuntimeCache struct {
	mu    sync.RWMutex
	warm  map[string]RevisionWarmStatus
	usage map[string]RevisionRuntimeStatus
}

func (s *Service) startTemplateRuntime() {
	// Background runtime work is separate from catalog HTTP requests. In
	// particular, catalog startup remains available during a Kubernetes outage.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		timer := time.NewTicker(2 * time.Second)
		defer timer.Stop()
		for {
			s.syncTemplateRuntime(s.ctx)
			select {
			case <-s.ctx.Done():
				return
			case <-timer.C:
			}
		}
	}()
}

type warmGroup struct {
	provider  boxprovider.WarmTemplateProvider
	namespace string
	records   []catalog.RevisionRecord
	templates []boxprovider.WarmTemplate
}

func (s *Service) syncTemplateRuntime(ctx context.Context) {
	records, err := s.catalog.RuntimeRevisions(ctx)
	if err != nil {
		return
	} // Existing snapshots age out; never fabricate zero counts.
	groups := map[string]*warmGroup{}
	for _, p := range s.config.Profiles {
		if p.Provider != "resumable-k8s-pod" {
			continue
		}
		provider, ok := s.providers[p.Provider].(boxprovider.WarmTemplateProvider)
		if ok {
			groups[p.Provider+"/"+p.Namespace] = &warmGroup{provider: provider, namespace: p.Namespace}
		}
	}
	next := map[string]RevisionWarmStatus{}
	invalid := map[string]bool{}
	for _, r := range records {
		v := r.Revision
		state := RevisionWarmStatus{RevisionID: v.ID, Generation: v.Generation, DesiredSlots: effectiveWarmSlots(v), SyncStatus: "pending", Stale: true}
		var p Profile
		if err := json.Unmarshal(r.Profile, &p); err != nil {
			state.SyncStatus, state.Error = "error", "Invalid runtime profile snapshot"
			// A corrupt snapshot cannot be safely interpreted as a withdrawn demand.
			for key := range groups {
				invalid[key] = true
			}
		} else if group := groups[p.Provider+"/"+p.Namespace]; group != nil && s.configuredWarmLocation(p) {
			group.records = append(group.records, r)
			if state.DesiredSlots > 0 {
				spec := runtimeSpec(boxRecord{Box: Box{ID: v.ID}, ClientID: r.ClientID, Profile: p})
				group.templates = append(group.templates, boxprovider.WarmTemplate{ID: v.ID, Slots: state.DesiredSlots, Spec: spec})
			}
		} else {
			state.SyncStatus, state.Error = "unsupported", "No warm-pool provider configured for this revision location"
		}
		next[v.ID] = state
	}
	for key, group := range groups {
		attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
		var observations []boxprovider.WarmPoolObservation
		var err error
		if invalid[key] {
			err = fmt.Errorf("Invalid catalog profile snapshot; preserving published demand")
		} else {
			err = group.provider.SetWarmTemplates(attempt, group.namespace, group.templates)
		}
		if err == nil {
			observer, ok := group.provider.(boxprovider.WarmTemplateObserver)
			if !ok {
				err = fmt.Errorf("Warm-pool observation unavailable")
			} else {
				nodes := []string{}
				for _, profile := range s.config.Profiles {
					if profile.Provider == "resumable-k8s-pod" && profile.Namespace == group.namespace {
						nodes = append(nodes, profile.NodeName)
					}
				}
				observations, err = observer.ObserveWarmTemplates(attempt, group.namespace, nodes)
			}
		}
		cancel()
		now := time.Now().UTC()
		byID := map[string]boxprovider.WarmPoolObservation{}
		allCurrent := true
		for _, pool := range observations {
			if pool.Stale || now.Sub(pool.ObservedAt) > templateRuntimeMaxAge {
				allCurrent = false
			}
			for _, id := range pool.TemplateIDs {
				byID[id] = pool
			}
		}
		for _, r := range group.records {
			state := next[r.Revision.ID]
			if err != nil {
				state.SyncStatus, state.Error = "error", "Warm-pool synchronization or observation failed"
				// Keep the last known pool for diagnostics, clearly marked stale.
				s.templateRuntime.mu.RLock()
				old := s.templateRuntime.warm[r.Revision.ID]
				s.templateRuntime.mu.RUnlock()
				state.Pool, state.ObservedAt, state.ObservedGeneration = old.Pool, old.ObservedAt, old.ObservedGeneration
			} else {
				state.ObservedAt = &now
				if pool, ok := byID[r.Revision.ID]; ok {
					state.Pool = &WarmPoolStatus{PoolID: pool.PoolID, RequestedSlots: pool.RequestedSlots, AllocatedSlots: pool.AllocatedSlots, ReadySlots: pool.ReadySlots, PreparingSlots: pool.PreparingSlots, FailedSlots: pool.FailedSlots, Resources: pool.Resources, ObservedAt: pool.ObservedAt, Stale: pool.Stale || now.Sub(pool.ObservedAt) > templateRuntimeMaxAge}
					state.Stale = state.Pool.Stale
				} else if state.DesiredSlots == 0 && allCurrent {
					state.Stale = false
				}
				if !state.Stale {
					state.ObservedGeneration, state.SyncStatus = r.Revision.Generation, "applied"
				}
			}
			next[r.Revision.ID] = state
		}
	}
	s.templateRuntime.mu.Lock()
	s.templateRuntime.warm = next
	s.templateRuntime.mu.Unlock()
	s.syncTemplateUsage(ctx, records)
}

func (s *Service) configuredWarmLocation(p Profile) bool {
	for _, configured := range s.config.Profiles {
		if configured.Provider == p.Provider && configured.Namespace == p.Namespace && configured.NodeName == p.NodeName {
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

func (s *Service) syncTemplateUsage(ctx context.Context, records []catalog.RevisionRecord) {
	// Group by client and runtime namespace; one CR-only LIST per group, never
	// an Inspect or guest probe per sandbox. Status handlers read this cache.
	type usageGroup struct {
		profile   Profile
		client    string
		revisions []string
	}
	groups := map[string]*usageGroup{}
	for _, r := range records {
		var p Profile
		if json.Unmarshal(r.Profile, &p) != nil {
			continue
		}
		key := p.Provider + "/" + p.Namespace + "/" + r.ClientID
		if groups[key] == nil {
			groups[key] = &usageGroup{profile: p, client: r.ClientID}
		}
		groups[key].revisions = append(groups[key].revisions, r.Revision.ID)
	}
	for _, group := range groups {
		boxes := []Box{}
		var err error
		if provider, ok := s.providers[group.profile.Provider].(boxprovider.ResourceInventoryProvider); ok {
			attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
			rows, e := provider.ListResources(attempt, group.profile.Namespace, group.client)
			cancel()
			err = e
			if err == nil {
				for _, row := range rows {
					var box Box
					if e := json.Unmarshal(row.Box, &box); e != nil {
						err = e
						break
					}
					boxes = append(boxes, box)
				}
			}
		} else if group.profile.Provider == "docker" {
			err = s.store.View(func(st State) error {
				for _, r := range st.Boxes {
					if r.ClientID == group.client && r.Profile.Provider == group.profile.Provider && r.Box.Phase != "deleted" {
						boxes = append(boxes, r.Box)
					}
				}
				return nil
			})
		} else {
			err = fmt.Errorf("Runtime inventory unavailable")
		}
		now := time.Now().UTC()
		counts := map[string]map[string]int{}
		for _, box := range boxes {
			if box.Phase == "deleted" {
				continue
			}
			if counts[box.RevisionID] == nil {
				counts[box.RevisionID] = map[string]int{}
			}
			counts[box.RevisionID][box.Phase]++
		}
		s.templateRuntime.mu.Lock()
		if s.templateRuntime.usage == nil {
			s.templateRuntime.usage = map[string]RevisionRuntimeStatus{}
		}
		for _, id := range group.revisions {
			state := s.templateRuntime.usage[id]
			state.RevisionID = id
			if err != nil {
				state.Stale = true
				state.Error = "Runtime inventory observation failed"
			} else {
				phases := counts[id]
				if phases == nil {
					phases = map[string]int{}
				}
				total := 0
				for _, n := range phases {
					total += n
				}
				state.SandboxCount = &total
				state.Phases = phases
				state.Stale = false
				state.Error = ""
				state.ObservedAt = &now
			}
			s.templateRuntime.usage[id] = state
		}
		s.templateRuntime.mu.Unlock()
	}
}

func (s *Service) revisionWarmStatus(v catalog.Revision) RevisionWarmStatus {
	s.templateRuntime.mu.RLock()
	state, exists := s.templateRuntime.warm[v.ID]
	s.templateRuntime.mu.RUnlock()
	if !exists {
		return RevisionWarmStatus{RevisionID: v.ID, Generation: v.Generation, DesiredSlots: effectiveWarmSlots(v), SyncStatus: "pending", Stale: true}
	}
	state.Generation, state.DesiredSlots = v.Generation, effectiveWarmSlots(v)
	if state.ObservedGeneration != v.Generation && state.SyncStatus == "applied" {
		state.SyncStatus = "pending"
		state.Stale = true
	}
	if state.ObservedAt == nil || time.Since(*state.ObservedAt) > templateRuntimeMaxAge {
		state.Stale = true
	}
	if state.Pool != nil {
		copy := *state.Pool
		copy.Stale = copy.Stale || state.Stale || time.Since(copy.ObservedAt) > templateRuntimeMaxAge
		state.Pool = &copy
	}
	return state
}

func (s *Service) registerTemplateRuntime(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/template-revisions/{id}/warm-pool", func(w http.ResponseWriter, r *http.Request) {
		v, _, err := s.catalog.RevisionSnapshot(r.Context(), clientID(r), r.PathValue("id"))
		if err != nil {
			fail(w, catalogAPIError(err))
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, 200, s.revisionWarmStatus(v))
	})
	mux.HandleFunc("GET /v1/templates/{id}/runtime", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if _, err := s.catalog.TemplateSnapshot(r.Context(), clientID(r), id); err != nil {
			fail(w, catalogAPIError(err))
			return
		}
		records, err := s.catalog.RuntimeRevisions(r.Context())
		if err != nil {
			fail(w, err)
			return
		}
		out := TemplateRuntimeStatus{TemplateID: id, Revisions: []RevisionRuntimeStatus{}}
		for _, record := range records {
			v := record.Revision
			if record.ClientID != clientID(r) || v.TemplateID != id {
				continue
			}
			s.templateRuntime.mu.RLock()
			state, exists := s.templateRuntime.usage[v.ID]
			s.templateRuntime.mu.RUnlock()
			state.RevisionID = v.ID
			if !exists || state.ObservedAt == nil || time.Since(*state.ObservedAt) > templateRuntimeMaxAge {
				state.Stale = true
			}
			state.WarmPool = s.revisionWarmStatus(v)
			out.Revisions = append(out.Revisions, state)
		}
		sort.Slice(out.Revisions, func(i, j int) bool { return out.Revisions[i].RevisionID < out.Revisions[j].RevisionID })
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, 200, out)
	})
}
