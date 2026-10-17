// SPDX-License-Identifier: Apache-2.0

package warmtemplates

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const StatusConfigMapPrefix = "cellbox-warm-status-"

type Snapshot struct {
	NodeName           string                            `json:"nodeName"`
	DesiredHash        string                            `json:"desiredHash"`
	ObservedGeneration int64                             `json:"observedGeneration"`
	ObservedAt         time.Time                         `json:"observedAt"`
	Pools              []boxprovider.WarmPoolObservation `json:"pools"`
}

// DesiredHash is stable across map iteration and covers the complete desired
// warm-template payload published to the controller.
func DesiredHash(data map[string]string) string {
	encoded, _ := json.Marshal(data)
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:])
}

// StatusConfigMapName gives each node in the controller DaemonSet an
// independent status object while keeping the Kubernetes name bounded.
func StatusConfigMapName(node string) string {
	hash := sha256.Sum256([]byte(node))
	return StatusConfigMapPrefix + hex.EncodeToString(hash[:8])
}

func EncodeSnapshot(snapshot Snapshot) (map[string]string, error) {
	if strings.TrimSpace(snapshot.NodeName) == "" || snapshot.DesiredHash == "" {
		return nil, fmt.Errorf("warm pool snapshot identity is required")
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}
	return map[string]string{"snapshot.json": string(encoded)}, nil
}

// LocalStore keeps desired warm templates and per-node observations in the
// server process. It is safe for concurrent API and controller goroutines.
// An absent namespace is deliberately distinct from an initialized empty set.
type LocalStore struct {
	mu         sync.RWMutex
	configured map[string]localConfig
	snapshots  map[string]map[string]Snapshot
	changes    map[string]chan struct{}
}

type localConfig struct {
	templates  []Template
	hash       string
	generation int64
}

func NewLocalStore() *LocalStore {
	return &LocalStore{
		configured: map[string]localConfig{},
		snapshots:  map[string]map[string]Snapshot{},
		changes:    map[string]chan struct{}{},
	}
}

// Set atomically replaces the complete configured set. Equal content keeps
// the generation; changed content invalidates prior observations and wakes the
// local controller immediately.
func (s *LocalStore) Set(namespace string, templates []Template) error {
	if s == nil || len(validation.IsDNS1123Label(namespace)) > 0 {
		return errors.New("valid namespace and local warm-template store are required")
	}
	copyTemplates := make([]Template, 0, len(templates))
	data := make(map[string]string, len(templates))
	seen := make(map[string]bool, len(templates))
	for _, template := range templates {
		if err := Validate(namespace, template.ID, template); err != nil {
			return err
		}
		if seen[template.ID] {
			return fmt.Errorf("duplicate warm template ID %q", template.ID)
		}
		seen[template.ID] = true
		encoded, err := json.Marshal(template)
		if err != nil {
			return err
		}
		data[template.ID] = string(encoded)
		copyTemplates = append(copyTemplates, deepCopyTemplate(template))
	}
	hash := DesiredHash(data)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.configured == nil {
		s.configured = map[string]localConfig{}
	}
	if s.snapshots == nil {
		s.snapshots = map[string]map[string]Snapshot{}
	}
	if s.changes == nil {
		s.changes = map[string]chan struct{}{}
	}
	old, exists := s.configured[namespace]
	if exists && old.hash == hash {
		return nil
	}
	generation := int64(1)
	if exists {
		generation = old.generation + 1
	}
	s.configured[namespace] = localConfig{templates: copyTemplates, hash: hash, generation: generation}
	delete(s.snapshots, namespace)
	ch := s.changeChannelLocked(namespace)
	select {
	case ch <- struct{}{}:
	default:
	}
	return nil
}

// Read returns a defensive copy. initialized=false means no full catalog sync
// has happened yet and must never be interpreted as an empty desired set.
func (s *LocalStore) Read(namespace string) (templates []Template, hash string, generation int64, initialized bool) {
	if s == nil {
		return nil, "", 0, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	config, ok := s.configured[namespace]
	if !ok {
		return nil, "", 0, false
	}
	out := make([]Template, len(config.templates))
	for i := range config.templates {
		out[i] = deepCopyTemplate(config.templates[i])
	}
	return out, config.hash, config.generation, true
}

func (s *LocalStore) Changes(namespace string) <-chan struct{} {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changeChannelLocked(namespace)
}

func (s *LocalStore) changeChannelLocked(namespace string) chan struct{} {
	if s.changes == nil {
		s.changes = map[string]chan struct{}{}
	}
	ch := s.changes[namespace]
	if ch == nil {
		ch = make(chan struct{}, 1)
		s.changes[namespace] = ch
	}
	return ch
}

func (s *LocalStore) Publish(namespace string, snapshot Snapshot) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	config, ok := s.configured[namespace]
	if !ok || snapshot.DesiredHash != config.hash || snapshot.ObservedGeneration != config.generation || snapshot.NodeName == "" {
		return
	}
	if s.snapshots == nil {
		s.snapshots = map[string]map[string]Snapshot{}
	}
	if s.snapshots[namespace] == nil {
		s.snapshots[namespace] = map[string]Snapshot{}
	}
	s.snapshots[namespace][snapshot.NodeName] = cloneSnapshot(snapshot)
}

func (s *LocalStore) Snapshot(namespace, node string) (Snapshot, bool) {
	if s == nil {
		return Snapshot{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot, ok := s.snapshots[namespace][node]
	if !ok {
		return Snapshot{}, false
	}
	return cloneSnapshot(snapshot), true
}

func deepCopyTemplate(template Template) Template {
	copy := template
	copy.Spec.Container = *template.Spec.Container.DeepCopy()
	if template.Spec.Upgrade != nil {
		upgrade := *template.Spec.Upgrade
		upgrade.PreviousContainer = *template.Spec.Upgrade.PreviousContainer.DeepCopy()
		copy.Spec.Upgrade = &upgrade
	}
	if template.Spec.ServicePorts != nil {
		copy.Spec.ServicePorts = make([]core.ServicePort, len(template.Spec.ServicePorts))
		for i := range template.Spec.ServicePorts {
			template.Spec.ServicePorts[i].DeepCopyInto(&copy.Spec.ServicePorts[i])
		}
	}
	return copy
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	snapshot.Pools = append([]boxprovider.WarmPoolObservation(nil), snapshot.Pools...)
	for i := range snapshot.Pools {
		snapshot.Pools[i].TemplateIDs = append([]string(nil), snapshot.Pools[i].TemplateIDs...)
	}
	return snapshot
}
