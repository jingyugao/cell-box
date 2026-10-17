// SPDX-License-Identifier: Apache-2.0

package warmtemplates

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
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
