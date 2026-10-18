// SPDX-License-Identifier: Apache-2.0

package warmtemplates

import (
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
)

type Snapshot struct {
	NodeName    string                            `json:"nodeName"`
	DesiredHash string                            `json:"desiredHash"`
	ObservedAt  time.Time                         `json:"observedAt"`
	Pools       []boxprovider.WarmPoolObservation `json:"pools"`
}

func CloneSnapshot(snapshot Snapshot) Snapshot {
	out := snapshot
	out.Pools = append([]boxprovider.WarmPoolObservation(nil), snapshot.Pools...)
	for i := range out.Pools {
		out.Pools[i].TemplateIDs = append([]string(nil), snapshot.Pools[i].TemplateIDs...)
	}
	return out
}
