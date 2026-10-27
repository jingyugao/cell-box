// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
)

// A warm slot only shortens a checkpoint restore, so demand is derived from the
// checkpoints that exist on this node instead of a per-revision setting. When a
// project is archived its checkpoint disappears and the demand follows.
const (
	// One slot covers a handful of checkpoints; the pool answers concurrent
	// restores, not stock.
	warmCheckpointsPerSlot = 3
	warmMaxSlotsPerProfile = 2
	// A restored Box suspends again after idling, so keep one slot briefly to
	// avoid recreating the Pod for every resume/suspend cycle.
	warmRetainAfterLastCheckpoint = 20 * time.Minute
)

type warmDemand struct {
	template    *api.ResumablePod
	checkpoints int
	slots       int
}

type retainedDemand struct {
	template *api.ResumablePod
	seenAt   time.Time
}

func warmSlotsFor(checkpoints int) int {
	if checkpoints <= 0 {
		return 1
	}
	if n := 1 + checkpoints/warmCheckpointsPerSlot; n < warmMaxSlotsPerProfile {
		return n
	}
	return warmMaxSlotsPerProfile
}

// checkpointedProfiles groups suspended Boxes on nodeName by the runtime
// profile a warm Pod must match. Each Box is its own template: it carries the
// exact spec it will resume with.
func checkpointedProfiles(boxes []api.ResumablePod, nodeName string) map[string]*warmDemand {
	out := map[string]*warmDemand{}
	for i := range boxes {
		b := &boxes[i]
		if b.Spec.NodeName != nodeName || b.DeletionTimestamp != nil || b.Status.Phase != "Suspended" || b.Status.Snapshot == "" {
			continue
		}
		hash := warmFingerprint(b)
		d := out[hash]
		if d == nil {
			d = &warmDemand{template: b.DeepCopy()}
			out[hash] = d
		}
		d.checkpoints++
	}
	for _, d := range out {
		d.slots = warmSlotsFor(d.checkpoints)
	}
	return out
}

// retainDemand keeps a profile alive for warmRetainAfterLastCheckpoint after its
// last checkpoint is gone. The state is only a hysteresis timer: after a restart
// it is rebuilt from the live checkpoints.
func (p *WarmPool) retainDemand(live map[string]*warmDemand, now time.Time) map[string]*warmDemand {
	p.demandMu.Lock()
	defer p.demandMu.Unlock()
	if p.retained == nil {
		p.retained = map[string]retainedDemand{}
	}
	out := make(map[string]*warmDemand, len(live))
	for hash, d := range live {
		p.retained[hash] = retainedDemand{template: d.template, seenAt: now}
		out[hash] = d
	}
	for hash, kept := range p.retained {
		if _, ok := live[hash]; ok {
			continue
		}
		if now.Sub(kept.seenAt) >= warmRetainAfterLastCheckpoint {
			delete(p.retained, hash)
			continue
		}
		out[hash] = &warmDemand{template: kept.template, slots: warmSlotsFor(0)}
	}
	p.lastDemand = make(map[string]warmDemand, len(out))
	for hash, d := range out {
		p.lastDemand[hash] = warmDemand{checkpoints: d.checkpoints, slots: d.slots}
	}
	return out
}

func (p *WarmPool) demandSnapshot() map[string]warmDemand {
	p.demandMu.Lock()
	defer p.demandMu.Unlock()
	out := make(map[string]warmDemand, len(p.lastDemand))
	for hash, d := range p.lastDemand {
		out[hash] = d
	}
	return out
}
