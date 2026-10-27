// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	core "k8s.io/api/core/v1"
)

func checkpointed(node, image, snapshot, phase, home string) api.ResumablePod {
	return api.ResumablePod{
		Spec:   api.Spec{NodeName: node, HomeVolumeUID: home, Container: core.Container{Name: "cellbox", Image: image}},
		Status: api.Status{Phase: phase, Snapshot: snapshot},
	}
}

func TestCheckpointedProfilesDeriveSlotsFromCheckpoints(t *testing.T) {
	boxes := []api.ResumablePod{
		checkpointed("n1", "img-a", "s1", "Suspended", "home-1"),
		checkpointed("n1", "img-a", "s2", "Suspended", "home-2"), // HOME differs, same warm profile
		checkpointed("n1", "img-a", "s3", "Suspended", "home-3"),
		checkpointed("n1", "img-b", "s4", "Suspended", "home-4"),
		checkpointed("n1", "img-c", "", "Suspended", "home-5"),   // no checkpoint
		checkpointed("n1", "img-d", "s5", "Running", "home-6"),   // restored; checkpoint is gone
		checkpointed("n2", "img-e", "s6", "Suspended", "home-7"), // other node
	}
	got := checkpointedProfiles(boxes, "n1")
	if len(got) != 2 {
		t.Fatalf("profiles = %d, want 2 (img-a, img-b): %+v", len(got), got)
	}
	a := got[warmFingerprint(&boxes[0])]
	b := got[warmFingerprint(&boxes[3])]
	if a == nil || a.checkpoints != 3 || a.slots != 2 {
		t.Fatalf("img-a demand = %+v, want 3 checkpoints / 2 slots", a)
	}
	if b == nil || b.checkpoints != 1 || b.slots != 1 {
		t.Fatalf("img-b demand = %+v, want 1 checkpoint / 1 slot", b)
	}
}

func TestRetainDemandKeepsProfileUntilGracePeriodEnds(t *testing.T) {
	box := checkpointed("n1", "img-a", "s1", "Suspended", "")
	hash := warmFingerprint(&box)
	pool := &WarmPool{}
	start := time.Now()

	if got := pool.retainDemand(checkpointedProfiles([]api.ResumablePod{box}, "n1"), start); got[hash] == nil {
		t.Fatal("live checkpoint produced no demand")
	}
	// The checkpoint is consumed or deleted: one slot stays for the grace period.
	if got := pool.retainDemand(nil, start.Add(warmRetainAfterLastCheckpoint-time.Second)); got[hash] == nil || got[hash].slots != 1 {
		t.Fatalf("demand dropped before the grace period ended: %+v", got)
	}
	if got := pool.retainDemand(nil, start.Add(warmRetainAfterLastCheckpoint)); len(got) != 0 || len(pool.demandSnapshot()) != 0 {
		t.Fatalf("demand outlived the grace period: %+v", got)
	}
}
