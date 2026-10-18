// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"errors"
	"fmt"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/inventory"
)

// SyncInventory maintains the API's lightweight checkpoint lifecycle index.
// The annotation is attached only to resources created by the CoCell provider;
// unmanaged and local-only installations remain unaffected.
func (b *Backend) SyncInventory(ctx context.Context, w *api.ResumablePod) error {
	if b.Checkpoints == nil || w == nil || w.Annotations[inventory.Annotation] == "" {
		return nil
	}
	owner := string(w.UID)
	if !ValidID(owner) {
		return errors.New("invalid inventory owner UID")
	}
	phase := w.Status.Phase
	// The CR and the API operation already record an in-progress resume. Keep
	// the durable checkpoint descriptor unchanged until exact invalidation.
	if phase == "Restoring" || (phase == "Suspended" && w.Spec.DesiredState == "Running") {
		return nil
	}
	// Runtime observations need no checkpoint index. A consumed snapshot must
	// never be republished after an interrupted status transition.
	if (phase == "Running" && w.Status.Snapshot == "") || (w.Status.Snapshot != "" && b.snapshotConsumed(owner, w.Status.Snapshot)) {
		return nil
	}
	wantPublished := (w.DeletionTimestamp == nil || phase == "Deleting") && w.Status.Snapshot != "" &&
		(phase == "Checkpointing" || phase == "Suspending" || phase == "Suspended" ||
			phase == "Failing" || phase == "Failed" || phase == "Deleting")
	if phase == "Suspended" && w.Status.PodUID != "" {
		wantPublished = false
	}

	if !wantPublished {
		// Ordinary creation/running has no index to remove. For terminal states
		// without a snapshot, remove any remaining record for this runtime.
		if w.Status.Snapshot == "" && phase != "Failed" && phase != "Failing" && phase != "Deleting" {
			return nil
		}
		if err := b.Checkpoints.DeleteCheckpoint(ctx, owner, w.Status.Snapshot); err != nil && !errors.Is(err, inventory.ErrNotFound) {
			return err
		}
		return nil
	}

	if phase == "Suspended" {
		if w.Status.PodUID != "" {
			return errors.New("suspended inventory requires a released Pod")
		}
	}
	publicPhase := phase
	switch phase {
	case "Checkpointing":
		publicPhase = "checkpointing"
	case "Suspending":
		publicPhase = "suspending"
	case "Suspended":
		publicPhase = "suspended"
	case "Failing", "Failed":
		publicPhase = "failed"
	case "Deleting":
		publicPhase = "deleting"
	}
	record, err := inventory.FromResource(w, publicPhase)
	if err != nil {
		return err
	}
	if phase == "Suspended" {
		if err := b.verifyLocalSnapshot(w); err != nil {
			return err
		}
	}
	return b.Checkpoints.PutCheckpoint(ctx, record)
}

func (b *Backend) verifyLocalSnapshot(w *api.ResumablePod) error {
	if b.Base == "" || b.Runsc == "" {
		return errors.New("cannot publish suspended inventory without local checkpoint storage configuration")
	}
	path, err := SnapshotPath(b.Base, string(w.UID), w.Status.Snapshot)
	if err != nil {
		return err
	}
	m, err := VerifyCached(path, string(w.UID), w.Status.SpecHash, b.Runsc)
	if err != nil {
		return fmt.Errorf("cannot publish suspended inventory without a valid local snapshot: %w", err)
	}
	return b.verifySnapshotHome(w, m)
}
