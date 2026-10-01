// SPDX-License-Identifier: Apache-2.0

package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/inventory"
	"cellbox.local/cellbox/internal/objectstorage"
)

const maxInventoryRecordBytes = 1 << 20

// SyncInventory maintains the API's lightweight checkpoint lifecycle index.
// The annotation is attached only to resources created by the CoCell provider;
// unmanaged and local-only installations remain unaffected.
func (b *Backend) SyncInventory(ctx context.Context, w *api.ResumablePod) error {
	if b.Objects == nil || w == nil || w.Annotations[inventory.Annotation] == "" {
		return nil
	}
	owner := string(w.UID)
	if !ValidID(owner) {
		return errors.New("invalid inventory owner UID")
	}
	key := "checkpoints/" + owner + "/metadata.json"
	phase := w.Status.Phase
	// Running invalidates replay eligibility, but the index and blob are removed
	// together by Forget on the next Running reconcile. Keep the last checkpoint
	// record through this boundary instead of exposing an index-less race.
	if phase == "Running" && w.Status.Snapshot == "" {
		return nil
	}
	wantPublished := (w.DeletionTimestamp == nil || phase == "Deleting") && w.Status.Snapshot != "" &&
		(phase == "Checkpointing" || phase == "Suspending" || phase == "Suspended" ||
			phase == "Restoring" || phase == "Failing" || phase == "Failed" || phase == "Deleting")
	if phase == "Suspended" && w.Status.PodUID != "" {
		wantPublished = false
	}

	b.inventoryMu.Lock()
	defer b.inventoryMu.Unlock()
	if b.inventoryState == nil {
		b.inventoryState = map[string]string{}
	}
	if !wantPublished {
		state, cached := b.inventoryState[owner]
		if cached && state == "" {
			return nil
		}
		// Creating and ordinary running resources have no checkpoint index to
		// clean up. In particular, initial creation must not depend on OSS.
		if !cached && w.Status.Snapshot == "" && phase != "Failed" && phase != "Failing" && phase != "Deleting" {
			return nil
		}
		if err := b.Objects.Delete(ctx, key); err != nil {
			if errors.Is(err, objectstorage.ErrNotFound) {
				b.inventoryState[owner] = ""
				return nil
			}
			return retryableStorage(err)
		}
		b.inventoryState[owner] = ""
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
		if w.Spec.DesiredState == "Running" {
			publicPhase = "resuming"
		}
	case "Restoring":
		publicPhase = "resuming"
	case "Failing", "Failed":
		publicPhase = "failed"
	case "Deleting":
		publicPhase = "deleting"
	}
	record, err := inventory.FromResource(w, publicPhase)
	if err != nil {
		return err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if state, ok := b.inventoryState[owner]; ok && state == string(data) {
		return nil
	}
	if phase == "Suspended" {
		if err := b.verifyDurableSnapshot(ctx, owner, w.Status.Snapshot); err != nil {
			return err
		}
	}

	current, _, err := readInventoryObject(ctx, b.Objects, key)
	if err == nil && bytes.Equal(current, data) {
		b.inventoryState[owner] = string(data)
		return nil
	}
	if err != nil && !errors.Is(err, objectstorage.ErrNotFound) {
		return retryableStorage(err)
	}
	if _, err = b.Objects.Put(ctx, key, bytes.NewReader(data), int64(len(data)), ""); err != nil {
		return retryableStorage(err)
	}
	b.inventoryState[owner] = string(data)
	return nil
}

func (b *Backend) verifyDurableSnapshot(ctx context.Context, owner, snapshot string) error {
	snapshotKey, err := checkpointObjectKey(owner, snapshot)
	if err != nil {
		return err
	}
	body, size, _, err := b.Objects.Get(ctx, snapshotKey)
	if err != nil {
		if errors.Is(err, objectstorage.ErrNotFound) {
			return fmt.Errorf("cannot publish suspended inventory before its snapshot is durable")
		}
		return retryableStorage(err)
	}
	closeErr := body.Close()
	if closeErr != nil {
		return retryableStorage(closeErr)
	}
	if size < 1 {
		return errors.New("cannot publish an empty suspended snapshot")
	}
	return nil
}

func readInventoryObject(ctx context.Context, objects objectstorage.Objects, key string) ([]byte, string, error) {
	body, size, etag, err := objects.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	defer body.Close()
	if size < 1 || size > maxInventoryRecordBytes || etag == "" {
		return nil, "", errors.New("invalid sandbox inventory object")
	}
	data, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return nil, "", err
	}
	if int64(len(data)) != size {
		return nil, "", errors.New("sandbox inventory object size mismatch")
	}
	return data, etag, nil
}

func (b *Backend) clearInventoryState(owner string) {
	b.inventoryMu.Lock()
	delete(b.inventoryState, owner)
	b.inventoryMu.Unlock()
}
