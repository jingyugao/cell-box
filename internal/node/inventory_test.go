// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/inventory"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type memoryCheckpointStore struct {
	mu      sync.Mutex
	records map[string]inventory.Record
}

func (s *memoryCheckpointStore) PutCheckpoint(_ context.Context, record inventory.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = map[string]inventory.Record{}
	}
	record.Box = append(json.RawMessage(nil), record.Box...)
	s.records[record.RuntimeID] = record
	return nil
}

func (s *memoryCheckpointStore) GetCheckpoint(_ context.Context, runtimeID string) (inventory.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[runtimeID]
	if !ok {
		return inventory.Record{}, inventory.ErrNotFound
	}
	record.Box = append(json.RawMessage(nil), record.Box...)
	return record, nil
}

func (s *memoryCheckpointStore) DeleteCheckpoint(_ context.Context, runtimeID, snapshot string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[runtimeID]
	if !ok {
		return inventory.ErrNotFound
	}
	if snapshot != "" && record.Snapshot != snapshot {
		return fmt.Errorf("checkpoint snapshot mismatch: current %q, requested %q", record.Snapshot, snapshot)
	}
	delete(s.records, runtimeID)
	return nil
}

func (s *memoryCheckpointStore) ListCheckpoints(_ context.Context, clientID string) ([]inventory.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []inventory.Record
	for _, record := range s.records {
		if record.ClientID == clientID {
			record.Box = append(json.RawMessage(nil), record.Box...)
			out = append(out, record)
		}
	}
	return out, nil
}

func suspendedInventoryWorkload(owner, snapshot string) *api.ResumablePod {
	annotation, _ := json.Marshal(inventory.Record{
		ClientID: "client-one",
		Box:      json.RawMessage(`{"id":"box-one","phase":"running"}`),
	})
	return &api.ResumablePod{
		ObjectMeta: meta.ObjectMeta{
			UID:         typesUID(owner),
			Labels:      map[string]string{"cellbox.jingyugao.github.io/box-id": "box-one", inventory.ClientLabel: inventory.ClientValue("client-one")},
			Annotations: map[string]string{inventory.Annotation: string(annotation)},
		},
		Spec:   api.Spec{DesiredState: "Suspended"},
		Status: api.Status{Phase: "Suspended", Snapshot: snapshot, SpecHash: "spec"},
	}
}

func TestSuspendedInventoryUsesVerifiedLocalSnapshotWithoutRemoteTar(t *testing.T) {
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	const owner, snapshot = "owner-local", "checkpoint-local"
	if err := os.MkdirAll(filepath.Dir(runsc), 0700); err != nil {
		t.Fatal(err)
	}
	path := testCheckpoint(t, base, owner, snapshot, "spec", "image-id", runsc)
	checkpoints := &memoryCheckpointStore{}
	backend := &Backend{Base: base, Runsc: runsc, Checkpoints: checkpoints}
	if err := backend.SyncInventory(context.Background(), suspendedInventoryWorkload(owner, snapshot)); err != nil {
		t.Fatalf("publish inventory from valid local checkpoint: %v", err)
	}
	published, err := checkpoints.GetCheckpoint(context.Background(), owner)
	if err != nil || published.Snapshot != snapshot || published.RuntimeID != owner {
		t.Fatalf("suspended inventory was not published: record=%+v err=%v", published, err)
	}
	if _, err := VerifyPrepared(path, owner, "spec", runsc); err != nil {
		t.Fatalf("local checkpoint was not cached as verified: %v", err)
	}
}

func TestSuspendedInventoryRejectsMissingOrCorruptLocalSnapshot(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(t *testing.T, base, runsc string)
	}{
		{name: "missing"},
		{
			name: "corrupt",
			mutate: func(t *testing.T, base, runsc string) {
				path := testCheckpoint(t, base, "owner-local", "checkpoint-local", "spec", "image-id", runsc)
				if err := os.WriteFile(filepath.Join(path, "checkpoint.img"), []byte("corrupt"), 0600); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := t.TempDir()
			runsc := filepath.Join(base, "runsc")
			if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
				t.Fatal(err)
			}
			if test.mutate != nil {
				test.mutate(t, base, runsc)
			}
			checkpoints := &memoryCheckpointStore{}
			backend := &Backend{Base: base, Runsc: runsc, Checkpoints: checkpoints}
			err := backend.SyncInventory(context.Background(), suspendedInventoryWorkload("owner-local", "checkpoint-local"))
			if err == nil {
				t.Fatal("published suspended inventory without a valid local checkpoint")
			}
			if len(checkpoints.records) != 0 {
				t.Fatalf("invalid local checkpoint produced inventory: %v", checkpoints.records)
			}
		})
	}
}

func TestDeleteCheckpointRequiresExactSnapshot(t *testing.T) {
	store := &memoryCheckpointStore{}
	ctx := context.Background()
	if err := store.PutCheckpoint(ctx, inventory.Record{RuntimeID: "owner", Snapshot: "new"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteCheckpoint(ctx, "owner", "old"); err == nil {
		t.Fatal("stale deletion removed a newer checkpoint")
	}
	if current, err := store.GetCheckpoint(ctx, "owner"); err != nil || current.Snapshot != "new" {
		t.Fatalf("new checkpoint changed: record=%+v err=%v", current, err)
	}
	if err := store.DeleteCheckpoint(ctx, "owner", "new"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetCheckpoint(ctx, "owner"); !errors.Is(err, inventory.ErrNotFound) {
		t.Fatalf("checkpoint remains after matching deletion: %v", err)
	}
}
