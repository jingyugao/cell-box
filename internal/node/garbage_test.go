package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"cellbox.local/cellbox/internal/inventory"
)

func TestSnapshotCleanupSurvivesRestartAndPreservesNextCheckpoint(t *testing.T) {
	ctx := context.Background()
	checkpoints := &memoryCheckpointStore{records: map[string]inventory.Record{"owner": {RuntimeID: "owner", Snapshot: "checkpoint-1"}}}
	base := t.TempDir()
	b := &Backend{Base: base, Checkpoints: checkpoints}
	w := testWorkload("owner", "checkpoint-1", "spec", "image")
	old, _ := SnapshotPath(base, "owner", "checkpoint-1")
	if err := os.MkdirAll(old, 0700); err != nil {
		t.Fatal(err)
	}
	if err := b.InvalidateSnapshot(ctx, w); err != nil {
		t.Fatal(err)
	}
	if _, err := checkpoints.GetCheckpoint(ctx, "owner"); !errors.Is(err, inventory.ErrNotFound) {
		t.Fatalf("consumed snapshot still published: %v", err)
	}
	if err := checkpoints.PutCheckpoint(ctx, inventory.Record{RuntimeID: "owner", Snapshot: "checkpoint-2"}); err != nil {
		t.Fatal(err)
	}
	next, _ := SnapshotPath(base, "owner", "checkpoint-2")
	if err := os.MkdirAll(next, 0700); err != nil {
		t.Fatal(err)
	}
	b = &Backend{Base: base, Checkpoints: checkpoints}
	if err := b.CollectGarbage(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.InvalidateSnapshot(ctx, w); err != nil {
		t.Fatal(err)
	}
	if record, err := checkpoints.GetCheckpoint(ctx, "owner"); err != nil || record.Snapshot != "checkpoint-2" {
		t.Fatalf("old cleanup removed new checkpoint: record=%+v err=%v", record, err)
	}
	if _, err := os.Stat(next); err != nil {
		t.Fatal("new cache removed", err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("old cache remains")
	}
	if !b.snapshotConsumed("owner", "checkpoint-1") {
		t.Fatal("cleanup lost replay fence")
	}
	if err := b.prepareSnapshot(ctx, w); err == nil {
		t.Fatal("consumed snapshot accepted for another execution")
	}
	// An interrupted invalidation must not be collected until the DB deletion is retried.
	if err := AtomicJSON(filepath.Join(base, "garbage", "owner", "checkpoint-3.intent"), snapshotGarbage{Owner: "owner", Snapshot: "checkpoint-3"}); err != nil {
		t.Fatal(err)
	}
	pending, _ := SnapshotPath(base, "owner", "checkpoint-3")
	if err := os.MkdirAll(pending, 0700); err != nil {
		t.Fatal(err)
	}
	if err := b.CollectGarbage(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(pending); err != nil {
		t.Fatal("uncommitted invalidation was collected")
	}
	if err := checkpoints.PutCheckpoint(ctx, inventory.Record{RuntimeID: "owner", Snapshot: "checkpoint-3"}); err != nil {
		t.Fatal(err)
	}
	if err := b.InvalidateSnapshot(ctx, testWorkload("owner", "checkpoint-3", "spec", "image")); err != nil {
		t.Fatal("restart did not finish checkpoint deletion:", err)
	}
	if _, err := checkpoints.GetCheckpoint(ctx, "owner"); !errors.Is(err, inventory.ErrNotFound) {
		t.Fatalf("checkpoint index remains after resumed invalidation: %v", err)
	}
}
