package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotCleanupSurvivesRestartAndPreservesNextCheckpoint(t *testing.T) {
	ctx := context.Background()
	objects := &memoryObjects{objects: map[string][]byte{"checkpoints/owner/checkpoint-1.tar": []byte("old"), "checkpoints/owner/metadata.json": []byte(`{"runtimeId":"owner","snapshot":"checkpoint-1"}`)}}
	base := t.TempDir()
	b := &Backend{Base: base, Objects: objects}
	w := testWorkload("owner", "checkpoint-1", "spec", "image")
	old, _ := SnapshotPath(base, "owner", "checkpoint-1")
	if err := os.MkdirAll(old, 0700); err != nil {
		t.Fatal(err)
	}
	if err := b.InvalidateSnapshot(ctx, w); err != nil {
		t.Fatal(err)
	}
	if _, ok := objects.objects["checkpoints/owner/metadata.json"]; ok {
		t.Fatal("consumed snapshot still published")
	}
	if _, ok := objects.objects["checkpoints/owner/checkpoint-1.tar"]; !ok {
		t.Fatal("invalidation waited for physical cleanup")
	}
	objects.objects["checkpoints/owner/checkpoint-2.tar"] = []byte("new")
	objects.objects["checkpoints/owner/metadata.json"] = []byte("new-index")
	next, _ := SnapshotPath(base, "owner", "checkpoint-2")
	if err := os.MkdirAll(next, 0700); err != nil {
		t.Fatal(err)
	}
	b = &Backend{Base: base, Objects: objects}
	if err := b.CollectGarbage(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := objects.objects["checkpoints/owner/checkpoint-1.tar"]; ok {
		t.Fatal("old snapshot not cleaned")
	}
	if err := b.InvalidateSnapshot(ctx, w); err != nil {
		t.Fatal(err)
	}
	if string(objects.objects["checkpoints/owner/metadata.json"]) != "new-index" || string(objects.objects["checkpoints/owner/checkpoint-2.tar"]) != "new" {
		t.Fatal("old cleanup removed new checkpoint")
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
	// An interrupted invalidation has not revoked remote replay eligibility;
	// its intent must not be collected until invalidation is retried.
	if err := AtomicJSON(filepath.Join(base, "garbage", "owner", "checkpoint-3.intent"), snapshotGarbage{Owner: "owner", Snapshot: "checkpoint-3"}); err != nil {
		t.Fatal(err)
	}
	objects.objects["checkpoints/owner/checkpoint-3.tar"] = []byte("pending")
	if err := b.CollectGarbage(ctx); err != nil {
		t.Fatal(err)
	}
	if string(objects.objects["checkpoints/owner/checkpoint-3.tar"]) != "pending" {
		t.Fatal("uncommitted invalidation was collected")
	}
}
