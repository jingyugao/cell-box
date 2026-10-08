package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/homevolume"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPersistentHomeRestoreAndCleanupBoundaries(t *testing.T) {
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runtime"), 0700); err != nil {
		t.Fatal(err)
	}
	b := &Backend{Base: base, Runsc: runsc, Images: imageClient{id: "image"}}
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "box-owner"}, Spec: api.Spec{NodeName: "node", PersistentHome: true, Container: core.Container{Image: "image"}}, Status: api.Status{Cycle: 1, SpecHash: "spec"}}
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod-one", Name: "one", Namespace: "test"}}
	ctx := context.Background()
	if err := b.Prepare(ctx, w, p); err != nil {
		t.Fatal(err)
	}
	var ticket Ticket
	if err := ReadJSON(filepath.Join(base, "tickets", "pod-one.json"), &ticket); err != nil {
		t.Fatal(err)
	}
	if ticket.HomeID == "" {
		t.Fatal("HOME identity missing from runtime authorization")
	}
	if err := homevolume.ClaimExecution(base, "box-owner", ticket.HomeID, "pod-one", ""); err != nil {
		t.Fatal(err)
	}
	home, _ := homevolume.Path(base, "box-owner")
	if err := os.WriteFile(filepath.Join(home, "workspace-data"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := homevolume.Checkpoint(base, "box-owner", ticket.HomeID, "pod-one", "checkpoint-1"); err != nil {
		t.Fatal(err)
	}
	hash, err := Digest(runsc)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := SnapshotPath(base, "box-owner", "checkpoint-1")
	if err := os.MkdirAll(snapshot, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapshot, "checkpoint.img"), []byte("state"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Seal(snapshot, Manifest{OwnerUID: "box-owner", SpecHash: "spec", ImageID: "image", RunscHash: hash, HomeID: ticket.HomeID}); err != nil {
		t.Fatal(err)
	}
	w.Status.Cycle, w.Status.Snapshot = 2, "checkpoint-1"
	p.UID, p.Name = "pod-two", "two"
	if err := b.Prepare(ctx, w, p); err != nil {
		t.Fatal(err)
	}
	if err := homevolume.ClaimExecution(base, "box-owner", ticket.HomeID, "pod-two", "checkpoint-1"); err != nil {
		t.Fatal(err)
	}
	p.UID = "pod-three"
	if err := b.Prepare(ctx, w, p); err == nil {
		t.Fatal("consumed HOME/checkpoint pairing authorized again")
	}
	if _, err := os.Stat(filepath.Join(base, "tickets", "pod-three.json")); !os.IsNotExist(err) {
		t.Fatal("failed restore created a ticket")
	}
	w.Status.Snapshot = ""
	if err := b.Forget(ctx, w); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(home, "workspace-data")); err != nil || string(data) != "keep" {
		t.Fatalf("ordinary cleanup lost HOME: %q %v", data, err)
	}
	if _, err := homevolume.Prepare(base, "other-owner", "node", "spec", true); err != nil {
		t.Fatal(err)
	}
	now := meta.Now()
	w.DeletionTimestamp = &now
	if err := b.Forget(ctx, w); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Fatal("Box deletion did not reclaim its HOME")
	}
	other, _ := homevolume.Path(base, "other-owner")
	if _, err := os.Stat(other); err != nil {
		t.Fatal("Box deletion affected another HOME", err)
	}
}

func TestPersistentHomeRestoreNeverCreatesMissingStorage(t *testing.T) {
	b := &Backend{Base: t.TempDir()}
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "owner"}, Spec: api.Spec{NodeName: "node", PersistentHome: true}, Status: api.Status{Cycle: 2, Snapshot: "checkpoint-1", SpecHash: "spec"}}
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod"}}
	if err := b.Prepare(context.Background(), w, p); err == nil {
		t.Fatal("restore accepted missing HOME")
	}
	home, _ := homevolume.Path(b.Base, "owner")
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Fatal("restore silently recreated HOME")
	}
	if _, err := os.Stat(filepath.Join(b.Base, "tickets", "pod.json")); !os.IsNotExist(err) {
		t.Fatal("restore wrote authorization for missing HOME")
	}
}

func TestFailedBoxRebuildPreservesExistingHomeAndRejectsMissingHome(t *testing.T) {
	base := t.TempDir()
	b := &Backend{Base: base}
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "owner"}, Spec: api.Spec{NodeName: "node", PersistentHome: true, RebuildNonce: "restart"}, Status: api.Status{Cycle: 2, SpecHash: "spec", PodUID: "old-pod", Snapshot: "stale"}}
	home, err := homevolume.Prepare(base, "owner", "node", "spec", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := homevolume.ClaimExecution(base, "owner", home.ID, "old-pod", ""); err != nil {
		t.Fatal(err)
	}
	if err := b.Rebuild(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	if err := homevolume.ClaimExecution(base, "owner", home.ID, "new-pod", ""); err != nil {
		t.Fatal(err)
	}
	w.UID = "missing"
	if err := b.Rebuild(context.Background(), w); err == nil {
		t.Fatal("missing HOME silently replaced")
	}
}
