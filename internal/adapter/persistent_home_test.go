package adapter

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/node"
)

func TestPersistentHomeIsConsumedBeforeRestoreStarts(t *testing.T) {
	base := t.TempDir()
	home, err := homevolume.Prepare(base, "owner", "node", "spec", true)
	if err != nil {
		t.Fatal(err)
	}
	if err = homevolume.ClaimExecution(base, "owner", home.ID, "old-pod", ""); err != nil {
		t.Fatal(err)
	}
	if err = homevolume.Checkpoint(base, "owner", home.ID, "old-pod", "snapshot"); err != nil {
		t.Fatal(err)
	}
	runsc := filepath.Join(base, "runsc")
	if err = os.WriteFile(runsc, []byte("runtime"), 0700); err != nil {
		t.Fatal(err)
	}
	hash, err := node.Digest(runsc)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := node.SnapshotPath(base, "owner", "snapshot")
	if err = os.MkdirAll(snapshot, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(snapshot, "checkpoint.img"), []byte("state"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = node.Seal(snapshot, node.Manifest{OwnerUID: "owner", SpecHash: "spec", RunscHash: hash, HomeID: home.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err = node.VerifyCached(snapshot, "owner", "spec", runsc); err != nil {
		t.Fatal(err)
	}
	a := Adapter{Base: base, Runsc: runsc}
	create := func(pod, sid string) error {
		bundle := filepath.Join(base, "bundle-"+pod)
		ticket := node.Ticket{OwnerUID: "owner", PodUID: pod, PodName: pod, Namespace: "test", Snapshot: "snapshot", SpecHash: "spec", HomeID: home.ID}
		if err := node.AtomicJSON(filepath.Join(base, "tickets", pod+".json"), ticket); err != nil {
			return err
		}
		annotations := map[string]string{"io.kubernetes.cri.container-type": "sandbox", "io.kubernetes.cri.sandbox-uid": pod, "io.kubernetes.cri.sandbox-name": pod, "io.kubernetes.cri.sandbox-namespace": "test", api.TicketAnnotation: pod}
		if err := node.AtomicJSON(filepath.Join(bundle, "config.json"), map[string]any{"annotations": annotations}); err != nil {
			return err
		}
		_, err := a.Rewrite([]string{"create", "--bundle", bundle, sid})
		return err
	}
	sid := strings.Repeat("a", 64)
	if err = create("new-pod", sid); err != nil {
		t.Fatal(err)
	}
	args, err := a.Rewrite([]string{"start", sid})
	if err != nil || len(args) == 0 || args[0] != "restore" {
		t.Fatalf("restore rewrite: %v %v", args, err)
	}
	if err = homevolume.ValidateCheckpoint(base, "owner", home.ID, "snapshot"); err == nil {
		t.Fatal("snapshot still eligible after authorizing restored code")
	}
	// Simulate controller restart before it invalidates the old remote checkpoint.
	if err = create("another-pod", strings.Repeat("b", 64)); err == nil {
		t.Fatal("old checkpoint replayed against a HOME that may have changed")
	}
}
