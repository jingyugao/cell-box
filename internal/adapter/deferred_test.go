package adapter

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jingyugao/cell-box/internal/homevolume"
	"github.com/jingyugao/cell-box/internal/node"
)

// The whole deferred restore: a slot built from one Box waits, another Box
// claims it, and the wrapper must hand runsc a container that carries the
// claimant's command line, because that is what its checkpoint recorded.
func TestDeferredRestoreRunsWithTheClaimingBoxsArguments(t *testing.T) {
	base := t.TempDir()
	const volumeUID = "volume-stable"
	home, err := homevolume.Prepare(base, volumeUID, "node-a", homevolume.VolumeSpecHash, true)
	if err != nil {
		t.Fatal(err)
	}
	mounts := []node.VolumeMountAuthorization{{UID: volumeUID, MountPath: "/data", HomeID: home.ID}}
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
	if err = node.Seal(snapshot, node.Manifest{OwnerUID: "owner", SpecHash: "spec", RunscHash: hash, Mounts: mounts}); err != nil {
		t.Fatal(err)
	}
	if _, err = node.VerifyCached(snapshot, "owner", "spec", runsc); err != nil {
		t.Fatal(err)
	}

	sid, id := strings.Repeat("a", 64), strings.Repeat("b", 64)
	built := guestArgv(t, "guest-built-from", nil)
	claimant := guestArgv(t, "guest-claimant", nil)
	bundle := writeBundle(t, built)

	claimed := node.Ticket{OwnerUID: "owner", NodeName: "node-a", PodUID: "new-pod", PodName: "new-pod", Namespace: "test", Snapshot: "snapshot", SpecHash: "spec", Mounts: mounts, GuestArgv: claimant}
	if err = node.AtomicJSON(filepath.Join(base, "tickets", "new-pod.json"), claimed); err != nil {
		t.Fatal(err)
	}
	lease := node.WarmLease{UID: "new-pod", Name: "new-pod", Namespace: "test", SpecHash: "spec", SID: sid, Phase: "created"}
	leasePath := filepath.Join(base, "warm", "new-pod.json")
	if err = node.AtomicJSON(leasePath, lease); err != nil {
		t.Fatal(err)
	}
	waiting := node.Ticket{PodUID: "new-pod", PodName: "new-pod", Namespace: "test"}
	record := node.ChildRecord{Ticket: waiting, SandboxID: sid, DeferredRestore: true, BundleDir: bundle}
	if err = node.AtomicJSON(filepath.Join(base, "children", id+".json"), record); err != nil {
		t.Fatal(err)
	}

	go func() { // The controller claims the slot while the wrapper waits.
		for i := 0; i < 200; i++ {
			var current node.WarmLease
			if node.ReadJSON(leasePath, &current) == nil && current.Phase == "waiting" {
				current.Phase, current.Ticket = "claimed", claimed
				_ = node.AtomicJSON(leasePath, current)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	a := Adapter{Base: base, Runsc: runsc, DeferCheckpoint: true, NoRootContainer: true}
	got, err := a.childStartArgs([]string{"start", id}, 0, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) < 2 || got[0] != "restore" || got[len(got)-1] != id {
		t.Fatalf("not a restore command: %v", got)
	}
	args, _ := bundleSpec(t, bundle)
	if !reflect.DeepEqual(args, claimant) {
		t.Fatalf("runsc would restore a container with arguments %v, not the claimant's", args)
	}
}
