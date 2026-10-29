package adapter

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/homevolume"
	"github.com/jingyugao/cell-box/internal/node"
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
	args, err := a.Rewrite([]string{"--root=/run/test", "start", sid})
	want := []string{"--root=/run/test", "restore", "--background", "--detach", "--image-path=" + snapshot, sid}
	if err != nil || !reflect.DeepEqual(args, want) {
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

func TestIndependentVolumeCanBeUsedByMultipleSandboxTickets(t *testing.T) {
	base := t.TempDir()
	const volumeUID = "volume-stable"
	home, err := homevolume.Prepare(base, volumeUID, "node-a", homevolume.VolumeSpecHash, true)
	if err != nil {
		t.Fatal(err)
	}
	a := Adapter{Base: base, Runsc: filepath.Join(base, "runsc")}
	if err := os.WriteFile(a.Runsc, []byte("runtime"), 0700); err != nil {
		t.Fatal(err)
	}
	want, _ := homevolume.Path(base, volumeUID)
	if err := os.WriteFile(filepath.Join(want, "shared-data"), []byte("shared"), 0600); err != nil {
		t.Fatal(err)
	}
	for index, pod := range []string{"pod-a", "pod-b"} {
		sid := strings.Repeat(string(rune('a'+index)), 64)
		bundle := filepath.Join(base, "bundle-"+pod)
		ticket := node.Ticket{
			OwnerUID: "box-" + pod, NodeName: "node-a", PodUID: pod, PodName: pod,
			Namespace: "test", SpecHash: "runtime-" + pod,
			Mounts: []node.VolumeMountAuthorization{{UID: volumeUID, MountPath: "/data", HomeID: home.ID}},
		}
		if err := node.AtomicJSON(filepath.Join(base, "tickets", pod+".json"), ticket); err != nil {
			t.Fatal(err)
		}
		annotations := map[string]string{
			"io.kubernetes.cri.container-type": "sandbox", "io.kubernetes.cri.sandbox-uid": pod,
			"io.kubernetes.cri.sandbox-name": pod, "io.kubernetes.cri.sandbox-namespace": "test",
			api.TicketAnnotation: pod,
		}
		if err := node.AtomicJSON(filepath.Join(bundle, "config.json"), map[string]any{"annotations": annotations, "mounts": []any{}}); err != nil {
			t.Fatal(err)
		}
		if _, err := a.Rewrite([]string{"create", "--bundle", bundle, sid}); err != nil {
			t.Fatalf("sandbox %s create rejected a shared volume: %v", pod, err)
		}
		if _, err := a.Rewrite([]string{"start", sid}); err != nil {
			t.Fatalf("sandbox %s start rejected a shared volume: %v", pod, err)
		}
		childBundle := filepath.Join(base, "child-"+pod)
		if err := node.AtomicJSON(filepath.Join(childBundle, "config.json"), map[string]any{"mounts": []any{}}); err != nil {
			t.Fatal(err)
		}
		childAnnotations := map[string]string{
			"io.kubernetes.cri.sandbox-id": sid, "io.kubernetes.cri.sandbox-uid": pod,
			"io.kubernetes.cri.sandbox-name": pod, "io.kubernetes.cri.sandbox-namespace": "test",
		}
		childID := strings.Repeat(string(rune('c'+index)), 64)
		if err := a.childCreate(childID, childAnnotations, childBundle); err != nil {
			t.Fatalf("sandbox %s child Gofer rejected a shared volume: %v", pod, err)
		}
		if err := a.childStart(childID); err != nil {
			t.Fatalf("sandbox %s child Gofer start rejected a shared volume: %v", pod, err)
		}
		if index == 0 {
			if err := homevolume.SyncVolume(base, volumeUID, "node-a", home.ID); err != nil {
				t.Fatalf("sandbox %s checkpoint sync failed: %v", pod, err)
			}
		}
	}
	if data, err := os.ReadFile(filepath.Join(want, "shared-data")); err != nil || string(data) != "shared" {
		t.Fatalf("shared volume data changed: %q %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(want), ".state.json")); !os.IsNotExist(err) {
		t.Fatalf("independent volume unexpectedly acquired sandbox execution state: %v", err)
	}
}

func TestSharedVolumeSnapshotsAreConsumedPerSandbox(t *testing.T) {
	base := t.TempDir()
	const volumeUID = "volume-shared"
	home, err := homevolume.Prepare(base, volumeUID, "node-a", homevolume.VolumeSpecHash, true)
	if err != nil {
		t.Fatal(err)
	}
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runtime"), 0700); err != nil {
		t.Fatal(err)
	}
	runscHash, err := node.Digest(runsc)
	if err != nil {
		t.Fatal(err)
	}
	a := Adapter{Base: base, Runsc: runsc}
	start := func(owner, pod, snapshot string, n byte) error {
		specHash := "spec-" + owner
		snapshotDir, err := node.SnapshotPath(base, owner, snapshot)
		if err != nil {
			return err
		}
		mounts := []node.VolumeMountAuthorization{{UID: volumeUID, MountPath: "/data", HomeID: home.ID}}
		if _, err := os.Stat(snapshotDir); os.IsNotExist(err) {
			if err := os.MkdirAll(snapshotDir, 0700); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(snapshotDir, "checkpoint.img"), []byte("state-"+owner), 0600); err != nil {
				return err
			}
			if err := node.Seal(snapshotDir, node.Manifest{OwnerUID: owner, SpecHash: specHash, RunscHash: runscHash, Mounts: mounts}); err != nil {
				return err
			}
			if _, err := node.VerifyCached(snapshotDir, owner, specHash, a.Runsc); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		ticket := node.Ticket{OwnerUID: owner, NodeName: "node-a", PodUID: pod, PodName: pod, Namespace: "test", Snapshot: snapshot, SpecHash: specHash, Mounts: mounts}
		if err := node.AtomicJSON(filepath.Join(base, "tickets", pod+".json"), ticket); err != nil {
			return err
		}
		sid := strings.Repeat(string(n), 64)
		bundle := filepath.Join(base, "bundle-"+pod)
		annotations := map[string]string{
			"io.kubernetes.cri.container-type": "sandbox", "io.kubernetes.cri.sandbox-uid": pod,
			"io.kubernetes.cri.sandbox-name": pod, "io.kubernetes.cri.sandbox-namespace": "test",
			api.TicketAnnotation: pod,
		}
		if err := node.AtomicJSON(filepath.Join(bundle, "config.json"), map[string]any{"annotations": annotations}); err != nil {
			return err
		}
		if _, err := a.Rewrite([]string{"create", "--bundle", bundle, sid}); err != nil {
			return fmt.Errorf("create sandbox: %w", err)
		}
		if _, err := a.Rewrite([]string{"start", sid}); err != nil {
			return fmt.Errorf("start sandbox: %w", err)
		}
		return nil
	}
	if err := start("box-a", "pod-a", "snapshot-a", 'a'); err != nil {
		t.Fatalf("Box A restore failed: %v", err)
	}
	if err := start("box-b", "pod-b", "snapshot-b", 'b'); err != nil {
		t.Fatalf("Box B shared-volume restore was blocked by Box A: %v", err)
	}
	if err := start("box-a", "pod-a2", "snapshot-a", 'c'); err == nil || !strings.Contains(err.Error(), "snapshot already consumed") {
		t.Fatalf("same Box checkpoint replay was not fenced: %v", err)
	}
}
