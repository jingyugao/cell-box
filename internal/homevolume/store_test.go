package homevolume

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestIndependentVolumeSyncAndRemove(t *testing.T) {
	base := filepath.Join(t.TempDir(), "cellbox")
	const volumeUID = "volume-data"
	identity, err := Prepare(base, volumeUID, "node", VolumeSpecHash, true)
	if err != nil {
		t.Fatal(err)
	}
	_, dir, data, _ := paths(base, volumeUID)
	if _, err := os.Stat(filepath.Join(dir, stateName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("independent volume unexpectedly has execution state: %v", err)
	}
	oldSync := syncFS
	syncCalls := 0
	syncFS = func(int) error { syncCalls++; return nil }
	t.Cleanup(func() { syncFS = oldSync })
	if err := os.WriteFile(filepath.Join(data, "persisted"), []byte("durable volume data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := SyncVolume(base, volumeUID, "node", identity.ID); err != nil {
		t.Fatal(err)
	}
	if syncCalls != 1 {
		t.Fatalf("SyncVolume called syncfs %d times, want 1", syncCalls)
	}
	for _, tc := range []struct{ node, id string }{{"wrong-node", identity.ID}, {"node", "wrong-id"}} {
		if err := SyncVolume(base, volumeUID, tc.node, tc.id); err == nil {
			t.Fatalf("SyncVolume accepted node=%q id=%q", tc.node, tc.id)
		}
	}
	if syncCalls != 1 {
		t.Fatalf("invalid SyncVolume calls reached syncfs: %d", syncCalls-1)
	}
	if err := RemoveVolume(base, volumeUID, "wrong-node"); err == nil {
		t.Fatal("RemoveVolume accepted a mismatched node")
	}
	if err := RemoveVolume(base, volumeUID, "node"); err != nil {
		t.Fatal(err)
	}
	if err := RemoveVolume(base, volumeUID, "node"); err != nil {
		t.Fatalf("repeated RemoveVolume should be idempotent: %v", err)
	}
	if _, err := os.Stat(data); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("RemoveVolume left data behind: %v", err)
	}
}

func TestPromoteLegacyPreservesDataAndIsIdempotent(t *testing.T) {
	base := filepath.Join(t.TempDir(), "cellbox")
	identity, err := Prepare(base, "legacy-box", "node", "legacy-spec", true)
	if err != nil {
		t.Fatal(err)
	}
	oldSync := syncFS
	syncFS = func(int) error { return nil }
	t.Cleanup(func() { syncFS = oldSync })
	if err := ClaimExecution(base, "legacy-box", identity.ID, "old-pod", ""); err != nil {
		t.Fatal(err)
	}
	data, _ := Path(base, "legacy-box")
	if err := os.WriteFile(filepath.Join(data, "user-data"), []byte("preserve me"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Checkpoint(base, "legacy-box", identity.ID, "old-pod", "old-snapshot"); err != nil {
		t.Fatal(err)
	}
	if err := PromoteLegacy(base, "legacy-box", "node", "legacy-spec"); err != nil {
		t.Fatal(err)
	}
	promoted, err := Verify(base, "legacy-box", "node", VolumeSpecHash, identity.ID)
	if err != nil {
		t.Fatalf("promoted identity did not verify: %v", err)
	}
	if promoted.ID != identity.ID || promoted.Device != identity.Device || promoted.Inode != identity.Inode {
		t.Fatalf("promotion changed the HOME ID or inode: before=%#v after=%#v", identity, promoted)
	}
	if got, err := os.ReadFile(filepath.Join(data, "user-data")); err != nil || string(got) != "preserve me" {
		t.Fatalf("legacy data changed: data=%q err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(base, "homes", "legacy-box", stateName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("promotion retained legacy execution state: %v", err)
	}
	if err := PromoteLegacy(base, "legacy-box", "node", "legacy-spec"); err != nil {
		t.Fatalf("promotion retry failed: %v", err)
	}
}

func TestPromoteLegacyInitializesMissingHomeAndRejectsUnknownData(t *testing.T) {
	base := filepath.Join(t.TempDir(), "cellbox")
	if err := PromoteLegacy(base, "missing-box", "node", "legacy-spec"); err != nil {
		t.Fatalf("missing legacy HOME did not create an empty volume: %v", err)
	}
	if _, err := Verify(base, "missing-box", "node", VolumeSpecHash, ""); err != nil {
		t.Fatalf("initialized volume did not verify: %v", err)
	}
	if err := PromoteLegacy(base, "missing-empty-spec", "node", ""); err != nil {
		t.Fatalf("missing legacy HOME with empty legacy spec did not create an empty volume: %v", err)
	}
	if _, err := Verify(base, "missing-empty-spec", "node", VolumeSpecHash, ""); err != nil {
		t.Fatalf("empty-spec initialized volume did not verify: %v", err)
	}
	if _, err := Prepare(base, "legacy-empty-spec", "node", "legacy-spec", true); err != nil {
		t.Fatal(err)
	}
	if err := PromoteLegacy(base, "legacy-empty-spec", "node", ""); err == nil {
		t.Fatal("promotion accepted an empty spec for an existing legacy identity")
	}
	if err := os.MkdirAll(filepath.Join(base, "homes", "unknown-box", "data"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "homes", "unknown-box", "data", "unknown"), []byte("do not adopt"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := PromoteLegacy(base, "unknown-box", "node", "legacy-spec"); err == nil {
		t.Fatal("promotion adopted a non-empty unknown HOME directory")
	}
	if err := PromoteLegacy(base, "unknown-box", "node", ""); err == nil {
		t.Fatal("empty-spec promotion adopted a non-empty unknown HOME directory")
	}
}

func TestPrepareVerifyAndOwnerIsolation(t *testing.T) {
	base := filepath.Join(t.TempDir(), "cellbox")
	if _, err := Path("relative", "owner-a"); err == nil {
		t.Fatal("Path accepted a relative base")
	}
	for _, field := range []struct{ node, spec string }{{"", "spec"}, {"node", ""}} {
		if _, err := Prepare(base, "invalid-init", field.node, field.spec, true); err == nil {
			t.Fatalf("Prepare initialized with empty node/spec: %#v", field)
		}
	}
	if _, err := os.Stat(base); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty node/spec created storage before rejection: %v", err)
	}
	one, err := Prepare(base, "owner-a", "node-a", "spec-a", true)
	if err != nil {
		t.Fatal(err)
	}
	two, err := Prepare(base, "owner-b", "node-a", "spec-a", true)
	if err != nil {
		t.Fatal(err)
	}
	if one.ID == two.ID || one.Inode == two.Inode {
		t.Fatal("owners must have independent lifecycle identities and data directories")
	}
	if got, err := Path(base, "owner-a"); err != nil || got != filepath.Join(base, "homes", "owner-a", "data") {
		t.Fatalf("Path = %q, %v", got, err)
	}
	for _, owner := range []string{"", "..", "../escape", "a/b", ".hidden", "a\\b"} {
		if _, err := Path(base, owner); err == nil {
			t.Errorf("Path accepted invalid owner %q", owner)
		}
	}
	if _, err := Verify(base, "owner-a", "node-a", "wrong", one.ID); err == nil {
		t.Fatal("Verify accepted the wrong spec")
	}
	if _, err := Verify(base, "owner-b", "node-a", "spec-a", one.ID); err == nil {
		t.Fatal("Verify accepted another owner's lifecycle ID")
	}
	if _, err := Prepare(base, "owner-a", "node-a", "wrong", true); err == nil {
		t.Fatal("Prepare accepted the wrong spec for an existing home")
	}
}

func TestPrepareCleansAndRecoversInterruptedInitialization(t *testing.T) {
	t.Run("cleans stale marker without resetting state", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "cellbox")
		identity, err := Prepare(base, "owner", "node", "spec", true)
		if err != nil {
			t.Fatal(err)
		}
		if err := ClaimExecution(base, "owner", identity.ID, "pod", ""); err != nil {
			t.Fatal(err)
		}
		_, dir, _, _ := paths(base, "owner")
		partial := initializingIdentity{ID: identity.ID, OwnerUID: identity.OwnerUID, NodeName: identity.NodeName, SpecHash: identity.SpecHash}
		if err := writeJSONAtomic(filepath.Join(dir, initName), partial, 0600); err != nil {
			t.Fatal(err)
		}
		got, err := Prepare(base, "owner", "node", "spec", true)
		if err != nil || got.ID != identity.ID {
			t.Fatalf("Prepare = %#v, %v", got, err)
		}
		if _, err := os.Lstat(filepath.Join(dir, initName)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stale initializing marker remains: %v", err)
		}
		state, err := readState(filepath.Join(dir, stateName))
		if err != nil || state.Phase != "active" || state.Pod != "pod" {
			t.Fatalf("Prepare reset existing execution state: %#v, %v", state, err)
		}
	})

	t.Run("refuses to reset state for non-empty data", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "cellbox")
		identity, err := Prepare(base, "owner", "node", "spec", true)
		if err != nil {
			t.Fatal(err)
		}
		_, dir, data, _ := paths(base, "owner")
		if err := os.WriteFile(filepath.Join(data, "guest-file"), []byte("persisted"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(dir, stateName)); err != nil {
			t.Fatal(err)
		}
		partial := initializingIdentity{ID: identity.ID, OwnerUID: identity.OwnerUID, NodeName: identity.NodeName, SpecHash: identity.SpecHash}
		if err := writeJSONAtomic(filepath.Join(dir, initName), partial, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Prepare(base, "owner", "node", "spec", true); err == nil {
			t.Fatal("Prepare reset state for a non-empty home")
		}
		if _, err := os.Stat(filepath.Join(data, "guest-file")); err != nil {
			t.Fatalf("Prepare altered persisted data: %v", err)
		}
		if _, err := os.Stat(filepath.Join(dir, stateName)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Prepare unexpectedly wrote state: %v", err)
		}
	})
}

func TestPrepareMissingAndReplacedDataFailClosed(t *testing.T) {
	base := filepath.Join(t.TempDir(), "cellbox")
	if _, err := Prepare(base, "missing", "node", "spec", false); err == nil {
		t.Fatal("Prepare(false) created a missing home")
	}
	identity, err := Prepare(base, "owner", "node", "spec", true)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := Path(base, "owner")
	if err := os.Rename(data, data+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(base, "owner", "node", "spec", identity.ID); err == nil {
		t.Fatal("Verify accepted replacement data inode")
	}
	if _, err := Prepare(base, "owner", "node", "spec", true); err == nil {
		t.Fatal("Prepare accepted replacement data inode")
	}
}

func TestClaimCheckpointResumeConsumesSnapshot(t *testing.T) {
	base := filepath.Join(t.TempDir(), "cellbox")
	identity, err := Prepare(base, "owner", "node", "spec", true)
	if err != nil {
		t.Fatal(err)
	}
	oldSync := syncFS
	syncFS = func(int) error { return nil }
	t.Cleanup(func() { syncFS = oldSync })
	if err := ClaimExecution(base, "owner", identity.ID, "pod-a", ""); err != nil {
		t.Fatal(err)
	}
	if err := ClaimExecution(base, "owner", identity.ID, "pod-b", ""); err == nil {
		t.Fatal("active execution was claimed by a second Pod")
	}
	if err := Checkpoint(base, "owner", identity.ID, "pod-b", "snap-1"); err == nil {
		t.Fatal("checkpoint accepted a different active Pod")
	}
	if err := Checkpoint(base, "owner", identity.ID, "pod-a", "snap-1"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCheckpoint(base, "owner", identity.ID, "snap-1"); err != nil {
		t.Fatal(err)
	}
	if err := ClaimExecution(base, "owner", identity.ID, "pod-b", "snap-1"); err != nil {
		t.Fatal(err)
	}
	if err := ClaimExecution(base, "owner", identity.ID, "pod-c", "snap-1"); err == nil {
		t.Fatal("consumed snapshot was replayed")
	}
	if err := Checkpoint(base, "owner", identity.ID, "pod-b", "snap-1"); err == nil {
		t.Fatal("checkpoint accepted a consumed snapshot")
	}
	if err := Checkpoint(base, "owner", identity.ID, "pod-b", "snap-2"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCheckpoint(base, "owner", identity.ID, "snap-1"); err == nil {
		t.Fatal("old checkpoint remained valid after the home advanced")
	}
}

func TestRemoveOnlyRemovesValidatedOwnerAndDoesNotFollowGuestSymlink(t *testing.T) {
	base := filepath.Join(t.TempDir(), "cellbox")
	if _, err := Prepare(base, "owner-a", "node", VolumeSpecHash, true); err != nil {
		t.Fatal(err)
	}
	if _, err := Prepare(base, "owner-b", "node", VolumeSpecHash, true); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.Mkdir(outside, 0700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(outside, "keep")
	if err := os.WriteFile(sentinel, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	data, _ := Path(base, "owner-a")
	if err := os.Symlink(outside, filepath.Join(data, "guest-link")); err != nil {
		t.Fatal(err)
	}
	if err := RemoveVolume(base, "owner-a", "wrong-node"); err == nil {
		t.Fatal("RemoveVolume accepted a mismatched node")
	}
	if err := RemoveVolume(base, "owner-a", "node"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("Remove followed a guest symlink: %v", err)
	}
	if _, err := Verify(base, "owner-b", "node", VolumeSpecHash, ""); err != nil {
		t.Fatalf("removing owner-a affected owner-b: %v", err)
	}
	if err := RemoveVolume(base, "owner-a", "node"); err != nil {
		t.Fatalf("repeated RemoveVolume should be idempotent: %v", err)
	}
}
