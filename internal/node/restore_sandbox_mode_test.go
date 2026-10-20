// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func writeSandboxModePolicy(t *testing.T, base, mode string) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("restore policy integration requires a root-owned policy file")
	}
	contents, err := json.Marshal(map[string]string{"sandboxMode": mode})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(base, restorePolicyFile)
	if err := os.WriteFile(path, contents, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestRestorePolicySandboxModeValidationAndDefault(t *testing.T) {
	if err := validateRestorePolicy(RestorePolicy{}); err != nil {
		t.Fatalf("missing sandbox mode should default to root: %v", err)
	}
	for _, mode := range []string{"", "root", "no-root"} {
		if err := validateRestorePolicy(RestorePolicy{SandboxMode: mode}); err != nil {
			t.Errorf("sandbox mode %q rejected: %v", mode, err)
		}
	}
	if err := validateRestorePolicy(RestorePolicy{SandboxMode: "userns"}); err == nil {
		t.Fatal("unknown sandbox mode accepted")
	}
	policy, err := LoadRestorePolicy(filepath.Join(t.TempDir(), "missing"))
	if err != nil || policy.noRoot() {
		t.Fatalf("missing policy should use root mode: policy=%+v err=%v", policy, err)
	}
}

func TestPrepareUsesSealedSandboxModeForExistingCheckpoint(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	path := testCheckpoint(t, base, "owner", "snapshot", "spec", "image-id", runsc)
	var manifest Manifest
	if err := ReadJSON(filepath.Join(path, "manifest.json"), &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.NoRoot = true
	if err := os.Remove(filepath.Join(path, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if err := Seal(path, manifest); err != nil {
		t.Fatal(err)
	}
	b := &Backend{Base: base, Runsc: runsc, Images: imageClient{id: "image-id"}}
	w := testWorkload("owner", "snapshot", "spec", "image")
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod", Name: "pod", Namespace: "test"}}
	if err := b.Prepare(ctx, w, p); err != nil {
		t.Fatal(err)
	}
	var ticket Ticket
	if err := ReadJSON(filepath.Join(base, "tickets", "pod.json"), &ticket); err != nil {
		t.Fatal(err)
	}
	if !ticket.NoRoot {
		t.Fatal("current default policy changed the mode sealed in the checkpoint")
	}
	verified, err := VerifyCached(path, "owner", "spec", runsc)
	if err != nil || !verified.NoRoot {
		t.Fatalf("no-root checkpoint manifest changed: manifest=%+v err=%v", verified, err)
	}
}

func TestPrepareColdStartUsesNoRootPolicy(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("restore policy integration requires a root-owned policy file")
	}
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	writeSandboxModePolicy(t, base, "no-root")
	b := &Backend{Base: base, Runsc: runsc}
	w := testWorkload("owner", "", "spec", "image")
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod", Name: "pod", Namespace: "test"}}
	if err := b.Prepare(context.Background(), w, p); err != nil {
		t.Fatal(err)
	}
	var ticket Ticket
	if err := ReadJSON(filepath.Join(base, "tickets", "pod.json"), &ticket); err != nil {
		t.Fatal(err)
	}
	if !ticket.NoRoot {
		t.Fatal("cold start ticket omitted configured no-root mode")
	}
}

func TestExistingRootCheckpointStaysRootUnderNoRootPolicy(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("restore policy integration requires a root-owned policy file")
	}
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	testCheckpoint(t, base, "owner", "snapshot", "spec", "image-id", runsc)
	writeSandboxModePolicy(t, base, "no-root")
	b := &Backend{Base: base, Runsc: runsc, Images: imageClient{id: "image-id"}}
	w := testWorkload("owner", "snapshot", "spec", "image")
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod", Name: "pod", Namespace: "test"}}
	if err := b.Prepare(context.Background(), w, p); err != nil {
		t.Fatal(err)
	}
	var ticket Ticket
	if err := ReadJSON(filepath.Join(base, "tickets", "pod.json"), &ticket); err != nil {
		t.Fatal(err)
	}
	if ticket.NoRoot {
		t.Fatal("no-root policy changed the mode of an existing root checkpoint")
	}
}

func TestCheckpointModeComesFromLiveRequest(t *testing.T) {
	base := t.TempDir()
	r := testWorkload("owner", "next-snapshot", "spec", "image")
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod", Name: "pod", Namespace: "test"}}
	const sid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := AtomicJSON(filepath.Join(base, "requests", sid+".json"), Ticket{OwnerUID: "owner", PodUID: "pod", PodName: "pod", Namespace: "test", SpecHash: "spec", NoRoot: true}); err != nil {
		t.Fatal(err)
	}
	if noRoot, err := checkpointSandboxMode(base, sid, r, p); err != nil || !noRoot {
		t.Fatalf("request-locked no-root mode = %t, err=%v", noRoot, err)
	}
	if err := os.Remove(filepath.Join(base, "requests", sid+".json")); err != nil {
		t.Fatal(err)
	}
	if noRoot, err := checkpointSandboxMode(base, sid, r, p); err != nil || noRoot {
		t.Fatalf("legacy root request fallback = %t, err=%v", noRoot, err)
	}
	if os.Geteuid() == 0 {
		writeSandboxModePolicy(t, base, "no-root")
		if err := AtomicJSON(filepath.Join(base, "requests", sid+".json"), Ticket{OwnerUID: "owner", PodUID: "pod", PodName: "pod", Namespace: "test", SpecHash: "spec"}); err != nil {
			t.Fatal(err)
		}
		if noRoot, err := checkpointSandboxMode(base, sid, r, p); err != nil || noRoot {
			t.Fatalf("identity-checked legacy root request = %t, err=%v", noRoot, err)
		}
		if err := os.Remove(filepath.Join(base, "requests", sid+".json")); err != nil {
			t.Fatal(err)
		}
		if _, err := checkpointSandboxMode(base, sid, r, p); err == nil || !strings.Contains(err.Error(), "missing") {
			t.Fatalf("missing request under no-root policy was accepted: %v", err)
		}
	}
	if err := AtomicJSON(filepath.Join(base, "requests", sid+".json"), Ticket{OwnerUID: "other", PodUID: "pod", PodName: "pod", Namespace: "test", SpecHash: "spec"}); err != nil {
		t.Fatal(err)
	}
	if _, err := checkpointSandboxMode(base, sid, r, p); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("foreign request identity was accepted: %v", err)
	}
}

func TestWarmRejectsModeMismatchBeforeVolumePreparation(t *testing.T) {
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	path := testCheckpoint(t, base, "owner", "snapshot", "spec", "image-id", runsc)
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "warm-pod", Name: "warm-pod", Namespace: "test"}}
	b := &Backend{Base: base, Runsc: runsc, Images: imageClient{id: "image-id"}}
	profile := testWorkload("profile", "snapshot", "spec", "image")
	profile.Spec.PersistentHome = true
	profile.Spec.NodeName = "node"
	if err := b.RegisterWarm(context.Background(), p, api.SpecFingerprint(profile)); err != nil {
		t.Fatal(err)
	}
	if _, err := warmUpdate(base, "warm-pod", false, func(l *WarmLease) error {
		l.NoRoot = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := BindWarm(base, "warm-pod", p.Name, p.Namespace, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	var placeholder Ticket
	if err := ReadJSON(filepath.Join(base, "requests", strings.Repeat("a", 64)+".json"), &placeholder); err != nil {
		t.Fatal(err)
	}
	if !placeholder.NoRoot {
		t.Fatal("warm bind placeholder dropped the registered no-root mode")
	}
	if _, err := warmUpdate(base, "warm-pod", false, func(l *WarmLease) error {
		l.Phase = "waiting"
		l.Deadline = time.Now().Add(time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	w := testWorkload("owner", "snapshot", "spec", "image")
	w.Spec.PersistentHome = true
	w.Spec.HomeVolumeUID = "volume-owner"
	w.Spec.NodeName = "node"
	compatible, err := b.WarmCompatible(context.Background(), w, p)
	if err != nil || compatible {
		t.Fatalf("mode-incompatible warm lease: compatible=%t err=%v", compatible, err)
	}
	if _, err := b.ActivateWarm(context.Background(), w, p); err == nil || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("mode mismatch was not refused: %v", err)
	}
	lease, err := b.WarmStatus(context.Background(), p)
	if err != nil || lease.Phase != "waiting" {
		t.Fatalf("mode mismatch changed warm lease before claim: lease=%+v err=%v", lease, err)
	}
	if _, err := os.Stat(filepath.Join(base, "tickets", "warm-pod.json")); !os.IsNotExist(err) {
		t.Fatalf("mode mismatch wrote execution ticket before claim: %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, "homes", "volume-owner")); !os.IsNotExist(err) {
		t.Fatalf("mode mismatch attached a volume before rejection: %v", err)
	}
	manifest, err := VerifyCached(path, "owner", "spec", runsc)
	if err != nil || manifest.NoRoot {
		t.Fatalf("fixture root manifest changed: manifest=%+v err=%v", manifest, err)
	}
}

func TestWarmStateModeChangeRetiresOnlyIdleLease(t *testing.T) {
	for _, phase := range []string{"registered", "created", "waiting", "claimed", "restoring"} {
		t.Run(phase, func(t *testing.T) {
			base := t.TempDir()
			p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "warm-pod", Name: "warm-pod", Namespace: "test"}}
			b := &Backend{Base: base}
			if err := b.RegisterWarm(context.Background(), p, "profile"); err != nil {
				t.Fatal(err)
			}
			if _, err := warmUpdate(base, "warm-pod", false, func(l *WarmLease) error { l.Phase = phase; return nil }); err != nil {
				t.Fatal(err)
			}
			if _, err := warmUpdate(base, "warm-pod", false, func(l *WarmLease) error { l.NoRoot = true; return nil }); err != nil {
				t.Fatal(err)
			}
			got, _, err := b.WarmState(context.Background(), p)
			if err != nil {
				t.Fatal(err)
			}
			want := phase
			if phase == "registered" || phase == "created" || phase == "waiting" {
				want = "retired"
			}
			if got != want {
				t.Fatalf("WarmState phase = %q, want %q", got, want)
			}
		})
	}
}
