package adapter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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

func TestIndependentHomeAuthorizationUsesVolumeAndAttachmentIdentity(t *testing.T) {
	base := t.TempDir()
	const volumeUID, ownerUID = "volume-stable", "box-current"
	home, err := homevolume.Prepare(base, volumeUID, "node-a", homevolume.VolumeSpecHash, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = homevolume.Attach(base, volumeUID, "node-a", ownerUID); err != nil {
		t.Fatal(err)
	}
	a := Adapter{Base: base}
	ticket := node.Ticket{OwnerUID: ownerUID, NodeName: "node-a", HomeVolumeUID: volumeUID, HomeID: home.ID}
	if err := a.verifyHomeAttachment(ticket); err != nil {
		t.Fatal(err)
	}
	ticket.OwnerUID = "box-stale"
	if err := a.verifyHomeAttachment(ticket); err == nil {
		t.Fatal("stale Box ticket authorized the volume attachment")
	}

	ticket.OwnerUID = ownerUID
	bundle := filepath.Join(base, "bundle")
	if err := os.MkdirAll(bundle, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), []byte(`{"process":{},"root":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.bindHome(bundle, ticket); err != nil {
		t.Fatal(err)
	}
	var config struct {
		Mounts []struct {
			Source string `json:"source"`
		} `json:"mounts"`
	}
	if err := node.ReadJSON(filepath.Join(bundle, "config.json"), &config); err != nil {
		t.Fatal(err)
	}
	var got string
	for _, mount := range config.Mounts {
		got = mount.Source
	}
	want, _ := homevolume.Path(base, volumeUID)
	if got != want {
		b, _ := json.Marshal(config.Mounts)
		t.Fatalf("adapter bound HOME to %q (%s), want %q", got, b, want)
	}
}
