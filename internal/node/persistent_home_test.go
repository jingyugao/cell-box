package node

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/homevolume"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	if data, err := os.ReadFile(filepath.Join(home, "workspace-data")); err != nil || string(data) != "keep" {
		t.Fatalf("Box deletion did not preserve and promote its HOME: %q %v", data, err)
	}
	if _, err := homevolume.Verify(base, "box-owner", "node", homevolume.VolumeSpecHash, ""); err != nil {
		t.Fatalf("legacy HOME was not promoted to an independent volume: %v", err)
	}
	other, _ := homevolume.Path(base, "other-owner")
	if _, err := os.Stat(other); err != nil {
		t.Fatal("Box deletion affected another HOME", err)
	}
}

func TestIndependentHomeVolumeSurvivesBoxReplacementAndExplicitDelete(t *testing.T) {
	base := t.TempDir()
	b := &Backend{Base: base}
	const volumeUID = "volume-001"
	if err := b.CreateVolume(volumeUID, "node"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "box-a"}, Spec: api.Spec{NodeName: "node", PersistentHome: true, HomeVolumeUID: volumeUID}, Status: api.Status{Cycle: 1, SpecHash: "image-a"}}
	firstPod := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod-a", Namespace: "test"}}
	if err := b.Prepare(ctx, first, firstPod); err != nil {
		t.Fatal(err)
	}
	home, _ := homevolume.Path(base, volumeUID)
	if err := os.WriteFile(filepath.Join(home, "workspace-data"), []byte("reused"), 0600); err != nil {
		t.Fatal(err)
	}
	now := meta.Now()
	first.DeletionTimestamp = &now
	if err := b.Forget(ctx, first); err != nil {
		t.Fatal(err)
	}

	second := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "box-b"}, Spec: api.Spec{NodeName: "node", PersistentHome: true, HomeVolumeUID: volumeUID}, Status: api.Status{Cycle: 1, SpecHash: "image-b"}}
	secondPod := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod-b", Namespace: "test"}}
	if err := b.Prepare(ctx, second, secondPod); err != nil {
		t.Fatalf("new Box could not attach the retained volume: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(home, "workspace-data")); err != nil || string(got) != "reused" {
		t.Fatalf("replacement Box did not see existing data: %q %v", got, err)
	}
	second.DeletionTimestamp = &now
	if err := b.Forget(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteVolume(volumeUID, "node"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Fatalf("explicit volume deletion retained data: %v", err)
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

func TestMultipleVolumeCheckpointIdentityAndReplacement(t *testing.T) {
	base := t.TempDir()
	b := &Backend{Base: base}
	for _, uid := range []string{"home-volume", "data-volume"} {
		if err := b.CreateVolume(uid, "node"); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	mounts := []api.VolumeMount{
		{UID: "home-volume", MountPath: "/home/agent"},
		{UID: "data-volume", MountPath: "/data", ReadOnly: true},
	}
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "box-a"}, Spec: api.Spec{NodeName: "node", Mounts: mounts, Container: core.Container{Image: "image-a"}}, Status: api.Status{Cycle: 1, SpecHash: "image-a"}}
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod-a", Namespace: "test"}}
	if err := b.Prepare(ctx, w, p); err != nil {
		t.Fatal(err)
	}
	var ticket Ticket
	if err := ReadJSON(filepath.Join(base, "tickets", "pod-a.json"), &ticket); err != nil {
		t.Fatal(err)
	}
	if len(ticket.Mounts) != 2 || !reflect.DeepEqual(ticket.Mounts, []VolumeMountAuthorization{
		{UID: "home-volume", MountPath: "/home/agent", HomeID: ticket.Mounts[0].HomeID},
		{UID: "data-volume", MountPath: "/data", ReadOnly: true, HomeID: ticket.Mounts[1].HomeID},
	}) {
		t.Fatalf("runtime ticket lost mount identity/configuration: %+v", ticket.Mounts)
	}
	concurrent := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "box-concurrent"}, Spec: api.Spec{NodeName: "node", Mounts: mounts, Container: core.Container{Image: "image-b"}}, Status: api.Status{Cycle: 1, SpecHash: "image-b"}}
	concurrentPod := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod-concurrent", Namespace: "test"}}
	if err := b.Prepare(ctx, concurrent, concurrentPod); err != nil {
		t.Fatalf("second sandbox could not prepare shared volumes: %v", err)
	}
	for _, mount := range ticket.Mounts {
		root, _ := homevolume.Path(base, mount.UID)
		if err := os.WriteFile(filepath.Join(root, "marker"), []byte(mount.UID), 0600); err != nil {
			t.Fatal(err)
		}
		if err := homevolume.SyncVolume(base, mount.UID, "node", mount.HomeID); err != nil {
			t.Fatal(err)
		}
	}
	w.Status.Snapshot = "snapshot-a"
	manifest := &Manifest{Mounts: ticket.Mounts}
	if err := b.verifySnapshotHome(w, manifest); err != nil {
		t.Fatalf("matching multi-volume checkpoint rejected: %v", err)
	}
	if _, err := b.prepareVolumeMounts(concurrent, false); err != nil {
		t.Fatalf("first sandbox checkpoint invalidated the concurrent sandbox: %v", err)
	}
	w.Spec.Mounts[1].ReadOnly = false
	if err := b.verifySnapshotHome(w, manifest); err == nil {
		t.Fatal("checkpoint accepted a changed mount mode")
	}
	w.Spec.Mounts = mounts
	now := meta.Now()
	w.DeletionTimestamp = &now
	if err := b.Forget(ctx, w); err != nil {
		t.Fatal(err)
	}
	for _, uid := range []string{"home-volume", "data-volume"} {
		second := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "box-b"}, Spec: api.Spec{NodeName: "node", Mounts: []api.VolumeMount{{UID: uid, MountPath: "/data"}}}, Status: api.Status{Cycle: 1, SpecHash: "image-b"}}
		secondPod := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: types.UID("pod-b-" + uid), Namespace: "test"}}
		if err := b.Prepare(ctx, second, secondPod); err != nil {
			t.Fatalf("replacement Box could not attach %s: %v", uid, err)
		}
		root, _ := homevolume.Path(base, uid)
		if got, err := os.ReadFile(filepath.Join(root, "marker")); err != nil || string(got) != uid {
			t.Fatalf("replacement Box lost %s data: %q %v", uid, got, err)
		}
	}
}
