package node

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/homevolume"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCaptureWorkspaceArchiveFromSuspendedHomeIsBoundedAndIdempotent(t *testing.T) {
	base := t.TempDir()
	identity, err := homevolume.Prepare(base, "owner", "node-a", "spec", true)
	if err != nil {
		t.Fatal(err)
	}
	home, err := homevolume.Path(base, "owner")
	if err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(home, "workspace", "src")
	if err = os.MkdirAll(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(workspace, "main.go"), []byte("package main\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink("/etc/passwd", filepath.Join(workspace, "link")); err != nil {
		t.Fatal(err)
	}
	if err = homevolume.ClaimExecution(base, "owner", identity.ID, "pod", ""); err != nil {
		t.Fatal(err)
	}
	if err = homevolume.Checkpoint(base, "owner", identity.ID, "pod", "checkpoint-1"); err != nil {
		t.Fatal(err)
	}
	runsc := filepath.Join(base, "runsc")
	if err = os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	snapshotDir, err := SnapshotPath(base, "owner", "checkpoint-1")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(snapshotDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(snapshotDir, "checkpoint.img"), []byte("state"), 0600); err != nil {
		t.Fatal(err)
	}
	runscHash, err := Digest(runsc)
	if err != nil {
		t.Fatal(err)
	}
	if err = Seal(snapshotDir, Manifest{OwnerUID: "owner", SpecHash: "spec", RunscHash: runscHash, HomeID: identity.ID}); err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(guestapi.Config{Workspace: "/home/agent/workspace", Agent: guestapi.Identity{UID: 1000, GID: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	objects := &memoryObjects{objects: map[string][]byte{}}
	backend := &Backend{Base: base, Runsc: runsc, Objects: objects}
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "owner"}, Spec: api.Spec{NodeName: "node-a", DesiredState: "Suspended", PersistentHome: true, Container: core.Container{Args: []string{"serve", "--config-base64", base64.StdEncoding.EncodeToString(config)}}}, Status: api.Status{Phase: "Suspended", Snapshot: "checkpoint-1", SpecHash: "spec"}}
	request := api.ArchiveCaptureRequest{ID: "arc-0123456789abcdef0123456789abcdef", Snapshot: "checkpoint-1", ExpiresAt: time.Now().Add(time.Minute)}
	result, err := backend.CaptureWorkspaceArchive(context.Background(), w, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Size < 1 || len(result.SHA256) != 64 {
		t.Fatalf("invalid archive result: %+v", result)
	}
	data := objects.objects["archives/"+request.ID+".tar.gz"]
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	header, err := tr.Next()
	if err != nil || header.Name != "src/" || header.Typeflag != tar.TypeDir {
		t.Fatalf("first tar entry = %#v, %v", header, err)
	}
	header, err = tr.Next()
	if err != nil || header.Name != "src/main.go" {
		t.Fatalf("workspace file entry = %#v, %v", header, err)
	}
	content, err := io.ReadAll(tr)
	if err != nil || string(content) != "package main\n" {
		t.Fatalf("workspace file content %q: %v", content, err)
	}
	if _, err = tr.Next(); err != io.EOF {
		t.Fatalf("symlink was archived or tar had trailing entries: %v", err)
	}
	_ = gz.Close()
	if _, err = backend.CaptureWorkspaceArchive(context.Background(), w, request); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	if objects.putCalls != 1 {
		t.Fatalf("retry uploaded %d times", objects.putCalls)
	}
}

func TestWorkspaceArchiveRejectsOverlappingVolumesButAllowsExternalMounts(t *testing.T) {
	config, err := json.Marshal(guestapi.Config{Workspace: "/home/agent/workspace/src"})
	if err != nil {
		t.Fatal(err)
	}
	base := []api.VolumeMount{
		{UID: "home-volume", MountPath: "/home/agent"},
		{UID: "data-volume", MountPath: "/data"},
	}
	w := &api.ResumablePod{Spec: api.Spec{Mounts: base, Container: core.Container{Args: []string{"--config-base64", base64.StdEncoding.EncodeToString(config)}}}}
	if _, err := writableWorkspaceHomeMount(w); err != nil {
		t.Fatalf("external data volume should not block archive: %v", err)
	}
	w.Spec.Mounts = []api.VolumeMount{{UID: "data-volume", MountPath: "/data"}}
	if _, err := writableWorkspaceHomeMount(w); err == nil {
		t.Fatal("data-only mounts unexpectedly support HOME workspace archives")
	}
	for _, mountPath := range []string{"/home", "/home/agent/workspace", "/home/agent/workspace/src/cache"} {
		t.Run(mountPath, func(t *testing.T) {
			w.Spec.Mounts = append(append([]api.VolumeMount(nil), base...), api.VolumeMount{UID: "overlap-volume", MountPath: mountPath})
			if _, err := writableWorkspaceHomeMount(w); err == nil {
				t.Fatalf("overlapping volume %q was archived", mountPath)
			}
		})
	}
}

func TestCaptureWorkspaceArchiveRejectsIntermediateSymlink(t *testing.T) {
	base := t.TempDir()
	identity, err := homevolume.Prepare(base, "owner", "node-a", "spec", true)
	if err != nil {
		t.Fatal(err)
	}
	home, _ := homevolume.Path(base, "owner")
	if err = os.MkdirAll(filepath.Join(home, "real", "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(home, "real", "nested", "marker.txt"), []byte("must not be captured"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink("real", filepath.Join(home, "workspace-link")); err != nil {
		t.Fatal(err)
	}
	if err = homevolume.ClaimExecution(base, "owner", identity.ID, "pod", ""); err != nil {
		t.Fatal(err)
	}
	if err = homevolume.Checkpoint(base, "owner", identity.ID, "pod", "checkpoint-1"); err != nil {
		t.Fatal(err)
	}
	runsc := filepath.Join(base, "runsc")
	if err = os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	snapshotDir, err := SnapshotPath(base, "owner", "checkpoint-1")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(snapshotDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(snapshotDir, "checkpoint.img"), []byte("state"), 0600); err != nil {
		t.Fatal(err)
	}
	runscHash, err := Digest(runsc)
	if err != nil {
		t.Fatal(err)
	}
	if err = Seal(snapshotDir, Manifest{OwnerUID: "owner", SpecHash: "spec", RunscHash: runscHash, HomeID: identity.ID}); err != nil {
		t.Fatal(err)
	}
	config, _ := json.Marshal(guestapi.Config{Workspace: "/home/agent/workspace-link/nested"})
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "owner"}, Spec: api.Spec{NodeName: "node-a", DesiredState: "Suspended", PersistentHome: true, Container: core.Container{Args: []string{"--config-base64", base64.StdEncoding.EncodeToString(config)}}}, Status: api.Status{Phase: "Suspended", Snapshot: "checkpoint-1", SpecHash: "spec"}}
	objects := &memoryObjects{objects: map[string][]byte{}}
	backend := &Backend{Base: base, Runsc: runsc, Objects: objects}
	_, err = backend.CaptureWorkspaceArchive(context.Background(), w, api.ArchiveCaptureRequest{ID: "arc-1123456789abcdef0123456789abcdef", Snapshot: "checkpoint-1"})
	if err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("intermediate symlink accepted: %v", err)
	}
	if objects.putCalls != 0 {
		t.Fatalf("intermediate symlink path uploaded %d objects", objects.putCalls)
	}
	w.Status.Phase = "Failed"
	w.Status.Snapshot = ""
	_, err = backend.CaptureWorkspaceArchive(context.Background(), w, api.ArchiveCaptureRequest{ID: "arc-2123456789abcdef0123456789abcdef"})
	if err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("disk-only archive accepted intermediate symlink: %v", err)
	}
	if objects.putCalls != 0 {
		t.Fatalf("disk-only symlink path uploaded %d objects", objects.putCalls)
	}
}

func TestCaptureWorkspaceArchiveUsesSandboxManifestForSharedVolume(t *testing.T) {
	base := t.TempDir()
	const owner, volumeUID, snapshot = "box-a", "shared-volume", "checkpoint-a"
	identity, err := homevolume.Prepare(base, volumeUID, "node-a", homevolume.VolumeSpecHash, true)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := homevolume.Path(base, volumeUID)
	workspace := filepath.Join(root, "workspace", "src")
	if err = os.MkdirAll(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(workspace, "main.go"), []byte("shared volume"), 0600); err != nil {
		t.Fatal(err)
	}
	runsc := filepath.Join(base, "runsc")
	if err = os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	runscHash, err := Digest(runsc)
	if err != nil {
		t.Fatal(err)
	}
	snapshotDir, _ := SnapshotPath(base, owner, snapshot)
	if err = os.MkdirAll(snapshotDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(snapshotDir, "checkpoint.img"), []byte("sandbox state"), 0600); err != nil {
		t.Fatal(err)
	}
	volumeMount := VolumeMountAuthorization{UID: volumeUID, MountPath: homevolume.MountPath, HomeID: identity.ID}
	if err = Seal(snapshotDir, Manifest{OwnerUID: owner, SpecHash: "spec", RunscHash: runscHash, Mounts: []VolumeMountAuthorization{volumeMount}}); err != nil {
		t.Fatal(err)
	}
	config, _ := json.Marshal(guestapi.Config{Workspace: "/home/agent/workspace/src"})
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: owner}, Spec: api.Spec{
		NodeName: "node-a", DesiredState: "Suspended", Mounts: []api.VolumeMount{{UID: volumeUID, MountPath: homevolume.MountPath}},
		Container: core.Container{Args: []string{"--config-base64", base64.StdEncoding.EncodeToString(config)}},
	}, Status: api.Status{Phase: "Suspended", Snapshot: snapshot, SpecHash: "spec"}}
	objects := &memoryObjects{objects: map[string][]byte{}}
	backend := &Backend{Base: base, Runsc: runsc, Objects: objects}
	request := api.ArchiveCaptureRequest{ID: "arc-3123456789abcdef0123456789abcdef", Snapshot: snapshot}
	if _, err = backend.CaptureWorkspaceArchive(context.Background(), w, request); err != nil {
		t.Fatalf("paused Box could not archive a shared volume from its own manifest: %v", err)
	}
	if _, err = os.Stat(filepath.Join(filepath.Dir(root), ".state.json")); !os.IsNotExist(err) {
		t.Fatalf("archive required or created exclusive volume state: %v", err)
	}
}

func TestCaptureFailedStoppedPersistentHomeWithoutCheckpoint(t *testing.T) {
	base := t.TempDir()
	_, err := homevolume.Prepare(base, "owner", "node-a", "spec", true)
	if err != nil {
		t.Fatal(err)
	}
	home, err := homevolume.Path(base, "owner")
	if err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(home, "workspace", "src")
	if err = os.MkdirAll(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(workspace, "marker.txt"), []byte("saved without checkpoint"), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(guestapi.Config{Workspace: "/home/agent/workspace/src", Agent: guestapi.Identity{UID: 1000, GID: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	objects := &memoryObjects{objects: map[string][]byte{}}
	backend := &Backend{Base: base, Objects: objects}
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "owner"}, Spec: api.Spec{NodeName: "node-a", DesiredState: "Running", PersistentHome: true, Container: core.Container{Args: []string{"serve", "--config-base64", base64.StdEncoding.EncodeToString(config)}}}, Status: api.Status{Phase: "Failed", SpecHash: "spec"}}
	request := api.ArchiveCaptureRequest{ID: "arc-2123456789abcdef0123456789abcdef", ExpiresAt: time.Now().Add(time.Minute)}
	result, err := backend.CaptureWorkspaceArchive(context.Background(), w, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Size < 1 || len(result.SHA256) != 64 || result.Snapshot != "" {
		t.Fatalf("invalid failed-source archive result: %+v", result)
	}
	if objects.putCalls != 1 {
		t.Fatalf("failed-source archive uploaded %d objects", objects.putCalls)
	}
}
