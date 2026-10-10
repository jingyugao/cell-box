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
	config, err := json.Marshal(guestapi.Config{Workspace: "/home/agent/workspace", Agent: guestapi.Identity{UID: 1000, GID: 1000}})
	if err != nil {
		t.Fatal(err)
	}
	objects := &memoryObjects{objects: map[string][]byte{}}
	backend := &Backend{Base: base, Objects: objects}
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
	config, _ := json.Marshal(guestapi.Config{Workspace: "/home/agent/workspace-link/nested"})
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "owner"}, Spec: api.Spec{NodeName: "node-a", DesiredState: "Suspended", PersistentHome: true, Container: core.Container{Args: []string{"--config-base64", base64.StdEncoding.EncodeToString(config)}}}, Status: api.Status{Phase: "Suspended", Snapshot: "checkpoint-1", SpecHash: "spec"}}
	objects := &memoryObjects{objects: map[string][]byte{}}
	backend := &Backend{Base: base, Objects: objects}
	_, err = backend.CaptureWorkspaceArchive(context.Background(), w, api.ArchiveCaptureRequest{ID: "arc-1123456789abcdef0123456789abcdef", Snapshot: "checkpoint-1"})
	if err == nil || !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("intermediate symlink accepted: %v", err)
	}
	if objects.putCalls != 0 {
		t.Fatalf("intermediate symlink path uploaded %d objects", objects.putCalls)
	}
}
