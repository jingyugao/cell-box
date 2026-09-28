// SPDX-License-Identifier: Apache-2.0

package node

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"context"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManifestBindsContentOwnerSpecAndBinary(t *testing.T) {
	base := t.TempDir()
	bin := filepath.Join(base, "runsc")
	os.WriteFile(bin, []byte("binary"), 0700)
	hash, _ := Digest(bin)
	dir := filepath.Join(base, "snapshot")
	os.Mkdir(dir, 0700)
	os.WriteFile(filepath.Join(dir, "checkpoint.img"), []byte("memory"), 0600)
	if err := Seal(dir, Manifest{OwnerUID: "owner", SpecHash: "spec", RunscHash: hash}); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(dir, "owner", "spec", bin); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(dir, "other", "spec", bin); err == nil {
		t.Fatal("wrong owner accepted")
	}
	os.WriteFile(filepath.Join(dir, "checkpoint.img"), []byte("corrupt"), 0600)
	if _, err := Verify(dir, "owner", "spec", bin); err == nil {
		t.Fatal("corruption accepted")
	}
	os.Remove(filepath.Join(dir, "checkpoint.img"))
	os.Symlink(bin, filepath.Join(dir, "checkpoint.img"))
	if _, err := Verify(dir, "owner", "spec", bin); err == nil {
		t.Fatal("symlink accepted")
	}
	if _, err := SnapshotPath(base, "../escape", "snap"); err == nil {
		t.Fatal("traversal accepted")
	}
}

func TestCheckpointCrashRecoveryNeverRepeatsSave(t *testing.T) {
	base := t.TempDir()
	binary := filepath.Join(base, "runsc")
	os.WriteFile(binary, []byte("runtime"), 0700)
	b := &Backend{Base: base, Runsc: binary} // No CRI client: a replay would panic.
	w := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: "owner"}, Status: api.Status{SpecHash: "spec", Snapshot: "checkpoint-1"}}
	path, _ := SnapshotPath(base, "owner", "checkpoint-1")
	os.MkdirAll(path+".pending", 0700)
	if err := b.Checkpoint(context.Background(), w, nil); err == nil || !strings.Contains(err.Error(), "interrupted checkpoint") {
		t.Fatal(err)
	}
	hash, _ := Digest(binary)
	os.WriteFile(filepath.Join(path+".pending", "checkpoint.img"), []byte("memory"), 0600)
	if err := Seal(path+".pending", Manifest{OwnerUID: "owner", SpecHash: "spec", RunscHash: hash}); err != nil {
		t.Fatal(err)
	}
	os.Rename(path+".pending", path)
	if err := b.Checkpoint(context.Background(), w, nil); err != nil {
		t.Fatal("committed snapshot must be adopted", err)
	}
}
