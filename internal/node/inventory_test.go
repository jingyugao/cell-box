// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/inventory"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func suspendedInventoryWorkload(owner, snapshot string) *api.ResumablePod {
	annotation, _ := json.Marshal(inventory.Record{
		ClientID: "client-one",
		Box:      json.RawMessage(`{"id":"box-one","phase":"running"}`),
	})
	return &api.ResumablePod{
		ObjectMeta: meta.ObjectMeta{
			UID:         typesUID(owner),
			Labels:      map[string]string{"cellbox.local/box-id": "box-one", inventory.ClientLabel: inventory.ClientValue("client-one")},
			Annotations: map[string]string{inventory.Annotation: string(annotation)},
		},
		Spec:   api.Spec{DesiredState: "Suspended"},
		Status: api.Status{Phase: "Suspended", Snapshot: snapshot, SpecHash: "spec"},
	}
}

func TestSuspendedInventoryUsesVerifiedLocalSnapshotWithoutRemoteTar(t *testing.T) {
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	const owner, snapshot = "owner-local", "checkpoint-local"
	if err := os.MkdirAll(filepath.Dir(runsc), 0700); err != nil {
		t.Fatal(err)
	}
	path := testCheckpoint(t, base, owner, snapshot, "spec", "image-id", runsc)
	objects := &memoryObjects{objects: map[string][]byte{}}
	backend := &Backend{Base: base, Runsc: runsc, Objects: objects}
	if err := backend.SyncInventory(context.Background(), suspendedInventoryWorkload(owner, snapshot)); err != nil {
		t.Fatalf("publish inventory from valid local checkpoint: %v", err)
	}
	if _, ok := objects.objects["checkpoints/"+owner+"/metadata.json"]; !ok {
		t.Fatal("suspended inventory was not published")
	}
	if _, ok := objects.objects["checkpoints/"+owner+"/"+snapshot+".tar"]; ok {
		t.Fatal("checkpoint tar was unexpectedly published")
	}
	if objects.getCalls != 1 {
		t.Fatalf("inventory publication performed %d object reads; only the metadata lookup is expected", objects.getCalls)
	}
	if _, err := VerifyPrepared(path, owner, "spec", runsc); err != nil {
		t.Fatalf("local checkpoint was not cached as verified: %v", err)
	}
}

func TestSuspendedInventoryRejectsMissingOrCorruptLocalSnapshot(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(t *testing.T, base, runsc string)
	}{
		{name: "missing"},
		{
			name: "corrupt",
			mutate: func(t *testing.T, base, runsc string) {
				path := testCheckpoint(t, base, "owner-local", "checkpoint-local", "spec", "image-id", runsc)
				if err := os.WriteFile(filepath.Join(path, "checkpoint.img"), []byte("corrupt"), 0600); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := t.TempDir()
			runsc := filepath.Join(base, "runsc")
			if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
				t.Fatal(err)
			}
			if test.mutate != nil {
				test.mutate(t, base, runsc)
			}
			objects := &memoryObjects{objects: map[string][]byte{}}
			backend := &Backend{Base: base, Runsc: runsc, Objects: objects}
			err := backend.SyncInventory(context.Background(), suspendedInventoryWorkload("owner-local", "checkpoint-local"))
			if err == nil {
				t.Fatal("published suspended inventory without a valid local checkpoint")
			}
			if len(objects.objects) != 0 {
				t.Fatalf("invalid local checkpoint produced objects: %v", objects.objects)
			}
		})
	}
}
