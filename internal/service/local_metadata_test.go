package service

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/objectstorage"
)

func TestLocalMetadataMigratesAndServesBoxHistoryWithoutRemote(t *testing.T) {
	ctx := context.Background()
	remote := &ledgerObjects{}
	const (
		boxID     = "local-metadata-box"
		imageID   = "img-11111111111111111111111111111111"
		archiveID = "archive-11111111111111111111111111111111"
		assetKey  = "archives/archive-11111111111111111111111111111111/archive.tar.zst"
	)

	// Seed the existing object-store format, including data that must remain an
	// object-store asset rather than being copied into the local metadata tree.
	store := openRecords(t, remote)
	updateRecords(t, store, func(st *State) {
		box := coreBox(boxID)
		box.Box.OwnerKey = "owner-key"
		box.Box.ImportedImageID = imageID
		box.Staged, box.RestoreComplete, box.RestoreArchiveID = true, true, archiveID
		st.Boxes[boxID] = box
		st.Operations["operation-before-migration"] = operationRecord{ClientID: "owner",
			Operation: Operation{ID: "operation-before-migration", Kind: "resume", TargetID: boxID, Status: "succeeded", Version: 3}}
		st.Keys["owner:resume:"+boxID+":before-migration"] = keyRecord{Hash: "resume-input", OperationID: "operation-before-migration"}
		st.Archives[archiveID] = archiveRecord{ClientID: "owner", Archive: Archive{ID: archiveID,
			SourceBoxID: boxID, ManifestVersion: 1, ImportedImageID: imageID, PreparedImage: "prepared@sha256:abc"}}
		st.ImportedImages[imageID] = importedImageRecord{ClientID: "owner",
			ImportedImage: ImportedImage{ID: imageID, Source: "registry.example.test/original:tag"}}
		st.Executions["execution-before-migration"] = executionRecord{Execution: Execution{ID: "execution-before-migration",
			BoxID: boxID, OperationID: "operation-before-migration", State: "exited",
			Result: &guestapi.ExecResult{Stdout: "persisted execution output"}}}
	})
	asset := []byte("archive-payload-remains-in-object-storage")
	if _, err := remote.Put(ctx, assetKey, bytes.NewReader(asset), int64(len(asset)), "*"); err != nil {
		t.Fatal(err)
	}
	remoteRevision := remote.revision

	metadataDir := t.TempDir()
	metadata, err := objectstorage.OpenMetadata(ctx, metadataDir, remote, true)
	if err != nil {
		t.Fatal(err)
	}
	store, err = OpenObjectStore(ctx, metadata)
	if err != nil {
		metadata.Close()
		t.Fatal(err)
	}
	if remote.revision != remoteRevision {
		t.Fatalf("migration changed remote objects: revision %d -> %d", remoteRevision, remote.revision)
	}
	remote.mu.Lock()
	remoteAsset := bytes.Clone(remote.data[assetKey])
	remote.mu.Unlock()
	if !bytes.Equal(remoteAsset, asset) {
		t.Fatal("archive asset did not remain in object storage")
	}
	assetPath := filepath.Join(metadataDir, "objects", filepath.FromSlash(assetKey))
	if _, err := os.Stat(assetPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("archive asset was copied into local metadata: stat err=%v", err)
	}

	// Make the remote unavailable before Box admission/completion writes. The
	// Box record, operation, key, and execution history must all route locally.
	remote.unavailable = true
	if err := store.UpdateBox(boxID, func(st *State) error {
		box := st.Boxes[boxID]
		box.Box.OperationID = "operation-after-migration"
		box.Box.Version++
		st.Boxes[boxID] = box
		st.Operations["operation-after-migration"] = operationRecord{ClientID: "owner",
			Operation: Operation{ID: "operation-after-migration", Kind: "resume", TargetID: boxID, Status: "running", Version: 1}}
		st.Keys["owner:resume:"+boxID+":after-migration"] = keyRecord{Hash: "resume-input-2", OperationID: "operation-after-migration"}
		return nil
	}); err != nil {
		t.Fatalf("local admission update required remote storage: %v", err)
	}
	if err := store.UpdateBox(boxID, func(st *State) error {
		box := st.Boxes[boxID]
		box.Box.OperationID = ""
		box.Box.Version++
		st.Boxes[boxID] = box
		operation := st.Operations["operation-after-migration"]
		operation.Operation.Status = "succeeded"
		operation.Operation.Version++
		st.Operations["operation-after-migration"] = operation
		st.Executions["execution-after-migration"] = executionRecord{Execution: Execution{ID: "execution-after-migration",
			BoxID: boxID, OperationID: "operation-after-migration", State: "exited",
			Result: &guestapi.ExecResult{Stdout: "new local execution output"}}}
		return nil
	}); err != nil {
		t.Fatalf("local completion update required remote storage: %v", err)
	}
	if remote.revision != remoteRevision {
		t.Fatalf("local metadata writes changed remote revision: %d -> %d", remoteRevision, remote.revision)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}

	// A normal reopen must use only the initialized local database. No remote
	// fallback is available to hide missing metadata or stale object IDs.
	metadata, err = objectstorage.OpenMetadata(ctx, metadataDir, remote, false)
	if err != nil {
		t.Fatalf("reopen depended on unavailable remote storage: %v", err)
	}
	defer metadata.Close()
	store, err = OpenObjectStore(ctx, metadata)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if got := store.state.Boxes[boxID]; got.ClientID != "owner" || got.Box.OwnerKey != "owner-key" ||
		got.Box.ImportedImageID != imageID || !got.Staged || !got.RestoreComplete || got.RestoreArchiveID != archiveID || got.Box.OperationID != "" {
		t.Fatalf("Box identity or restore history was lost: %+v", got)
	}
	if got := store.state.Operations["operation-before-migration"].Operation; got.Status != "succeeded" || got.TargetID != boxID {
		t.Fatalf("pre-migration operation was lost: %+v", got)
	}
	if got := store.state.Operations["operation-after-migration"].Operation; got.Status != "succeeded" || got.TargetID != boxID || got.Version != 2 {
		t.Fatalf("local operation history was lost: %+v", got)
	}
	for _, key := range []string{"owner:resume:" + boxID + ":before-migration", "owner:resume:" + boxID + ":after-migration"} {
		if record := store.state.Keys[key]; record.OperationID == "" || record.Expired {
			t.Fatalf("idempotency key history was lost for %q: %+v", key, record)
		}
	}
	if got := store.state.Archives[archiveID]; got.ClientID != "owner" || got.Archive.ManifestVersion != 1 || got.Archive.SourceBoxID != boxID {
		t.Fatalf("archive metadata was lost: %+v", got)
	}
	if got := store.state.ImportedImages[imageID]; got.ImportedImage.ID != imageID || got.ImportedImage.Source != "registry.example.test/original:tag" {
		t.Fatalf("imported image metadata was lost: %+v", got)
	}
	for id, want := range map[string]string{
		"execution-before-migration": "persisted execution output",
		"execution-after-migration":  "new local execution output",
	} {
		execution := store.state.Executions[id]
		result, err := store.executionResult(ctx, execution)
		if err != nil || result.Result == nil || result.Result.Stdout != want {
			t.Fatalf("execution history %q was not available from local metadata: result=%+v err=%v", id, result.Result, err)
		}
	}
	if remote.revision != remoteRevision {
		t.Fatalf("reopen changed remote revision: %d -> %d", remoteRevision, remote.revision)
	}
	if _, _, _, err := remote.Get(ctx, assetKey); !errors.Is(err, objectstorage.ErrUnavailable) {
		t.Fatalf("remote fixture unexpectedly became available: %v", err)
	}
	if _, err := os.Stat(assetPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("archive asset appeared in local metadata after reopen: %v", err)
	}
}
