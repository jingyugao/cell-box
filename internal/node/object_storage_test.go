// SPDX-License-Identifier: Apache-2.0

package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/inventory"
	"cellbox.local/cellbox/internal/objectstorage"
	runtimebackend "cellbox.local/cellbox/internal/runtime"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type memoryObjects struct {
	objects          map[string][]byte
	putFailures      int
	putCalls         int
	deleteFailures   int
	deleteCalls      int
	deletedPrefix    string
	interruptedReads int
}

func (m *memoryObjects) Get(_ context.Context, key string) (io.ReadCloser, int64, string, error) {
	b, ok := m.objects[key]
	if !ok {
		return nil, 0, "", objectstorage.ErrNotFound
	}
	copyOf := append([]byte(nil), b...)
	if m.interruptedReads > 0 {
		m.interruptedReads--
		return io.NopCloser(&interruptedReadCloser{reader: bytes.NewReader(copyOf)}), int64(len(copyOf)), "etag", nil
	}
	return io.NopCloser(bytes.NewReader(copyOf)), int64(len(copyOf)), "etag", nil
}

type interruptedReadCloser struct {
	reader *bytes.Reader
	failed bool
}

func (r *interruptedReadCloser) Read(p []byte) (int, error) {
	if r.failed {
		return 0, io.ErrUnexpectedEOF
	}
	if len(p) > 16 {
		p = p[:16]
	}
	n, err := r.reader.Read(p)
	if err == nil {
		r.failed = true
	}
	return n, err
}
func (*interruptedReadCloser) Close() error { return nil }
func (m *memoryObjects) Put(_ context.Context, key string, body io.ReadSeeker, size int64, _ string) (string, error) {
	m.putCalls++
	if m.putFailures > 0 {
		m.putFailures--
		return "", errors.New("temporary upload failure")
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	b, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	if int64(len(b)) != size {
		return "", errors.New("length mismatch")
	}
	if m.objects == nil {
		m.objects = map[string][]byte{}
	}
	m.objects[key] = b
	return "etag", nil
}
func (m *memoryObjects) List(_ context.Context, prefix string) ([]string, error) {
	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/") + "/"
	}
	keys := make([]string, 0)
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}
func (m *memoryObjects) Delete(_ context.Context, key string) error {
	delete(m.objects, key)
	return nil
}
func (m *memoryObjects) DeleteIfMatch(_ context.Context, key, etag string) error {
	if _, ok := m.objects[key]; !ok {
		return objectstorage.ErrNotFound
	}
	if etag != "etag" {
		return objectstorage.ErrConflict
	}
	delete(m.objects, key)
	return nil
}
func (m *memoryObjects) DeletePrefix(_ context.Context, prefix string) error {
	m.deleteCalls++
	m.deletedPrefix = prefix
	if m.deleteFailures > 0 {
		m.deleteFailures--
		return errors.New("temporary delete failure")
	}
	for key := range m.objects {
		if strings.HasPrefix(key, strings.TrimSuffix(prefix, "/")+"/") {
			delete(m.objects, key)
		}
	}
	return nil
}

func testCheckpoint(t *testing.T, base, owner, snapshot, spec, imageID, runsc string) string {
	t.Helper()
	path, err := SnapshotPath(base, owner, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(path, "checkpoint.img"), []byte("checkpoint contents"), 0600); err != nil {
		t.Fatal(err)
	}
	hash, err := Digest(runsc)
	if err != nil {
		t.Fatal(err)
	}
	if err = Seal(path, Manifest{OwnerUID: owner, SpecHash: spec, ImageID: imageID, RunscHash: hash}); err != nil {
		t.Fatal(err)
	}
	return path
}

func testWorkload(owner, snapshot, spec, image string) *api.ResumablePod {
	return &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: typesUID(owner)}, Spec: api.Spec{Container: core.Container{Image: image}}, Status: api.Status{SpecHash: spec, Snapshot: snapshot}}
}

func TestCheckpointUploadRetryDoesNotRepeatRunscCheckpoint(t *testing.T) {
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	objects := &memoryObjects{putFailures: 1}
	workload := testWorkload("owner", "snapshot", "spec", "image")
	path := testCheckpoint(t, base, "owner", "snapshot", "spec", "image-id", runsc)
	b := &Backend{Base: base, Runsc: runsc, Objects: objects}
	if err := b.Checkpoint(context.Background(), workload, nil); err == nil {
		t.Fatal("expected upload failure")
	}
	if _, err := os.Stat(filepath.Join(path, "checkpoint.img")); err != nil {
		t.Fatal("local sealed checkpoint must remain after upload failure:", err)
	}
	if err := b.Checkpoint(context.Background(), workload, nil); err != nil {
		t.Fatal("retry must upload the sealed checkpoint without CRI/runsc access:", err)
	}
	if len(objects.objects) != 1 {
		t.Fatalf("expected one uploaded archive, got %d", len(objects.objects))
	}
}

func TestPrepareDownloadsAndVerifiesColdCacheSnapshot(t *testing.T) {
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	objects := &memoryObjects{objects: map[string][]byte{}}
	sourceBase := t.TempDir()
	sourcePath := testCheckpoint(t, sourceBase, "owner", "snapshot", "spec", "image-id", runsc)
	workload := testWorkload("owner", "snapshot", "spec", "image")
	source := &Backend{Base: sourceBase, Runsc: runsc, Objects: objects}
	if err := source.uploadSnapshot(context.Background(), workload, sourcePath); err != nil {
		t.Fatal(err)
	}
	b := &Backend{Base: base, Runsc: runsc, Objects: objects, Images: imageClient{id: "image-id"}}
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod-uid", Namespace: "ns", Name: "pod"}}
	if err := b.Prepare(context.Background(), workload, pod); err != nil {
		t.Fatal("cold cache restore should download and verify:", err)
	}
	if objects.putCalls != 1 {
		t.Fatalf("restore should not re-upload an already committed snapshot, puts=%d", objects.putCalls)
	}
	path, _ := SnapshotPath(base, "owner", "snapshot")
	if _, err := Verify(path, "owner", "spec", runsc); err != nil {
		t.Fatal("downloaded checkpoint did not verify:", err)
	}
	if _, err := os.Stat(filepath.Join(base, "tickets", "pod-uid.json")); err != nil {
		t.Fatal("ticket missing after validated restore:", err)
	}
}

func TestCheckpointAdoptsRemoteCommitWithoutRepeatingRunscSave(t *testing.T) {
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	objects := &memoryObjects{objects: map[string][]byte{}}
	sourceBase := t.TempDir()
	path := testCheckpoint(t, sourceBase, "owner", "snapshot", "spec", "image-id", runsc)
	workload := testWorkload("owner", "snapshot", "spec", "image")
	if err := (&Backend{Base: sourceBase, Runsc: runsc, Objects: objects}).uploadSnapshot(context.Background(), workload, path); err != nil {
		t.Fatal(err)
	}
	// A nil CRI client makes any attempt to run a new checkpoint fail. The
	// committed remote archive must be adopted and cached instead.
	b := &Backend{Base: base, Runsc: runsc, Objects: objects, Images: imageClient{id: "image-id"}}
	if err := b.Checkpoint(context.Background(), workload, nil); err != nil {
		t.Fatal("remote committed snapshot should be adopted:", err)
	}
	local, _ := SnapshotPath(base, "owner", "snapshot")
	if _, err := Verify(local, "owner", "spec", runsc); err != nil {
		t.Fatal("adopted checkpoint did not verify:", err)
	}
}

func TestPrepareRejectsCorruptRemoteSnapshot(t *testing.T) {
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	objects := &memoryObjects{objects: map[string][]byte{}}
	sourceBase := t.TempDir()
	path := testCheckpoint(t, sourceBase, "owner", "snapshot", "spec", "image-id", runsc)
	workload := testWorkload("owner", "snapshot", "spec", "image")
	if err := (&Backend{Base: sourceBase, Runsc: runsc, Objects: objects}).uploadSnapshot(context.Background(), workload, path); err != nil {
		t.Fatal(err)
	}
	archive := objects.objects["checkpoints/owner/snapshot.tar"]
	corruptContent := []byte("checkpoint contents")
	position := bytes.Index(archive, corruptContent)
	if position < 0 {
		t.Fatal("could not find checkpoint payload in archive")
	}
	archive[position+len(corruptContent)-1] = 'x'
	objects.objects["checkpoints/owner/snapshot.tar"] = archive
	b := &Backend{Base: base, Runsc: runsc, Objects: objects, Images: imageClient{id: "image-id"}}
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod-uid", Namespace: "ns", Name: "pod"}}
	if err := b.Prepare(context.Background(), workload, pod); err == nil {
		t.Fatal("corrupt remote snapshot was accepted")
	}
	if _, err := os.Stat(filepath.Join(base, "tickets", "pod-uid.json")); !os.IsNotExist(err) {
		t.Fatal("ticket created for corrupt remote snapshot")
	}
}

func TestPrepareRetriesInterruptedRemoteReadWithoutTicketOrCache(t *testing.T) {
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	objects := &memoryObjects{objects: map[string][]byte{}}
	sourceBase := t.TempDir()
	path := testCheckpoint(t, sourceBase, "owner", "snapshot", "spec", "image-id", runsc)
	workload := testWorkload("owner", "snapshot", "spec", "image")
	if err := (&Backend{Base: sourceBase, Runsc: runsc, Objects: objects}).uploadSnapshot(context.Background(), workload, path); err != nil {
		t.Fatal(err)
	}
	objects.interruptedReads = 1
	b := &Backend{Base: base, Runsc: runsc, Objects: objects, Images: imageClient{id: "image-id"}}
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod-uid", Namespace: "ns", Name: "pod"}}
	err := b.Prepare(context.Background(), workload, pod)
	if !errors.Is(err, runtimebackend.ErrRetryableStorage) {
		t.Fatalf("truncated transport read should be retryable, got %v", err)
	}
	local, _ := SnapshotPath(base, "owner", "snapshot")
	if _, err = os.Lstat(local); !os.IsNotExist(err) {
		t.Fatal("partial download became a final cache directory")
	}
	if _, err = os.Stat(filepath.Join(base, "tickets", "pod-uid.json")); !os.IsNotExist(err) {
		t.Fatal("ticket created before complete snapshot verification")
	}
	if err = b.Prepare(context.Background(), workload, pod); err != nil {
		t.Fatal("complete retry did not restore snapshot:", err)
	}
	if _, err = Verify(local, "owner", "spec", runsc); err != nil {
		t.Fatal("retried snapshot did not verify:", err)
	}
}

func TestForgetRetriesScopedRemoteDelete(t *testing.T) {
	base := t.TempDir()
	objects := &memoryObjects{objects: map[string][]byte{"checkpoints/owner/snapshot.tar": []byte("archive"), "checkpoints/other/snapshot.tar": []byte("other")}, deleteFailures: 1}
	b := &Backend{Base: base, Objects: objects}
	workload := testWorkload("owner", "snapshot", "spec", "image")
	ownerPath := filepath.Join(base, "workloads", "owner")
	if err := os.MkdirAll(ownerPath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ownerPath, "local"), []byte("cache"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := b.Forget(context.Background(), workload); err == nil {
		t.Fatal("expected remote delete failure")
	}
	if _, err := os.Stat(ownerPath); err != nil {
		t.Fatal("local cache must remain while remote deletion is unresolved:", err)
	}
	if err := b.Forget(context.Background(), workload); err != nil {
		t.Fatal("delete retry failed:", err)
	}
	if objects.deletedPrefix != "checkpoints/owner" {
		t.Fatalf("unexpected delete prefix %q", objects.deletedPrefix)
	}
	if _, ok := objects.objects["checkpoints/other/snapshot.tar"]; !ok {
		t.Fatal("delete escaped owner prefix")
	}
	if _, err := os.Stat(ownerPath); !os.IsNotExist(err) {
		t.Fatal("local cache was not cleared after remote deletion")
	}
}

func TestForgetSkipsRepeatedRemoteCleanupButCheckpointInvalidatesCache(t *testing.T) {
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	objects := &memoryObjects{objects: map[string][]byte{"checkpoints/owner/old.tar": []byte("old"), "checkpoints/other/keep.tar": []byte("keep")}}
	b := &Backend{Base: base, Runsc: runsc, Objects: objects}
	workload := testWorkload("owner", "snapshot", "spec", "image")
	workload.Status.Cycle = 4
	if err := b.Forget(context.Background(), workload); err != nil {
		t.Fatal(err)
	}
	if err := b.Forget(context.Background(), workload); err != nil {
		t.Fatal(err)
	}
	if objects.deleteCalls != 1 {
		t.Fatalf("repeated running cleanup should perform one remote delete, got %d", objects.deleteCalls)
	}

	// A checkpoint can begin in the same lifecycle cycle. Its successful upload
	// clears the prior cleanup cache so finalizer cleanup still deletes it.
	workload.Status.Snapshot = "checkpoint-4"
	sourceBase := t.TempDir()
	sourcePath := testCheckpoint(t, sourceBase, "owner", workload.Status.Snapshot, "spec", "image-id", runsc)
	archiveStore := &memoryObjects{objects: map[string][]byte{}}
	if err := (&Backend{Base: sourceBase, Runsc: runsc, Objects: archiveStore}).uploadSnapshot(context.Background(), workload, sourcePath); err != nil {
		t.Fatal(err)
	}
	objects.objects["checkpoints/owner/checkpoint-4.tar"] = archiveStore.objects["checkpoints/owner/checkpoint-4.tar"]
	b.Images = imageClient{id: "image-id"}
	if err := b.Checkpoint(context.Background(), workload, nil); err != nil {
		t.Fatal("same-cycle remote checkpoint adoption failed:", err)
	}
	if err := b.Forget(context.Background(), workload); err != nil {
		t.Fatal("same-cycle cleanup should delete the newly adopted checkpoint:", err)
	}
	if objects.deleteCalls != 2 {
		t.Fatalf("checkpoint adoption did not invalidate cleanup cache, calls=%d", objects.deleteCalls)
	}
	if _, exists := objects.objects["checkpoints/owner/checkpoint-4.tar"]; exists {
		t.Fatal("newly adopted checkpoint survived cleanup")
	}
	workload.DeletionTimestamp = &meta.Time{Time: time.Now()}
	if err := b.Forget(context.Background(), workload); err != nil {
		t.Fatal("deleting CR must perform remote cleanup:", err)
	}
	if objects.deleteCalls != 3 {
		t.Fatalf("CR deletion skipped remote cleanup, calls=%d", objects.deleteCalls)
	}
	if _, exists := objects.objects["checkpoints/owner/checkpoint-4.tar"]; exists {
		t.Fatal("checkpoint object survived deleting CR cleanup")
	}
	if _, exists := objects.objects["checkpoints/other/keep.tar"]; !exists {
		t.Fatal("cleanup escaped owner prefix")
	}
}

func TestSuspendedInventoryPublishesAfterSnapshotAndRepairsAfterRestart(t *testing.T) {
	owner, snapshot := "owner-123", "checkpoint-4"
	box, err := json.Marshal(map[string]any{"id": "box-123", "phase": "running", "generation": 3, "image": "prepared@sha256:abc"})
	if err != nil {
		t.Fatal(err)
	}
	annotation, err := json.Marshal(inventory.Record{ClientID: "client-one", Box: box})
	if err != nil {
		t.Fatal(err)
	}
	workload := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: typesUID(owner), Labels: map[string]string{
		"cellbox.local/box-id": "box-123", inventory.ClientLabel: inventory.ClientValue("client-one"),
	}, Annotations: map[string]string{inventory.Annotation: string(annotation)}},
		Spec: api.Spec{DesiredState: "Suspended"}, Status: api.Status{Phase: "Suspended", Snapshot: snapshot, Cycle: 4}}
	objects := &memoryObjects{objects: map[string][]byte{"checkpoints/" + owner + "/" + snapshot + ".tar": []byte("archive")}}
	backend := &Backend{Objects: objects}
	if err = backend.SyncInventory(context.Background(), workload); err != nil {
		t.Fatal("publish suspended inventory:", err)
	}
	indexKey := "checkpoints/" + owner + "/metadata.json"
	var published inventory.Record
	if err = json.Unmarshal(objects.objects[indexKey], &published); err != nil {
		t.Fatal("invalid published index:", err)
	}
	if published.ClientID != "client-one" || published.RuntimeID != owner || published.Snapshot != snapshot || published.NodeName != "" {
		t.Fatalf("incomplete inventory record: %+v", published)
	}
	var publishedBox map[string]json.RawMessage
	if err = json.Unmarshal(published.Box, &publishedBox); err != nil {
		t.Fatal(err)
	}
	var phase string
	if err = json.Unmarshal(publishedBox["phase"], &phase); err != nil || phase != "suspended" {
		t.Fatalf("inventory phase = %q, err=%v", phase, err)
	}
	if objects.putCalls != 1 {
		t.Fatalf("initial inventory publish made %d puts", objects.putCalls)
	}
	if err = backend.SyncInventory(context.Background(), workload); err != nil || objects.putCalls != 1 {
		t.Fatalf("unchanged inventory should be cached, puts=%d err=%v", objects.putCalls, err)
	}

	// A process restart has an empty cache and must reconstruct a missing index.
	delete(objects.objects, indexKey)
	restarted := &Backend{Objects: objects}
	if err = restarted.SyncInventory(context.Background(), workload); err != nil {
		t.Fatal("restart did not repair index:", err)
	}
	if _, ok := objects.objects[indexKey]; !ok {
		t.Fatal("suspended inventory index was not repaired")
	}

	// A resume updates the lifecycle phase but retains the checkpoint until Forget.
	resume := workload.DeepCopy()
	resume.Spec.DesiredState = "Running"
	resume.Status.Phase = "Restoring"
	resume.Status.Cycle++
	if err = restarted.SyncInventory(context.Background(), resume); err != nil {
		t.Fatal("update index before restore:", err)
	}
	if _, ok := objects.objects[indexKey]; !ok {
		t.Fatal("resume removed the checkpoint index before Forget")
	}
	published = inventory.Record{}
	if err = json.Unmarshal(objects.objects[indexKey], &published); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(published.Box, &publishedBox); err != nil {
		t.Fatal(err)
	}
	phase = ""
	if err = json.Unmarshal(publishedBox["phase"], &phase); err != nil || phase != "resuming" {
		t.Fatalf("resume index phase = %q, err=%v", phase, err)
	}
	if err = restarted.Forget(context.Background(), resume); err != nil {
		t.Fatal("running Forget did not remove checkpoint index:", err)
	}
	if _, ok := objects.objects[indexKey]; ok {
		t.Fatal("running Forget left checkpoint index behind")
	}
}

func TestCheckpointingInventoryPublishesBeforeArchiveUpload(t *testing.T) {
	owner := "owner-789"
	box := json.RawMessage(`{"id":"box-789","phase":"running"}`)
	annotation, err := json.Marshal(inventory.Record{ClientID: "client-three", Box: box})
	if err != nil {
		t.Fatal(err)
	}
	workload := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: typesUID(owner), Labels: map[string]string{
		"cellbox.local/box-id": "box-789", inventory.ClientLabel: inventory.ClientValue("client-three"),
	}, Annotations: map[string]string{inventory.Annotation: string(annotation)}},
		Spec: api.Spec{DesiredState: "Suspended"}, Status: api.Status{Phase: "Checkpointing", Snapshot: "checkpoint-9", Cycle: 9}}
	objects := &memoryObjects{objects: map[string][]byte{}}
	if err = (&Backend{Objects: objects}).SyncInventory(context.Background(), workload); err != nil {
		t.Fatal("publish checkpointing phase before archive upload:", err)
	}
	if _, ok := objects.objects["checkpoints/"+owner+"/checkpoint-9.tar"]; ok {
		t.Fatal("checkpointing phase unexpectedly required or created the archive")
	}
	var record inventory.Record
	if err = json.Unmarshal(objects.objects["checkpoints/"+owner+"/metadata.json"], &record); err != nil {
		t.Fatal("checkpointing index missing:", err)
	}
	var metadata map[string]json.RawMessage
	if err = json.Unmarshal(record.Box, &metadata); err != nil {
		t.Fatal(err)
	}
	var phase string
	if err = json.Unmarshal(metadata["phase"], &phase); err != nil || phase != "checkpointing" {
		t.Fatalf("index phase = %q, err=%v", phase, err)
	}

	for _, state := range []struct {
		phase string
		want  string
	}{
		{phase: "Failed", want: "failed"},
		{phase: "Deleting", want: "deleting"},
	} {
		copy := workload.DeepCopy()
		copy.Status.Phase = state.phase
		if state.phase == "Deleting" {
			copy.DeletionTimestamp = &meta.Time{Time: time.Now()}
		}
		if err = (&Backend{Objects: objects}).SyncInventory(context.Background(), copy); err != nil {
			t.Fatalf("publish %s index: %v", state.phase, err)
		}
		var record inventory.Record
		if err = json.Unmarshal(objects.objects["checkpoints/"+owner+"/metadata.json"], &record); err != nil {
			t.Fatal(err)
		}
		var metadata map[string]json.RawMessage
		if err = json.Unmarshal(record.Box, &metadata); err != nil {
			t.Fatal(err)
		}
		var actual string
		if err = json.Unmarshal(metadata["phase"], &actual); err != nil || actual != state.want {
			t.Fatalf("%s index phase = %q, err=%v", state.phase, actual, err)
		}
	}
}

func TestSuspendedInventoryRequiresDurableSnapshot(t *testing.T) {
	owner := "owner-456"
	box := json.RawMessage(`{"id":"box-456"}`)
	annotation, err := json.Marshal(inventory.Record{ClientID: "client-two", Box: box})
	if err != nil {
		t.Fatal(err)
	}
	workload := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: typesUID(owner), Labels: map[string]string{
		"cellbox.local/box-id": "box-456", inventory.ClientLabel: inventory.ClientValue("client-two"),
	}, Annotations: map[string]string{inventory.Annotation: string(annotation)}},
		Spec: api.Spec{DesiredState: "Suspended"}, Status: api.Status{Phase: "Suspended", Snapshot: "checkpoint-1"}}
	backend := &Backend{Objects: &memoryObjects{objects: map[string][]byte{}}}
	if err = backend.SyncInventory(context.Background(), workload); err == nil {
		t.Fatal("published inventory without a durable snapshot")
	}
	if len(backend.Objects.(*memoryObjects).objects) != 0 {
		t.Fatal("missing snapshot produced an inventory object")
	}
}

func typesUID(s string) types.UID { return types.UID(s) }
