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
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type memoryObjects struct {
	objects          map[string][]byte
	putFailures      int
	putCalls         int
	getCalls         int
	deleteFailures   int
	deleteCalls      int
	deletedPrefix    string
	interruptedReads int
	onPut            func(io.ReadSeeker)
	onRead           func()
}

func (m *memoryObjects) Get(_ context.Context, key string) (io.ReadCloser, int64, string, error) {
	m.getCalls++
	b, ok := m.objects[key]
	if !ok {
		return nil, 0, "", objectstorage.ErrNotFound
	}
	copyOf := append([]byte(nil), b...)
	if m.interruptedReads > 0 {
		m.interruptedReads--
		return io.NopCloser(&interruptedReadCloser{reader: bytes.NewReader(copyOf)}), int64(len(copyOf)), "etag", nil
	}
	return &observedArchiveReader{ReadCloser: io.NopCloser(bytes.NewReader(copyOf)), onRead: m.onRead}, int64(len(copyOf)), "etag", nil
}

type observedArchiveReader struct {
	io.ReadCloser
	onRead func()
}

func (r *observedArchiveReader) Read(p []byte) (int, error) {
	if r.onRead != nil {
		r.onRead()
		r.onRead = nil
	}
	return r.ReadCloser.Read(p)
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
	if m.onPut != nil {
		m.onPut(body)
	}
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

func testCheckpoint(t *testing.T, base, owner, snapshot, spec, imageID, runsc string, noRoot ...bool) string {
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
	if err = Seal(path, Manifest{OwnerUID: owner, SpecHash: spec, ImageID: imageID, RunscHash: hash, NoRoot: len(noRoot) > 0 && noRoot[0]}); err != nil {
		t.Fatal(err)
	}
	return path
}

func testWorkload(owner, snapshot, spec, image string) *api.ResumablePod {
	return &api.ResumablePod{ObjectMeta: meta.ObjectMeta{UID: typesUID(owner)}, Spec: api.Spec{Container: core.Container{Image: image}}, Status: api.Status{SpecHash: spec, Snapshot: snapshot}}
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
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	workload.Status.SpecHash = "spec"
	testCheckpoint(t, base, owner, snapshot, "spec", "image-id", runsc)
	checkpoints := &memoryCheckpointStore{}
	backend := &Backend{Base: base, Runsc: runsc, Checkpoints: checkpoints}
	if err = backend.SyncInventory(context.Background(), workload); err != nil {
		t.Fatal("publish suspended inventory:", err)
	}
	published, err := checkpoints.GetCheckpoint(context.Background(), owner)
	if err != nil {
		t.Fatal("read published inventory:", err)
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
	if err = backend.SyncInventory(context.Background(), workload); err != nil {
		t.Fatalf("repeat inventory sync failed: %v", err)
	}

	// The database record survives a process restart; the new backend needs no cache reconstruction.
	restarted := &Backend{Base: base, Runsc: runsc, Checkpoints: checkpoints}
	if err = restarted.SyncInventory(context.Background(), workload); err != nil {
		t.Fatal("restart inventory sync failed:", err)
	}

	// Resume state belongs to the CR/API; the durable checkpoint descriptor
	// stays unchanged until invalidation, with no redundant OSS write.
	resume := workload.DeepCopy()
	resume.Spec.DesiredState = "Running"
	resume.Status.Phase = "Restoring"
	resume.Status.Cycle++
	if err = restarted.SyncInventory(context.Background(), resume); err != nil {
		t.Fatal("update index before restore:", err)
	}
	published, err = checkpoints.GetCheckpoint(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(published.Box, &publishedBox); err != nil {
		t.Fatal(err)
	}
	phase = ""
	if err = json.Unmarshal(publishedBox["phase"], &phase); err != nil || phase != "suspended" {
		t.Fatalf("resume index phase = %q, err=%v", phase, err)
	}
	if err = restarted.Forget(context.Background(), resume); err != nil {
		t.Fatal("running Forget did not remove checkpoint index:", err)
	}
	if _, err = checkpoints.GetCheckpoint(context.Background(), owner); !errors.Is(err, inventory.ErrNotFound) {
		t.Fatalf("running Forget left checkpoint index behind: %v", err)
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
	checkpoints := &memoryCheckpointStore{}
	if err = (&Backend{Checkpoints: checkpoints}).SyncInventory(context.Background(), workload); err != nil {
		t.Fatal("publish checkpointing phase before archive upload:", err)
	}
	record, err := checkpoints.GetCheckpoint(context.Background(), owner)
	if err != nil {
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
		if err = (&Backend{Checkpoints: checkpoints}).SyncInventory(context.Background(), copy); err != nil {
			t.Fatalf("publish %s index: %v", state.phase, err)
		}
		record, err := checkpoints.GetCheckpoint(context.Background(), owner)
		if err != nil {
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

func typesUID(s string) types.UID { return types.UID(s) }

func TestCheckpointAndPrepareRemainLocalAcrossRestart(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	bin := filepath.Join(base, "runsc")
	if err := os.WriteFile(bin, []byte("runtime"), 0700); err != nil {
		t.Fatal(err)
	}
	objects := &memoryObjects{}
	w := testWorkload("owner", "snapshot", "spec", "image")
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod", Name: "pod", Namespace: "test"}}
	path := testCheckpoint(t, base, "owner", "snapshot", "spec", "image-id", bin)
	b := &Backend{Base: base, Runsc: bin, Objects: objects, Images: imageClient{id: "image-id"}}
	if err := b.Checkpoint(ctx, w, nil); err != nil {
		t.Fatal(err)
	}
	gets, puts := objects.getCalls, objects.putCalls
	if gets != 0 || puts != 0 {
		t.Fatal("checkpoint accessed object storage")
	}
	if err := b.Prepare(ctx, w, p); err != nil {
		t.Fatal(err)
	}
	if objects.getCalls != gets || objects.putCalls != puts {
		t.Fatal("warm prepare accessed OSS")
	}
	if _, err := VerifyPrepared(path, "owner", "spec", bin); err != nil {
		t.Fatal(err)
	}
	restarted := &Backend{Base: base, Runsc: bin, Objects: objects, Images: imageClient{id: "image-id"}}
	if err := restarted.Prepare(ctx, w, p); err != nil {
		t.Fatal(err)
	}
	if objects.getCalls != gets || objects.putCalls != puts {
		t.Fatal("restart accessed object storage for a local checkpoint")
	}
	if err := restarted.Prepare(ctx, w, p); err != nil {
		t.Fatal(err)
	}
	if objects.getCalls != gets {
		t.Fatal("repeated prepare re-read OSS")
	}
	for _, change := range []string{"file", "runtime"} {
		target := filepath.Join(path, "checkpoint.img")
		if change == "runtime" {
			target = bin
		}
		before, _ := os.Stat(target)
		data, _ := os.ReadFile(target)
		data[0] ^= 1
		if err := os.WriteFile(target, data, before.Mode()); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(target, before.ModTime(), before.ModTime()); err != nil {
			t.Fatal(err)
		}
		if _, err := VerifyPrepared(path, "owner", "spec", bin); err == nil {
			t.Fatal("changed file accepted", change)
		}
		if _, err := VerifyCached(path, "owner", "spec", bin); err == nil {
			t.Fatal("corrupt file revalidated", change)
		}
		data[0] ^= 1
		os.WriteFile(target, data, before.Mode())
		if _, err := VerifyCached(path, "owner", "spec", bin); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPrepareMissingLocalCheckpointNeverDownloadsRemoteCopy(t *testing.T) {
	base := t.TempDir()
	objects := &memoryObjects{objects: map[string][]byte{"checkpoints/owner/snapshot.tar": []byte("legacy remote snapshot")}}
	b := &Backend{Base: base, Objects: objects}
	if _, err := b.prepareSnapshot(context.Background(), testWorkload("owner", "snapshot", "spec", "image")); err == nil {
		t.Fatal("missing local checkpoint accepted")
	}
	if objects.getCalls != 0 || objects.putCalls != 0 {
		t.Fatal("restore accessed object storage")
	}
}

func TestForgetOnlyDeletesInventoryAndLocalOwnerFiles(t *testing.T) {
	base := t.TempDir()
	objects := &memoryObjects{objects: map[string][]byte{
		"checkpoints/owner/snapshot.tar":  []byte("legacy"),
		"checkpoints/other/metadata.json": []byte("other"),
	}}
	checkpoints := &memoryCheckpointStore{records: map[string]inventory.Record{
		"owner": {RuntimeID: "owner", Snapshot: "snapshot"},
		"other": {RuntimeID: "other", Snapshot: "other-snapshot"},
	}}
	path := filepath.Join(base, "workloads", "owner")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	b := &Backend{Base: base, Objects: objects, Checkpoints: checkpoints}
	if err := b.Forget(context.Background(), testWorkload("owner", "snapshot", "spec", "image")); err != nil {
		t.Fatal(err)
	}
	if _, err := checkpoints.GetCheckpoint(context.Background(), "owner"); !errors.Is(err, inventory.ErrNotFound) {
		t.Fatal("inventory remains")
	}
	if len(objects.objects) != 2 || objects.deleteCalls != 0 {
		t.Fatal("cleanup scanned or deleted checkpoint payloads")
	}
	if _, err := checkpoints.GetCheckpoint(context.Background(), "other"); err != nil {
		t.Fatal("Forget deleted another runtime's checkpoint inventory:", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("local checkpoint remains", err)
	}
}
