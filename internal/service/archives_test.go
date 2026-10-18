package service

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"cellbox.local/cellbox/internal/archivelimits"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/objectstorage"
)

type archiveTestEntry struct {
	name string
	kind byte
	body string
}

func makeArchiveTestData(t *testing.T, entries ...archiveTestEntry) []byte {
	t.Helper()
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		h := &tar.Header{Name: e.name, Typeflag: e.kind, Mode: 0640, Size: int64(len(e.body))}
		if e.kind == tar.TypeDir {
			h.Mode = 0750
			h.Size = 0
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if e.body != "" && e.kind != tar.TypeDir {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestValidateArchive(t *testing.T) {
	valid := makeArchiveTestData(t, archiveTestEntry{"dir/", tar.TypeDir, ""}, archiveTestEntry{"dir/run.sh", tar.TypeReg, "#!/bin/sh\n"})
	if err := validateArchive(bytes.NewReader(valid)); err != nil {
		t.Fatal(err)
	}
	cases := map[string][]archiveTestEntry{
		"traversal":      {{"../escape", tar.TypeReg, "x"}},
		"absolute":       {{"/etc/passwd", tar.TypeReg, "x"}},
		"missing parent": {{"dir/file", tar.TypeReg, "x"}},
		"duplicate":      {{"same", tar.TypeReg, "x"}, {"same", tar.TypeReg, "y"}},
		"symlink":        {{"link", tar.TypeSymlink, ""}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			if err := validateArchive(bytes.NewReader(makeArchiveTestData(t, entries...))); err == nil {
				t.Fatal("unsafe archive accepted")
			}
		})
	}
	if err := validateArchive(bytes.NewReader(valid[:len(valid)-4])); err == nil {
		t.Fatal("truncated gzip accepted")
	}
}

type archiveZeroReader struct{}

func (archiveZeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestValidateArchiveLargeEntryBoundary(t *testing.T) {
	// Exercise a payload just over the former 1 GiB limit without buffering it
	// in memory; a streamed all-zero payload compresses to a small fixture.
	for _, size := range []int64{(1 << 30) + 1, archivelimits.MaxEntryBytes + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			var body bytes.Buffer
			gz := gzip.NewWriter(&body)
			tw := tar.NewWriter(gz)
			if err := tw.WriteHeader(&tar.Header{Name: "large.bin", Typeflag: tar.TypeReg, Mode: 0640, Size: size}); err != nil {
				t.Fatal(err)
			}
			if size == (1<<30)+1 {
				if _, err := io.CopyN(tw, archiveZeroReader{}, size); err != nil {
					t.Fatal(err)
				}
				if err := tw.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := gz.Close(); err != nil {
				t.Fatal(err)
			}
			err := validateArchive(bytes.NewReader(body.Bytes()))
			if size == (1<<30)+1 && err != nil {
				t.Fatalf("entry just over 1 GiB must be accepted: %v", err)
			}
			if size > archivelimits.MaxEntryBytes && (err == nil || !strings.Contains(err.Error(), "content limit")) {
				t.Fatalf("oversized header must be rejected before reading its body: %v", err)
			}
		})
	}
}

func TestVerifyArchiveChecksumAndSize(t *testing.T) {
	data := makeArchiveTestData(t, archiveTestEntry{"file.txt", tar.TypeReg, "content"})
	name := filepath.Join(t.TempDir(), "archive.tar.gz")
	if err := os.WriteFile(name, data, 0600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sum := sha256.Sum256(data)
	a := Archive{Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	if err := verifyArchive(f, a); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(data))
	if _, err := f.Read(got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("verify did not rewind archive")
	}
	a.SHA256 = strings.Repeat("0", 64)
	if err := verifyArchive(f, a); err == nil {
		t.Fatal("wrong checksum accepted")
	}
	a.SHA256 = hex.EncodeToString(sum[:])
	a.Size++
	if err := verifyArchive(f, a); err == nil {
		t.Fatal("wrong size accepted")
	}
}

func TestOpenArchiveRejectsSymlink(t *testing.T) {
	p := filepath.Join(t.TempDir(), "archive.tar.gz")
	if err := os.Symlink("/etc/passwd", p); err != nil {
		t.Fatal(err)
	}
	if _, err := openArchive(p); err == nil {
		t.Fatal("symlinked archive accepted")
	}
}

func TestArchiveDeleteOwnershipAndActiveRestore(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	objects := &ledgerObjects{data: map[string][]byte{}}
	s := &Service{config: Config{DataDir: dir}, store: store, objects: objects}
	id := "arc-0123456789abcdef0123456789abcdef"
	payload := makeArchiveTestData(t, archiveTestEntry{"file", tar.TypeReg, "x"})
	if _, err := objects.Put(context.Background(), "archives/"+id+".tar.gz", bytes.NewReader(payload), int64(len(payload)), "*"); err != nil {
		t.Fatal(err)
	}
	err = store.Write(context.Background(), func(tx *storeTx) error {
		if err := tx.PutArchive(archiveRecord{Archive: Archive{ID: id}, ClientID: "owner"}); err != nil {
			return err
		}
		if err := tx.PutBox(boxRecord{Box: Box{ID: "box", OperationID: "op"}, RestoreArchiveID: id, ClientID: "owner"}); err != nil {
			return err
		}
		return tx.PutOperation(operationRecord{Operation: Operation{ID: "op", Kind: "restore", Status: "running"}, ClientID: "owner"})
	})
	if err != nil {
		t.Fatal(err)
	}
	deleteAs := func(client string) int {
		r := httptest.NewRequest("DELETE", "/v1/archives/"+id, nil)
		r.SetPathValue("id", id)
		r = r.WithContext(context.WithValue(r.Context(), clientContextKey{}, client))
		w := httptest.NewRecorder()
		s.deleteArchive(w, r)
		return w.Code
	}
	if got := deleteAs("other"); got != 404 {
		t.Fatalf("cross-client delete: %d", got)
	}
	if got := deleteAs("owner"); got != 409 {
		t.Fatalf("active restore delete: %d", got)
	}
	if err := store.Write(context.Background(), func(tx *storeTx) error {
		b, err := tx.Box("owner", "box")
		if err != nil {
			return err
		}
		b.RestoreComplete = true
		if err := tx.PutBox(b); err != nil {
			return err
		}
		op, err := tx.Operation("op")
		if err != nil {
			return err
		}
		op.Operation.Status = "succeeded"
		return tx.PutOperation(op)
	}); err != nil {
		t.Fatal(err)
	}
	if got := deleteAs("owner"); got != 204 {
		t.Fatalf("delete: %d", got)
	}
	if _, _, _, err := objects.Get(context.Background(), "archives/"+id+".tar.gz"); !errors.Is(err, objectstorage.ErrNotFound) {
		t.Fatalf("archive object remains: %v", err)
	}
}

func TestRunningArchiveCaptureIsRejected(t *testing.T) {
	f := newCoreFixture(t)
	useK3sProfile(t, f)
	_, source := f.createBox(t, "running-capture-source")
	objects := &ledgerObjects{data: map[string][]byte{}}
	f.service.objects = objects
	f.service.providers["resumable-k8s-pod"] = &suspendedArchiveTestProvider{fakeCoreProvider: f.provider, objects: objects}
	guestCalls := f.provider.guestCount()
	status, body := f.call(t, "POST", "/v1/boxes/"+source.ID+"/archives", testClientToken, "", nil)
	wantStatus(t, status, 409, body)
	if f.provider.guestCount() != guestCalls {
		t.Fatal("archive attempt contacted a running guest")
	}
	status, body = f.call(t, "POST", "/v1/boxes/"+source.ID+"/archives", testClientToken, "", map[string]bool{"diskOnly": true})
	wantStatus(t, status, 400, body)
}

func TestDockerArchiveCaptureIsUnsupported(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "docker-archive-source")
	status, body := f.call(t, "POST", "/v1/boxes/"+box.ID+"/archives", testClientToken, "", nil)
	wantStatus(t, status, 422, body)
}

func TestPortableArchiveRestoreAcrossImagesRequiresOptIn(t *testing.T) {
	f := newCoreFixture(t)
	useK3sProfile(t, f)
	const archiveID = "arc-11111111111111111111111111111111"
	payload := makeArchiveTestData(t, archiveTestEntry{"work.txt", tar.TypeReg, "portable"})
	objects := &ledgerObjects{data: map[string][]byte{}}
	f.service.objects = objects
	if _, err := objects.Put(context.Background(), "archives/"+archiveID+".tar.gz", bytes.NewReader(payload), int64(len(payload)), "*"); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(payload)
	archive := Archive{ID: archiveID, SourceBoxID: "deleted-source", ImageID: "registry.example/source@sha256:" + strings.Repeat("a", 64), Agent: f.config.Profiles[0].Guest.Agent, SHA256: hex.EncodeToString(h[:]), Size: int64(len(payload)), Portable: true}
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error {
		return tx.PutArchive(archiveRecord{Archive: archive, ClientID: "client-a"})
	}); err != nil {
		t.Fatal(err)
	}
	var restored atomic.Bool
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(200)
			return
		}
		if r.URL.Path == "/v1/restore" {
			body, readErr := io.ReadAll(r.Body)
			if readErr == nil && bytes.Equal(body, payload) {
				restored.Store(true)
				w.WriteHeader(204)
				return
			}
			http.Error(w, "invalid archive", 400)
			return
		}
		http.NotFound(w, r)
	}))
	defer guest.Close()
	f.provider.mu.Lock()
	f.provider.guestURL = guest.URL
	f.provider.mu.Unlock()
	templateID := f.template(t)
	request := map[string]any{"templateId": templateID, "ownerKey": "portable-target", "archiveId": archiveID}
	status, body := f.call(t, "POST", "/v1/boxes:restore", testClientToken, "restore-needs-optin", request)
	wantStatus(t, status, 409, body)
	request["acceptImageChange"] = true
	archive.Agent.UID++
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error {
		record, err := tx.Archive("client-a", archiveID)
		if err != nil {
			return err
		}
		record.Archive = archive
		return tx.PutArchive(record)
	}); err != nil {
		t.Fatal(err)
	}
	status, body = f.call(t, "POST", "/v1/boxes:restore", testClientToken, "restore-agent-mismatch", request)
	wantStatus(t, status, 409, body)
	archive.Agent = f.config.Profiles[0].Guest.Agent
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error {
		record, err := tx.Archive("client-a", archiveID)
		if err != nil {
			return err
		}
		record.Archive = archive
		return tx.PutArchive(record)
	}); err != nil {
		t.Fatal(err)
	}
	status, body = f.call(t, "POST", "/v1/boxes:restore", testClientToken, "restore-accepted", request)
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	f.waitOperation(t, op.ID, "succeeded")
	request["acceptImageChange"] = false
	status, body = f.call(t, "POST", "/v1/boxes:restore", testClientToken, "restore-accepted", request)
	wantStatus(t, status, 409, body)
	if !restored.Load() {
		t.Fatal("portable archive was not restored into selected target image")
	}
	b, err := f.service.store.BoxRecord(op.TargetID)
	if err != nil {
		t.Fatal(err)
	}
	if b.Box.RevisionID == "" || b.Box.Image != b.Profile.Image {
		t.Fatalf("restore did not retain the selected catalog revision: %+v", b)
	}
}

type suspendedArchiveTestProvider struct {
	*fakeCoreProvider
	objects   *ledgerObjects
	payload   []byte
	badDigest bool
	calls     int
}

func TestFailedArchiveCaptureUsesStoppedWorkspaceProvider(t *testing.T) {
	f := newCoreFixture(t)
	_, source := f.createBox(t, "failed-archive-source")
	objects := &ledgerObjects{data: map[string][]byte{}}
	provider := &suspendedArchiveTestProvider{fakeCoreProvider: f.provider, objects: objects,
		payload: makeArchiveTestData(t, archiveTestEntry{"src/", tar.TypeDir, ""}, archiveTestEntry{"src/file.txt", tar.TypeReg, "persisted"})}
	f.service.objects = objects
	f.service.providers["resumable-k8s-pod"] = provider
	if err := f.service.store.ChangeBox(context.Background(), source.ID, func(box *boxRecord) error {
		box.Box.Phase = "failed"
		box.Profile.Provider = "resumable-k8s-pod"
		box.Profile.PersistentHome = true
		box.Profile.Guest.Workspace = "/home/agent/workspace"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	guestCalls := f.provider.guestCount()
	status, body := f.call(t, "POST", "/v1/boxes/"+source.ID+"/archives", testClientToken, "", nil)
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	done := f.waitOperation(t, op.ID, "succeeded")
	archive, err := f.service.archiveForClient("client-a", done.Result["archiveId"])
	if err != nil || !archive.Portable || provider.calls != 1 || f.provider.guestCount() != guestCalls {
		t.Fatalf("failed stopped workspace capture: archive=%+v calls=%d guestCalls=%d/%d err=%v", archive, provider.calls, guestCalls, f.provider.guestCount(), err)
	}
}

func (p *suspendedArchiveTestProvider) CaptureWorkspaceArchive(ctx context.Context, _ boxprovider.Handle, id string) (int64, string, error) {
	p.calls++
	if _, err := p.objects.Put(ctx, "archives/"+id+".tar.gz", bytes.NewReader(p.payload), int64(len(p.payload)), "*"); err != nil {
		return 0, "", err
	}
	sum := sha256.Sum256(p.payload)
	digest := hex.EncodeToString(sum[:])
	if p.badDigest {
		digest = strings.Repeat("0", 64)
	}
	return int64(len(p.payload)), digest, nil
}

func TestSuspendedArchiveValidatesStoredNodeCaptureBeforePublishingMetadata(t *testing.T) {
	for _, badDigest := range []bool{false, true} {
		name := "valid node bytes"
		if badDigest {
			name = "corrupt node digest"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			store, err := OpenStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			objects := &ledgerObjects{data: map[string][]byte{}}
			payload := makeArchiveTestData(t, archiveTestEntry{"src/", tar.TypeDir, ""}, archiveTestEntry{"src/main.go", tar.TypeReg, "package main\n"})
			provider := &suspendedArchiveTestProvider{fakeCoreProvider: &fakeCoreProvider{}, objects: objects, payload: payload, badDigest: badDigest}
			s := &Service{config: Config{DataDir: dir}, store: store, objects: objects,
				providers: map[string]boxprovider.Provider{"resumable-k8s-pod": provider}}
			box := boxRecord{Box: Box{ID: "box-1", ProfileID: "profile-1", ImageID: "image-1", Phase: "suspended"}, ClientID: "client-1",
				Profile: Profile{ID: "profile-1", Provider: "resumable-k8s-pod", PersistentHome: true, Guest: guestapi.Config{Workspace: "/home/agent/workspace"}},
				Handle:  boxprovider.Handle{Provider: "resumable-k8s-pod", ID: "box-1"}}
			archive, captureErr := s.captureStoppedWorkspaceArchive(context.Background(), box)
			if provider.calls != 1 || provider.guestCount() != 0 {
				t.Fatalf("capture calls=%d guest calls=%d", provider.calls, provider.guestCount())
			}
			records, err := store.ArchiveRecords("client-1")
			if err != nil {
				t.Fatal(err)
			}
			archiveCount := len(records)
			if badDigest {
				if captureErr == nil || archiveCount != 0 {
					t.Fatalf("corrupt node bytes were published: err=%v archives=%d", captureErr, archiveCount)
				}
				return
			}
			if captureErr != nil {
				t.Fatal(captureErr)
			}
			if archive.ID == "" || archive.SourceBoxID != box.Box.ID || archive.ProfileID != box.Box.ProfileID || archive.ImageID != box.Box.ImageID || archive.Size != int64(len(payload)) {
				t.Fatalf("archive metadata does not describe source capture: %+v", archive)
			}
			if archiveCount != 1 {
				t.Fatalf("published %d archive metadata records", archiveCount)
			}
		})
	}
}

func (*suspendedArchiveTestProvider) Inspect(context.Context, boxprovider.Handle) (boxprovider.Observation, error) {
	return boxprovider.Observation{Phase: "suspended"}, nil
}

func TestSuspendedArchiveHTTPDispatchKeepsSourcePausedAndDoesNotReplayByKey(t *testing.T) {
	f := newCoreFixture(t)
	_, source := f.createBox(t, "suspended-capture-source")
	objects := &ledgerObjects{data: map[string][]byte{}}
	provider := &suspendedArchiveTestProvider{fakeCoreProvider: f.provider, objects: objects,
		payload: makeArchiveTestData(t, archiveTestEntry{"state.txt", tar.TypeReg, "latest workspace"})}
	f.service.objects = objects
	f.service.providers["resumable-k8s-pod"] = provider
	if err := f.service.store.ChangeBox(context.Background(), source.ID, func(box *boxRecord) error {
		box.Box.Phase = "suspended"
		box.Profile.Provider = "resumable-k8s-pod"
		box.Profile.PersistentHome = true
		box.Profile.Guest.Workspace = "/home/agent/workspace"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	guestCalls := f.provider.guestCount()
	status, body := f.call(t, "POST", "/v1/boxes/"+source.ID+"/archives", testClientToken, "ignored-key", nil)
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	completed := f.waitOperation(t, op.ID, "succeeded")
	if completed.Result["archiveId"] == "" {
		t.Fatal("capture operation returned no archive reference")
	}
	status, body = f.call(t, "POST", "/v1/boxes/"+source.ID+"/archives", testClientToken, "ignored-key", nil)
	wantStatus(t, status, 202, body)
	second := decodeResponse[Operation](t, body)
	if second.ID == op.ID {
		t.Fatal("Idempotency-Key unexpectedly replayed the previous operation")
	}
	f.waitOperation(t, second.ID, "succeeded")
	if provider.calls != 2 || f.provider.guestCount() != guestCalls {
		t.Fatal("a distinct request did not capture from stopped storage")
	}
	updated, err := f.service.rawBox(source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Box.Phase != "suspended" || updated.Box.OperationID != "" {
		t.Fatal("archive altered source lifecycle state or left it busy")
	}
}
