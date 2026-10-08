package service

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
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

func TestValidateArchiveOneGiBEntryBoundary(t *testing.T) {
	for _, size := range []int64{1 << 30, (1 << 30) + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			var body bytes.Buffer
			gz := gzip.NewWriter(&body)
			tw := tar.NewWriter(gz)
			if err := tw.WriteHeader(&tar.Header{Name: "large.bin", Typeflag: tar.TypeReg, Mode: 0640, Size: size}); err != nil {
				t.Fatal(err)
			}
			if size == 1<<30 {
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
			if size == 1<<30 && err != nil {
				t.Fatalf("1 GiB must be accepted: %v", err)
			}
			if size > 1<<30 && (err == nil || !strings.Contains(err.Error(), "content limit")) {
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

func TestArchivePathCannotEscape(t *testing.T) {
	dir := t.TempDir()
	for _, id := range []string{"../secret", "arc-x", "arc-0123456789abcdef0123456789abcdef/../x"} {
		if _, err := archivePath(dir, id); err == nil {
			t.Errorf("accepted ID %q", id)
		}
	}
	good := "arc-0123456789abcdef0123456789abcdef"
	p, err := archivePath(dir, good)
	if err != nil || p != filepath.Join(dir, good+".tar.gz") {
		t.Fatalf("valid path: %q %v", p, err)
	}
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
	s := &Service{config: Config{DataDir: dir}, store: store}
	archiveDir, err := s.archivesDir()
	if err != nil {
		t.Fatal(err)
	}
	id := "arc-0123456789abcdef0123456789abcdef"
	name, err := archivePath(archiveDir, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, makeArchiveTestData(t, archiveTestEntry{"file", tar.TypeReg, "x"}), 0600); err != nil {
		t.Fatal(err)
	}
	err = store.Update(func(st *State) error {
		st.Archives[id] = archiveRecord{Archive: Archive{ID: id}, ClientID: "owner"}
		st.Boxes["box"] = boxRecord{Box: Box{ID: "box", OperationID: "op"}, RestoreArchiveID: id}
		st.Operations["op"] = operationRecord{Operation: Operation{ID: "op", Kind: "restore", Status: "running"}, ClientID: "owner"}
		return nil
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
	if err := store.Update(func(st *State) error {
		b := st.Boxes["box"]
		b.RestoreComplete = true
		st.Boxes["box"] = b
		op := st.Operations["op"]
		op.Operation.Status = "succeeded"
		st.Operations["op"] = op
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := deleteAs("owner"); got != 204 {
		t.Fatalf("delete: %d", got)
	}
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Fatalf("archive file remains: %v", err)
	}
}

func TestCaptureSurvivesBoxDeletionAndRestores(t *testing.T) {
	f := newCoreFixture(t)
	_, source := f.createBox(t, "source-create")
	payload := makeArchiveTestData(t, archiveTestEntry{"work.txt", tar.TypeReg, "durable"})
	var restored atomic.Bool
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			http.Error(w, "unauthorized", 401)
			return
		}
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(200)
		case "/v1/archive":
			w.Header().Set("Content-Type", "application/gzip")
			_, _ = w.Write(payload)
		case "/v1/restore":
			body, err := io.ReadAll(r.Body)
			if err != nil || !bytes.Equal(body, payload) {
				http.Error(w, "wrong restore body", 400)
				return
			}
			restored.Store(true)
			w.WriteHeader(204)
		default:
			http.NotFound(w, r)
		}
	}))
	defer guest.Close()
	f.provider.mu.Lock()
	f.provider.guestURL = guest.URL
	f.provider.mu.Unlock()
	status, body := f.call(t, "POST", "/v1/boxes/"+source.ID+"/archives", testClientToken, "capture-once", nil)
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	done := f.waitOperation(t, op.ID, "succeeded")
	archiveID := done.Result["archiveId"]
	if archiveID == "" {
		t.Fatal("capture omitted archive ID")
	}
	archive, err := f.service.archiveForClient("client-a", archiveID)
	if err != nil || !archive.Portable {
		t.Fatalf("validated native workspace archive is not portable: %+v %v", archive, err)
	}
	status, body = f.call(t, "GET", "/v1/archives/"+archiveID, otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "GET", "/v1/archives/"+archiveID+"/content", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if !bytes.Equal(body, payload) {
		t.Fatal("download differs from captured workspace")
	}
	status, body = f.call(t, "POST", "/v1/boxes/"+source.ID+":destroy", testClientToken, "destroy-source", nil)
	wantStatus(t, status, 202, body)
	destroy := decodeResponse[Operation](t, body)
	f.waitOperation(t, destroy.ID, "succeeded")
	status, body = f.call(t, "GET", "/v1/archives/"+archiveID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	status, body = f.call(t, "POST", "/v1/boxes:restore", testClientToken, "restore-once", map[string]string{"profileId": "profile-a", "ownerKey": "restored", "archiveId": archiveID})
	wantStatus(t, status, 202, body)
	restore := decodeResponse[Operation](t, body)
	f.waitOperation(t, restore.ID, "succeeded")
	if !restored.Load() {
		t.Fatal("archive was not sent to staged guest")
	}
	status, body = f.call(t, "GET", "/v1/boxes/"+restore.TargetID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	box := decodeResponse[Box](t, body)
	if box.Phase != "staged" {
		t.Fatalf("restored box state: %s", box.Phase)
	}
}

func TestPortableArchiveRestoreAcrossImagesRequiresOptIn(t *testing.T) {
	f := newCoreFixture(t)
	f.service.config.Profiles[0].Provider = "resumable-k8s-pod"
	f.service.providers["resumable-k8s-pod"] = f.provider
	const archiveID = "arc-11111111111111111111111111111111"
	payload := makeArchiveTestData(t, archiveTestEntry{"work.txt", tar.TypeReg, "portable"})
	dir, err := f.service.archivesDir()
	if err != nil {
		t.Fatal(err)
	}
	name, err := archivePath(dir, archiveID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, payload, 0600); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(payload)
	archive := Archive{ID: archiveID, SourceBoxID: "deleted-source", ImageID: "registry.example/source@sha256:" + strings.Repeat("a", 64), Agent: f.config.Profiles[0].Guest.Agent, SHA256: hex.EncodeToString(h[:]), Size: int64(len(payload)), Portable: true}
	if err := f.service.store.Update(func(st *State) error {
		st.Archives[archiveID] = archiveRecord{Archive: archive, ClientID: "client-a"}
		return nil
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
	request := map[string]any{"profileId": "profile-a", "ownerKey": "portable-target", "archiveId": archiveID}
	status, body := f.call(t, "POST", "/v1/boxes:restore", testClientToken, "restore-needs-optin", request)
	wantStatus(t, status, 409, body)
	request["acceptImageChange"] = true
	archive.Agent.UID++
	if err := f.service.store.Update(func(st *State) error {
		record := st.Archives[archiveID]
		record.Archive = archive
		st.Archives[archiveID] = record
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	status, body = f.call(t, "POST", "/v1/boxes:restore", testClientToken, "restore-agent-mismatch", request)
	wantStatus(t, status, 409, body)
	archive.Agent = f.config.Profiles[0].Guest.Agent
	if err := f.service.store.Update(func(st *State) error {
		record := st.Archives[archiveID]
		record.Archive = archive
		st.Archives[archiveID] = record
		return nil
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
	if err := f.service.store.View(func(st State) error {
		b := st.Boxes[op.TargetID]
		if b.Box.ImportedImageID != "" || b.Profile.Image != f.config.Profiles[0].Image {
			t.Fatalf("restore depended on deleted source import metadata: %+v", b)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureRejectsInvalidGuestArchiveAndCleansTemp(t *testing.T) {
	f := newCoreFixture(t)
	_, source := f.createBox(t, "invalid-capture-source")
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/archive" {
			_, _ = w.Write([]byte("invalid gzip"))
			return
		}
		w.WriteHeader(200)
	}))
	defer guest.Close()
	f.provider.mu.Lock()
	f.provider.guestURL = guest.URL
	f.provider.mu.Unlock()
	status, body := f.call(t, "POST", "/v1/boxes/"+source.ID+"/archives", testClientToken, "invalid-capture", nil)
	wantStatus(t, status, 202, body)
	op := decodeResponse[Operation](t, body)
	f.waitOperation(t, op.ID, "failed")
	archivesDir, err := f.service.archivesDir()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(archivesDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid capture left files: %v", entries)
	}
	if err := f.service.store.View(func(st State) error {
		if len(st.Archives) != 0 {
			t.Fatal("invalid capture persisted metadata")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
