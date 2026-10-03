package service

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/inventory"
	"cellbox.local/cellbox/internal/objectstorage"
	"encoding/json"
)

func TestS3IntegrationAPIRestartWithoutLocalDataAndArchiveRestore(t *testing.T) {
	endpoint := os.Getenv("CELLBOX_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("CELLBOX_TEST_S3_ENDPOINT is unset")
	}
	f := newCoreFixture(t)
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	f.service = nil
	cfg := objectstorage.Config{Endpoint: endpoint, Bucket: os.Getenv("CELLBOX_TEST_S3_BUCKET"), Region: "us-east-1", Prefix: fmt.Sprintf("cellbox-tests/%d", time.Now().UnixNano()), PathStyle: true, AccessKeyEnv: "OSS_ACCESS_KEY", SecretKeyEnv: "OSS_SECRET_KEY"}
	f.config.ObjectStorage = cfg
	if err := os.RemoveAll(f.config.DataDir); err != nil {
		t.Fatal(err)
	}
	objects, err := objectstorage.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, prefix := range []string{"metadata", "archives", "images", "checkpoints"} {
			if err := objects.DeletePrefix(ctx, prefix); err != nil {
				t.Error(err)
			}
		}
	})
	f.service, err = New(f.config, map[string]boxprovider.Provider{"docker": f.provider})
	if err != nil {
		t.Fatal(err)
	}
	first := "img-11111111111111111111111111111111"
	second := "img-22222222222222222222222222222222"
	if err = f.service.store.Update(func(st *State) error {
		for _, id := range []string{first, second} {
			st.ImportedImages[id] = importedImageRecord{ClientID: "client-a", ImportedImage: ImportedImage{ID: id, Source: "original"}}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	bodyMeta, _, secondVersion, err := objects.Get(context.Background(), "images/"+second+"/metadata.json")
	if err != nil {
		t.Fatal(err)
	}
	bodyMeta.Close()
	if err = f.service.store.Update(func(st *State) error {
		image := st.ImportedImages[first]
		image.ImportedImage.Source = "updated"
		st.ImportedImages[first] = image
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	bodyMeta, _, version, err := objects.Get(context.Background(), "images/"+second+"/metadata.json")
	if err != nil {
		t.Fatal(err)
	}
	bodyMeta.Close()
	if version != secondVersion {
		t.Fatal("updating one image rewrote neighbor metadata")
	}

	// Verify lists against actual OSS objects absent from the service mutation cache.
	externalID := "img-33333333333333333333333333333333"
	externalKey := "images/" + externalID + "/metadata.json"
	data, _ := json.Marshal(storedImage{Revision: "outside-cache", Record: &importedImageRecord{ClientID: "client-a", ImportedImage: ImportedImage{ID: externalID, Source: "external"}}})
	if _, err = objects.Put(context.Background(), externalKey, bytes.NewReader(data), int64(len(data)), "*"); err != nil {
		t.Fatal(err)
	}
	statusList, bodyList := f.call(t, "GET", "/v1/images", testClientToken, "", nil)
	wantStatus(t, statusList, 200, bodyList)
	if len(decodeResponse[[]ImportedImage](t, bodyList)) != 3 {
		t.Fatalf("OSS-only image missing: %s", bodyList)
	}
	statusList, bodyList = f.call(t, "GET", "/v1/images/"+externalID, testClientToken, "", nil)
	wantStatus(t, statusList, 200, bodyList)
	if decodeResponse[ImportedImage](t, bodyList).Source != "external" {
		t.Fatal("single image read used memory")
	}
	if err = objects.Delete(context.Background(), externalKey); err != nil {
		t.Fatal(err)
	}
	checkpointKey := "checkpoints/oss-only-runtime/metadata.json"
	data, _ = json.Marshal(inventory.Record{ClientID: "client-a", RuntimeID: "oss-only-runtime", Snapshot: "checkpoint-1", Box: mustBoxJSON(Box{ID: "box-oss-only", Phase: "suspended"})})
	if _, err = objects.Put(context.Background(), checkpointKey, bytes.NewReader(data), int64(len(data)), "*"); err != nil {
		t.Fatal(err)
	}
	statusList, bodyList = f.call(t, "GET", "/v1/checkpoints", testClientToken, "", nil)
	wantStatus(t, statusList, 200, bodyList)
	if rows := decodeResponse[[]Box](t, bodyList); len(rows) != 1 || rows[0].ID != "box-oss-only" {
		t.Fatalf("OSS checkpoint list missing: %s", bodyList)
	}
	if err = objects.Delete(context.Background(), checkpointKey); err != nil {
		t.Fatal(err)
	}

	create, source := f.createBox(t, "source-create")
	payload := makeArchiveTestData(t, archiveTestEntry{"main.go", tar.TypeReg, "package main\nfunc main() {}\n"})
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
			_, _ = w.Write(payload)
		case "/v1/restore":
			body, err := io.ReadAll(r.Body)
			if err != nil || !bytes.Equal(body, payload) {
				http.Error(w, "wrong restore payload", 400)
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
	capture := decodeResponse[Operation](t, body)
	archiveID := f.waitOperation(t, capture.ID, "succeeded").Result["archiveId"]
	if _, err := os.Stat(filepath.Join(f.config.DataDir, "state.json")); !os.IsNotExist(err) {
		t.Fatalf("API wrote local ledger: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.config.DataDir, "archives", archiveID+".tar.gz")); !os.IsNotExist(err) {
		t.Fatalf("archive retained as local durable content: %v", err)
	}
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	f.service = nil
	if err := os.RemoveAll(f.config.DataDir); err != nil {
		t.Fatal(err)
	}
	f.config.DataDir = t.TempDir()
	f.service, err = New(f.config, map[string]boxprovider.Provider{"docker": f.provider})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.service.store.View(func(st State) error {
		if len(st.ImportedImages) != 2 || st.ImportedImages[first].ImportedImage.Source != "updated" {
			t.Fatal("image metadata was not reloaded from individual objects")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	status, body = f.call(t, "GET", "/v1/boxes/"+source.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	status, body = f.call(t, "POST", "/v1/boxes", testClientToken, "source-create", map[string]string{"profileId": "profile-a", "ownerKey": "owner-a"})
	wantStatus(t, status, 202, body)
	replay := decodeResponse[Operation](t, body)
	if replay.ID != create.ID {
		t.Fatal("idempotency key lost after restart")
	}
	status, body = f.call(t, "GET", "/v1/operations/"+capture.ID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	status, body = f.call(t, "GET", "/v1/archives/"+archiveID+"/content", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if !bytes.Equal(body, payload) {
		t.Fatal("S3 archive changed after API restart")
	}
	status, body = f.call(t, "POST", "/v1/boxes:restore", testClientToken, "restore-once", map[string]string{"profileId": "profile-a", "ownerKey": "restored", "archiveId": archiveID})
	wantStatus(t, status, 202, body)
	restore := decodeResponse[Operation](t, body)
	f.waitOperation(t, restore.ID, "succeeded")
	if !restored.Load() {
		t.Fatal("archive not restored to guest")
	}
	status, body = f.call(t, "DELETE", "/v1/archives/"+archiveID, otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "DELETE", "/v1/archives/"+archiveID, testClientToken, "", nil)
	wantStatus(t, status, 204, body)
	status, body = f.call(t, "GET", "/v1/archives/"+archiveID, testClientToken, "", nil)
	wantStatus(t, status, 404, body)
}
