package service

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jingyugao/cell-box/internal/objectstorage"
)

func TestS3IntegrationArchivePayloadSurvivesServiceRestart(t *testing.T) {
	endpoint := os.Getenv("CELLBOX_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("CELLBOX_TEST_S3_ENDPOINT is unset")
	}
	f := newCoreFixture(t)
	cfg := objectstorage.Config{Endpoint: endpoint, Bucket: os.Getenv("CELLBOX_TEST_S3_BUCKET"), Region: "us-east-1", Prefix: fmt.Sprintf("cellbox-tests/%d", time.Now().UnixNano()), PathStyle: true, AccessKeyEnv: "OSS_ACCESS_KEY", SecretKeyEnv: "OSS_SECRET_KEY"}
	f.config.ObjectStorage = cfg
	objects, err := objectstorage.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := objects.DeletePrefix(ctx, "archives"); err != nil {
			t.Error(err)
		}
	})
	f.reopen(t)
	const archiveID = "arc-0123456789abcdef0123456789abcdef"
	payload := makeArchiveTestData(t, archiveTestEntry{"main.go", tar.TypeReg, "package main\nfunc main() {}\n"})
	if _, err := objects.Put(context.Background(), "archives/"+archiveID+".tar.gz", bytes.NewReader(payload), int64(len(payload)), ""); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	a := Archive{ID: archiveID, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(payload))}
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error {
		return tx.PutArchive(archiveRecord{Archive: a, ClientID: "client-a"})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(f.config.DataDir, "cellbox.db")); err != nil {
		t.Fatalf("SQLite metadata database missing: %v", err)
	}

	// Metadata remains in SQLite; only the archive payload comes from S3.
	f.reopen(t)
	status, body := f.call(t, "GET", "/v1/archives/"+archiveID, testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	status, body = f.call(t, "GET", "/v1/archives/"+archiveID+"/content", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	if !bytes.Equal(body, payload) {
		t.Fatal("archive payload changed after service restart")
	}
	status, body = f.call(t, "DELETE", "/v1/archives/"+archiveID, otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "DELETE", "/v1/archives/"+archiveID, testClientToken, "", nil)
	wantStatus(t, status, 204, body)
	status, body = f.call(t, "GET", "/v1/archives/"+archiveID, testClientToken, "", nil)
	wantStatus(t, status, 404, body)
}
