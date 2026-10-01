package objectstorage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

// Opt-in integration test against the installation's S3 service. Every write
// is under a new random prefix and cleanup cannot touch another prefix.
func TestS3IntegrationConditionsMultipartAndPrefixIsolation(t *testing.T) {
	endpoint := os.Getenv("CELLBOX_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("CELLBOX_TEST_S3_ENDPOINT is unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	root := fmt.Sprintf("cellbox-tests/%d", time.Now().UnixNano())
	cfg := Config{Endpoint: endpoint, Bucket: os.Getenv("CELLBOX_TEST_S3_BUCKET"), Prefix: root, Region: "us-east-1", PathStyle: true, AccessKeyEnv: "OSS_ACCESS_KEY", SecretKeyEnv: "OSS_SECRET_KEY"}
	client, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := client.DeletePrefix(cleanupCtx, "objects"); err != nil {
			t.Error(err)
		}
	})
	put := func(key, content, condition string) (string, error) {
		return client.Put(ctx, "objects/"+key, bytes.NewReader([]byte(content)), int64(len(content)), condition)
	}
	etag, err := put("state", "initial", "*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = put("state", "duplicate", "*"); !errors.Is(err, ErrConflict) {
		t.Fatalf("create-only write failed to fence: %v", err)
	}
	if _, err = put("state", "wrong", "\"stale-etag\""); !errors.Is(err, ErrConflict) {
		t.Fatalf("compare-and-swap failed to fence: %v", err)
	}
	if _, err = put("state", "updated", etag); err != nil {
		t.Fatal(err)
	}
	if _, err = put("conditional-delete", "preserve-me", ""); err != nil {
		t.Fatal(err)
	}
	if err = client.DeleteIfMatch(ctx, "objects/conditional-delete", `"wrong-etag"`); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong ETag did not fence conditional delete: %v", err)
	}
	body, _, _, err := client.Get(ctx, "objects/conditional-delete")
	if err != nil {
		t.Fatal("conditional delete removed object with wrong ETag")
	}
	body.Close()
	if err = client.DeleteIfMatch(ctx, "objects/conditional-delete", etag); !errors.Is(err, ErrConflict) {
		t.Fatalf("ETag from another object unexpectedly matched: %v", err)
	}
	conditionalETag, err := put("conditional-delete", "preserve-me", "")
	if err != nil {
		t.Fatal(err)
	}
	if err = client.DeleteIfMatch(ctx, "objects/conditional-delete", conditionalETag); err != nil {
		t.Fatalf("matching ETag delete failed: %v", err)
	}
	if _, _, _, err = client.Get(ctx, "objects/conditional-delete"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("conditionally deleted object remains: %v", err)
	}
	large := bytes.Repeat([]byte("multipart-test"), 2<<20)
	if _, err = client.Put(ctx, "objects/workload/checkpoint.tar", bytes.NewReader(large), int64(len(large)), ""); err != nil {
		t.Fatal(err)
	}
	if _, err = put("workload-neighbor/keep", "neighbor", ""); err != nil {
		t.Fatal(err)
	}
	body, size, _, err := client.Get(ctx, "objects/workload/checkpoint.tar")
	if err != nil {
		t.Fatal(err)
	}
	actual, readErr := io.ReadAll(body)
	body.Close()
	if readErr != nil || size != int64(len(large)) || !bytes.Equal(actual, large) {
		t.Fatal("multipart content changed")
	}
	if err = client.DeletePrefix(ctx, "objects/workload"); err != nil {
		t.Fatal(err)
	}
	listed, err := client.List(ctx, "objects/workload")
	if err != nil {
		t.Fatal("list workload prefix:", err)
	}
	if len(listed) != 0 {
		t.Fatalf("deleted workload prefix still listed: %v", listed)
	}
	if _, _, _, err = client.Get(ctx, "objects/workload/checkpoint.tar"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("checkpoint remains: %v", err)
	}
	body, _, _, err = client.Get(ctx, "objects/workload-neighbor/keep")
	if err != nil {
		t.Fatal("prefix cleanup deleted neighbor")
	}
	body.Close()
	listed, err = client.List(ctx, "objects/workload")
	if err != nil {
		t.Fatal("list workload prefix after neighbor setup:", err)
	}
	if len(listed) != 0 {
		t.Fatalf("sibling prefix leaked into workload listing: %v", listed)
	}
	listed, err = client.List(ctx, "objects/workload-neighbor")
	if err != nil {
		t.Fatal("list sibling prefix:", err)
	}
	if len(listed) != 1 || listed[0] != "objects/workload-neighbor/keep" {
		t.Fatalf("listing returned unexpected relative keys: %v", listed)
	}
}
