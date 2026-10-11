package objectstorage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type metadataRemote struct {
	Objects
	reads, writes int
	unavailable   bool
	failRead      int
	changeVersion bool
}

func (r *metadataRemote) Get(ctx context.Context, key string) (io.ReadCloser, int64, string, error) {
	r.reads++
	if r.unavailable || r.reads == r.failRead {
		return nil, 0, "", ErrUnavailable
	}
	body, size, etag, err := r.Objects.Get(ctx, key)
	if r.changeVersion && r.reads > 1 {
		etag = "\"changed\""
	}
	return body, size, etag, err
}
func (r *metadataRemote) List(ctx context.Context, prefix string) ([]string, error) {
	if r.unavailable {
		return nil, ErrUnavailable
	}
	return r.Objects.List(ctx, prefix)
}
func (r *metadataRemote) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, condition string) (string, error) {
	r.writes++
	if r.unavailable {
		return "", ErrUnavailable
	}
	return r.Objects.Put(ctx, key, body, size, condition)
}

func TestLocalMetadataRoutesRecordsAndKeepsAssetsRemoteAcrossRestart(t *testing.T) {
	ctx := context.Background()
	source, err := NewLocal(filepath.Join(t.TempDir(), "source"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	metaKeys := []string{"metadata/state.json", "metadata/profiles/p.json", "metadata/results/exec.json", "images/img-1/metadata.json", "checkpoints/owner/metadata.json"}
	for _, key := range append(append([]string{}, metaKeys...), "checkpoints/owner/checkpoint-1.tar", "archives/archive.tar.gz") {
		if err := source.Import(ctx, key, bytes.NewReader([]byte(key)), int64(len(key)), "\"source-etag\""); err != nil {
			t.Fatal(err)
		}
	}
	remote := &metadataRemote{Objects: source}
	dir := filepath.Join(t.TempDir(), "metadata")
	if _, err := OpenMetadata(ctx, dir, remote, false); err == nil {
		t.Fatal("silently created a fresh local database")
	}
	local, err := OpenMetadata(ctx, dir, remote, true)
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	for _, key := range metaKeys {
		body, _, etag, err := local.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(body)
		body.Close()
		if string(data) != key || etag != "\"source-etag\"" {
			t.Fatalf("lost imported bytes/ETag for %s", key)
		}
	}
	if _, _, _, err := local.local.Get(ctx, "checkpoints/owner/checkpoint-1.tar"); !errors.Is(err, ErrNotFound) {
		t.Fatal("copied checkpoint bytes into local metadata")
	}
	if _, err := local.Put(ctx, "metadata/state.json", bytes.NewReader([]byte("updated")), 7, "\"source-etag\""); err != nil {
		t.Fatal(err)
	}
	if err := local.DeleteIfMatch(ctx, "checkpoints/owner/metadata.json", "\"source-etag\""); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := local.Get(ctx, "checkpoints/owner/metadata.json"); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted metadata fell back to stale remote index")
	}
	keys, err := local.List(ctx, "checkpoints")
	if err != nil || !reflect.DeepEqual(keys, []string{"checkpoints/owner/checkpoint-1.tar"}) {
		t.Fatalf("merged listing revived old index: %v %v", keys, err)
	}
	if remote.writes != 0 {
		t.Fatal("metadata update wrote to remote storage")
	}
	if _, err := local.Put(ctx, "archives/new.tar.gz", bytes.NewReader([]byte("asset")), 5, ""); err != nil {
		t.Fatal(err)
	}
	if remote.writes != 1 {
		t.Fatal("archive content did not reach remote storage")
	}
	local.Close()
	remote.unavailable = true
	remote.reads = 0
	local, err = OpenMetadata(ctx, dir, remote, false)
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	body, _, _, err := local.Get(ctx, "metadata/state.json")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(body)
	body.Close()
	if string(data) != "updated" || remote.reads != 0 {
		t.Fatal("restart did not use authoritative local metadata")
	}
	keys, err = ListMetadata(ctx, local, "images")
	if err != nil || !reflect.DeepEqual(keys, []string{"images/img-1/metadata.json"}) {
		t.Fatalf("metadata listing depends on remote assets: %v %v", keys, err)
	}
	if _, _, _, err := local.Get(ctx, "archives/new.tar.gz"); !errors.Is(err, ErrUnavailable) {
		t.Fatal("archive content unexpectedly stored locally")
	}
}

func TestLocalMetadataImportFailureNeverPublishesPartialDatabase(t *testing.T) {
	ctx := context.Background()
	for _, change := range []bool{false, true} {
		t.Run(map[bool]string{false: "read-failure", true: "source-changed"}[change], func(t *testing.T) {
			source, err := NewLocal(filepath.Join(t.TempDir(), "source"))
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			if _, err := source.Put(ctx, "metadata/state.json", bytes.NewReader([]byte("head")), 4, "*"); err != nil {
				t.Fatal(err)
			}
			remote := &metadataRemote{Objects: source, failRead: 2, changeVersion: change}
			if change {
				remote.failRead = 0
			}
			dir := filepath.Join(t.TempDir(), "destination")
			if _, err := OpenMetadata(ctx, dir, remote, true); err == nil {
				t.Fatal("import succeeded despite incomplete/changing source")
			}
			if _, err := os.Stat(filepath.Join(dir, "objects")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("published incomplete metadata")
			}
			if _, err := OpenMetadata(ctx, dir, remote, false); err == nil {
				t.Fatal("opened partial import")
			}
			remote.failRead, remote.changeVersion = 0, false
			local, err := OpenMetadata(ctx, dir, remote, true)
			if err != nil {
				t.Fatal(err)
			}
			local.Close()
			if err := os.Remove(filepath.Join(dir, "objects", ".initialized")); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenMetadata(ctx, dir, remote, true); err == nil {
				t.Fatal("overwrote corrupt local database with old remote data")
			}
		})
	}
}
