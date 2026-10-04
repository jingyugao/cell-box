package service

import (
	"bytes"
	"compress/gzip"
	"context"
	"testing"
)

func TestReadLegacyCompressedMetadata(t *testing.T) {
	want := []byte(`{"revision":"legacy","state":{"schema":3}}`)
	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	_, _ = gz.Write(want)
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	objects := &ledgerObjects{data: map[string][]byte{"metadata/record.json": compressed.Bytes()}}
	got, etag, err := readObject(context.Background(), objects, "metadata/record.json")
	if err != nil || !bytes.Equal(got, want) || etag == "" {
		t.Fatalf("legacy metadata unreadable: %v", err)
	}
	objects.data["metadata/record.json"] = compressed.Bytes()[:compressed.Len()-1]
	if _, _, err := readObject(context.Background(), objects, "metadata/record.json"); err == nil {
		t.Fatal("truncated gzip accepted")
	}
}

func TestMergeOnlyDuplicateExpiredReplayFences(t *testing.T) {
	dst, src := newState(), newState()
	dst.Keys["replay"] = keyRecord{Expired: true, Hash: "old"}
	src.Keys["replay"] = keyRecord{Expired: true}
	if err := mergeRecordState(&dst, src); err != nil || !dst.Keys["replay"].Expired {
		t.Fatalf("expired fence migration failed: %v", err)
	}
	src.Keys["replay"] = keyRecord{Hash: "live"}
	if err := mergeRecordState(&dst, src); err == nil {
		t.Fatal("live duplicate replay key accepted")
	}
}
