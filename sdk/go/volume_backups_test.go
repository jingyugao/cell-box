package cellbox

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestVolumeBackupSDKContract(t *testing.T) {
	var backupPosts, restorePosts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertHeaders(t, r)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backup-policy":
			writeJSONTest(w, BackupPolicy{Engine: "restic", Scope: "volume", KeepLast: 3, Enabled: true})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/volumes/data-a/backups":
			backupPosts.Add(1)
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body) != 0 {
				t.Errorf("backup body = %#v, err=%v; want empty object", body, err)
			}
			writeJSONTest(w, Operation{ID: "op-backup", Kind: "volume-backup", TargetID: "data-a", Status: "queued", Result: map[string]string{"volumeId": "data-a"}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op-backup":
			writeJSONTest(w, Operation{ID: "op-backup", Kind: "volume-backup", TargetID: "data-a", Status: "succeeded", Result: map[string]string{"volumeId": "data-a", "backupId": strings.Repeat("a", 64)}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/volumes/data-a/backups":
			writeJSONTest(w, []VolumeBackup{{ID: strings.Repeat("a", 64), VolumeID: "data-a", OwnerKey: "project:p1", ProfileID: "k8s", CreatedAt: backupTestTime(), SizeBytes: backupInt64(300), AddedBytes: backupInt64(20)}, {ID: strings.Repeat("b", 64), VolumeID: "data-a", OwnerKey: "project:p1", ProfileID: "k8s", CreatedAt: backupTestTime()}})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/volumes/data-a/backups/"+strings.Repeat("a", 64):
			writeJSONTest(w, VolumeBackup{ID: strings.Repeat("a", 64), VolumeID: "data-a", SizeBytes: backupInt64(300), AddedBytes: backupInt64(20)})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/volumes:restore":
			restorePosts.Add(1)
			var body RestoreVolumeOptions
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.SourceVolumeID != "data-a" || body.BackupID != strings.Repeat("a", 64) || body.OwnerKey != "project:p2" || body.ID != "data-restore" {
				t.Errorf("restore request = %#v", body)
			}
			writeJSONTest(w, Operation{ID: "op-restore", Kind: "volume-restore", TargetID: "data-restore", Status: "succeeded", Result: map[string]string{"volumeId": "data-restore", "sourceVolumeId": "data-a", "backupId": body.BackupID}})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := testClient(t, server, "")

	policy, err := c.GetBackupPolicy(context.Background())
	if err != nil || *policy != (BackupPolicy{Engine: "restic", Scope: "volume", KeepLast: 3, Enabled: true}) {
		t.Fatalf("backup policy = %#v, %v", policy, err)
	}
	op, err := c.CreateVolumeBackupAsync(context.Background(), "data-a")
	if err != nil || op.ID != "op-backup" {
		t.Fatalf("async backup = %#v, %v", op, err)
	}
	completed, err := c.CreateVolumeBackup(context.Background(), "data-a")
	if err != nil || completed.Result["backupId"] != strings.Repeat("a", 64) {
		t.Fatalf("backup = %#v, %v", completed, err)
	}
	backups, err := c.ListVolumeBackups(context.Background(), "data-a")
	if err != nil || len(backups) != 2 || backups[0].SizeBytes == nil || *backups[0].SizeBytes != 300 || backups[1].SizeBytes != nil || backups[1].AddedBytes != nil {
		t.Fatalf("volume backups = %#v, %v", backups, err)
	}
	backup, err := c.GetVolumeBackup(context.Background(), "data-a", strings.Repeat("a", 64))
	if err != nil || backup.VolumeID != "data-a" {
		t.Fatalf("get volume backup = %#v, %v", backup, err)
	}
	restored, err := c.RestoreVolume(context.Background(), RestoreVolumeOptions{SourceVolumeID: "data-a", BackupID: strings.Repeat("a", 64), OwnerKey: "project:p2", ID: "data-restore"})
	if err != nil || restored.Kind != "volume-restore" || restored.Result["volumeId"] != "data-restore" {
		t.Fatalf("restore volume = %#v, %v", restored, err)
	}
	if backupPosts.Load() != 2 || restorePosts.Load() != 1 {
		t.Fatalf("mutations resent: backup=%d restore=%d", backupPosts.Load(), restorePosts.Load())
	}
	if backups[0].VolumeID != "data-a" {
		t.Fatal("backup source volume identity lost")
	}
}

func backupTestTime() time.Time { return time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC) }

func backupInt64(value int64) *int64 { return &value }

func TestRestoreVolumeAsyncValidatesRequiredFields(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.NotFound(w, r)
	}))
	defer server.Close()
	c, err := NewClient(Config{BaseURL: server.URL, ClientID: "sdk-test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	for _, opts := range []RestoreVolumeOptions{
		{SourceVolumeID: "data-a", BackupID: "backup", OwnerKey: "owner"},
		{SourceVolumeID: "bad/id", BackupID: strings.Repeat("a", 64), OwnerKey: "owner"},
		{SourceVolumeID: "data-a", BackupID: "ABCDEF" + strings.Repeat("a", 58), OwnerKey: "owner"},
		{SourceVolumeID: "data-a", BackupID: strings.Repeat("a", 64), OwnerKey: "owner", ID: "Bad_ID"},
		{SourceVolumeID: "data-a", BackupID: strings.Repeat("a", 64), OwnerKey: strings.Repeat("x", 257)},
	} {
		if _, err := c.RestoreVolumeAsync(context.Background(), opts); err == nil {
			t.Errorf("accepted invalid restore options: %#v", opts)
		}
	}
	for _, tc := range []struct{ volumeID, backupID string }{
		{"bad/id", strings.Repeat("a", 64)},
		{"data-a", "abcdef"},
		{"data-a", strings.Repeat("A", 64)},
	} {
		if _, err := c.GetVolumeBackup(context.Background(), tc.volumeID, tc.backupID); err == nil {
			t.Errorf("GetVolumeBackup accepted volume=%q backup=%q", tc.volumeID, tc.backupID)
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("invalid IDs sent %d HTTP requests", requests.Load())
	}
}
