package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/node"
	"cellbox.local/cellbox/internal/objectstorage"
	"cellbox.local/cellbox/internal/volumebackup"
)

func (p *diskCoreProvider) WithStoppedVolume(ctx context.Context, spec boxprovider.VolumeSpec, work func(string) error) error {
	return (&node.Backend{Base: p.base}).WithStoppedVolume(ctx, spec.UID, spec.NodeName, work)
}

type backupTestEngine struct {
	backup  func(context.Context, string, string, string) (volumebackup.Result, error)
	list    func(context.Context, string, string) ([]volumebackup.Snapshot, error)
	restore func(context.Context, string, string, string, string) error
}

func (e backupTestEngine) Backup(ctx context.Context, client, id, root string) (volumebackup.Result, error) {
	return e.backup(ctx, client, id, root)
}
func (e backupTestEngine) List(ctx context.Context, client, id string) ([]volumebackup.Snapshot, error) {
	return e.list(ctx, client, id)
}
func (e backupTestEngine) Restore(ctx context.Context, client, id, snapshot, root string) error {
	return e.restore(ctx, client, id, snapshot, root)
}

func TestDetachedVolumeBackupReadsWholeVolumeWithoutSandbox(t *testing.T) {
	f, provider := volumeFixture(t)
	volume := createTestVolume(t, f, "backup-disk")
	record, err := f.service.store.VolumeRecord("client-a", volume.ID)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := homevolume.Path(provider.base, record.UID)
	if err := os.WriteFile(filepath.Join(root, "outside-workspace"), []byte("persistent HOME data"), 0600); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	snapshot := volumebackup.Snapshot{ID: strings.Repeat("a", 64), CreatedAt: time.Now().UTC()}
	f.service.backups = backupTestEngine{
		backup: func(_ context.Context, client, id, path string) (volumebackup.Result, error) {
			calls.Add(1)
			if client != "client-a" || id != volume.ID || path != root {
				t.Errorf("wrong disk source: %q %q %q", client, id, path)
			}
			value, err := os.ReadFile(filepath.Join(path, "outside-workspace"))
			if err != nil || string(value) != "persistent HOME data" {
				t.Errorf("whole Volume was not directly readable: %q %v", value, err)
			}
			return volumebackup.Result{Snapshot: snapshot}, nil
		},
		list: func(_ context.Context, _, _ string) ([]volumebackup.Snapshot, error) {
			return []volumebackup.Snapshot{snapshot}, nil
		},
	}
	status, body := f.call(t, "POST", "/v1/volumes/"+volume.ID+"/backups", testClientToken, "", nil)
	wantStatus(t, status, 202, body)
	op := f.waitOperation(t, decodeResponse[Operation](t, body).ID, "succeeded")
	if op.Result["backupId"] != snapshot.ID || calls.Load() != 1 {
		t.Fatalf("backup result or submission count: %+v %d", op, calls.Load())
	}
	provider.mu.Lock()
	created := provider.createCalls
	provider.mu.Unlock()
	if created != 0 {
		t.Fatal("backup created a Sandbox")
	}
	status, body = f.call(t, "GET", "/v1/volumes/"+volume.ID+"/backups", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	backups := decodeResponse[[]VolumeBackup](t, body)
	if len(backups) != 1 || backups[0].VolumeID != volume.ID || backups[0].SizeBytes != nil {
		t.Fatalf("snapshot lost identity or manufactured an unknown size: %+v", backups)
	}
	status, body = f.call(t, "GET", "/v1/volumes/"+volume.ID+"/backups", otherClientToken, "", nil)
	wantStatus(t, status, 404, body)
	status, body = f.call(t, "GET", "/v1/backup-policy", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	policy := decodeResponse[struct {
		KeepLast int  `json:"keepLast"`
		Enabled  bool `json:"enabled"`
	}](t, body)
	if policy.KeepLast != 3 || !policy.Enabled {
		t.Fatalf("wrong default policy: %+v", policy)
	}
}

func TestVolumeBackupRejectsRunningSharedBoxAndSerializesOnlyItsVolume(t *testing.T) {
	f, _ := volumeFixture(t)
	volume := createTestVolume(t, f, "busy-disk")
	other := createTestVolume(t, f, "independent-disk")
	started, finish := make(chan struct{}), make(chan struct{})
	defer close(finish)
	f.service.backups = backupTestEngine{backup: func(ctx context.Context, _, id, _ string) (volumebackup.Result, error) {
		if id == volume.ID {
			close(started)
			select {
			case <-finish:
			case <-ctx.Done():
				return volumebackup.Result{}, ctx.Err()
			}
		}
		return volumebackup.Result{Snapshot: volumebackup.Snapshot{ID: strings.Repeat("b", 64), CreatedAt: time.Now().UTC()}}, nil
	}}
	box := boxRecord{Box: Box{ID: "shared-box", OwnerKey: volume.OwnerKey, Phase: "running", Mounts: []Mount{{VolumeID: volume.ID, MountPath: "/data", ReadOnly: true}}},
		ClientID: "client-a", Profile: f.config.Profiles[0]}
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error { return tx.PutBox(box) }); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.backupVolume("client-a", volume.ID); err == nil {
		t.Fatal("running shared attachment accepted")
	}
	box.Box.Phase = "suspended"
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error { return tx.PutBox(box) }); err != nil {
		t.Fatal(err)
	}
	op, err := f.service.backupVolume("client-a", volume.ID)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("backup did not reach disk reader")
	}
	for _, action := range []string{"resume", "activate"} {
		if _, err := f.service.action("client-a", box.Box.ID, action); err == nil || cloneError(err).Code != "BUSY" {
			t.Fatalf("%s was not rejected by volume admission: %v", action, err)
		}
	}
	if _, err := f.service.deleteVolume("client-a", volume.ID); err == nil {
		t.Fatal("deleted a backing-up volume")
	}
	if _, err := f.service.backupVolume("client-a", volume.ID); err == nil {
		t.Fatal("submitted a second backup")
	}
	_, err = f.service.create("client-a", createRequest{ID: "new-writer", OwnerKey: volume.OwnerKey,
		TemplateID: f.template(t), HomeVolumeID: volume.ID}, "", false)
	if err == nil {
		t.Fatal("created a new writer during backup")
	}
	independent, err := f.service.backupVolume("client-a", other.ID)
	if err != nil {
		t.Fatal(err)
	}
	f.waitOperation(t, independent.ID, "succeeded")
	retained, _ := f.service.store.VolumeRecord("client-a", volume.ID)
	if retained.Volume.OperationID != op.ID {
		t.Fatal("other volume cleared the pending operation")
	}
}

func TestVolumeRestoreAfterSourceDeletionCreatesOnlyNewVolume(t *testing.T) {
	f, provider := volumeFixture(t)
	volume := createTestVolume(t, f, "deleted-source")
	deleteOp, err := f.service.deleteVolume("client-a", volume.ID)
	if err != nil {
		t.Fatal(err)
	}
	f.waitOperation(t, deleteOp.ID, "succeeded")
	id := strings.Repeat("c", 64)
	f.service.backups = backupTestEngine{
		list: func(_ context.Context, client, source string) ([]volumebackup.Snapshot, error) {
			return []volumebackup.Snapshot{{ID: id, CreatedAt: time.Now().UTC()}}, nil
		},
		restore: func(_ context.Context, client, source, backup, root string) error {
			if client != "client-a" || source != volume.ID || backup != id {
				t.Errorf("wrong restore source: %q %q %q", client, source, backup)
			}
			return os.WriteFile(filepath.Join(root, "restored"), []byte("from S3"), 0600)
		},
	}
	status, body := f.call(t, "POST", "/v1/volumes:restore", testClientToken, "", restoreVolumeRequest{
		ID: "restored-disk", SourceVolumeID: volume.ID, BackupID: id, OwnerKey: "restored-owner"})
	wantStatus(t, status, 202, body)
	op := f.waitOperation(t, decodeResponse[Operation](t, body).ID, "succeeded")
	if op.TargetID != "restored-disk" || op.Result["backupId"] != id {
		t.Fatalf("wrong result: %+v", op)
	}
	source, _ := f.service.store.VolumeRecord("client-a", volume.ID)
	candidate, _ := f.service.store.VolumeRecord("client-a", op.TargetID)
	if source.Volume.Phase != "deleted" || source.Volume.OperationID != "" || candidate.Volume.Phase != "available" || source.UID == candidate.UID {
		t.Fatalf("restore changed the source or reused its identity: %+v %+v", source, candidate)
	}
	root, _ := homevolume.Path(provider.base, candidate.UID)
	value, err := os.ReadFile(filepath.Join(root, "restored"))
	if err != nil || string(value) != "from S3" {
		t.Fatalf("restore did not write new disk: %q %v", value, err)
	}
	provider.mu.Lock()
	created := provider.createCalls
	provider.mu.Unlock()
	if created != 0 {
		t.Fatal("restore created a Sandbox")
	}
}

func TestVolumeBackupFailureAndRetentionFailureRemainDistinct(t *testing.T) {
	for _, failBackup := range []bool{true, false} {
		f, _ := volumeFixture(t)
		volume := createTestVolume(t, f, "failure-disk")
		var calls atomic.Int32
		f.service.backups = backupTestEngine{backup: func(context.Context, string, string, string) (volumebackup.Result, error) {
			calls.Add(1)
			if failBackup {
				return volumebackup.Result{}, errors.New("backup failed")
			}
			return volumebackup.Result{Snapshot: volumebackup.Snapshot{ID: strings.Repeat("d", 64), CreatedAt: time.Now().UTC()}, RetentionError: "Repository cleanup failed"}, nil
		}}
		op, err := f.service.backupVolume("client-a", volume.ID)
		if err != nil {
			t.Fatal(err)
		}
		want := "succeeded"
		if failBackup {
			want = "failed"
		}
		finished := f.waitOperation(t, op.ID, want)
		if calls.Load() != 1 {
			t.Fatal("backup was automatically replayed")
		}
		if !failBackup && (finished.Result["backupId"] == "" || finished.Result["retentionError"] == "") {
			t.Fatalf("backup or cleanup result was lost: %+v", finished)
		}
		retained, err := f.service.store.VolumeRecord("client-a", volume.ID)
		if err != nil || retained.Volume.OperationID != "" {
			t.Fatalf("failed operation left a permanent reservation: %+v %v", retained, err)
		}
		if retained.Volume.Phase != "available" || retained.Volume.Error != nil {
			t.Fatalf("read-only backup failure changed disk availability: %+v", retained.Volume)
		}
	}
}

func TestInterruptedVolumeBackupIsUnknownAndNotReplayed(t *testing.T) {
	f, _ := volumeFixture(t)
	volume := createTestVolume(t, f, "interrupted-disk")
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error {
		record, err := tx.Volume("client-a", volume.ID)
		if err != nil {
			return err
		}
		record.Volume.OperationID, record.Volume.Phase = "interrupted", "backing-up"
		if err := tx.PutOperation(operationRecord{ClientID: "client-a", Operation: Operation{ID: "interrupted", Kind: "volume-backup", TargetID: volume.ID, Status: "running", CreatedAt: time.Now()}}); err != nil {
			return err
		}
		return tx.PutVolume(record)
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.service.store.InterruptOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	retained, err := f.service.store.VolumeRecord("client-a", volume.ID)
	if err != nil || retained.Volume.OperationID != "" || retained.Volume.Phase != "available" {
		t.Fatalf("interrupted volume was not made inspectable: %+v %v", retained, err)
	}
	status, body := f.call(t, "GET", "/v1/operations/interrupted", testClientToken, "", nil)
	wantStatus(t, status, 200, body)
	operation := decodeResponse[Operation](t, body)
	if operation.Status != "failed" || operation.Error == nil || operation.Error.Code != "UNKNOWN_OUTCOME" {
		t.Fatalf("interrupted backup was not reported as unknown: %+v", operation)
	}
}

func TestInterruptedVolumeRestorePreservesSourceAndFailsOnlyTarget(t *testing.T) {
	f, _ := volumeFixture(t)
	source := createTestVolume(t, f, "restore-source")
	target := createTestVolume(t, f, "restore-target")
	if err := f.service.store.Write(context.Background(), func(tx *storeTx) error {
		for _, id := range []string{source.ID, target.ID} {
			record, err := tx.Volume("client-a", id)
			if err != nil {
				return err
			}
			record.Volume.OperationID = "interrupted-restore"
			if id == target.ID {
				record.Volume.Phase = "creating"
			}
			if err := tx.PutVolume(record); err != nil {
				return err
			}
		}
		return tx.PutOperation(operationRecord{ClientID: "client-a", Operation: Operation{ID: "interrupted-restore", Kind: "volume-restore", TargetID: target.ID, Status: "running", CreatedAt: time.Now()}})
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.service.store.InterruptOperations(context.Background()); err != nil {
		t.Fatal(err)
	}
	original, _ := f.service.store.VolumeRecord("client-a", source.ID)
	candidate, _ := f.service.store.VolumeRecord("client-a", target.ID)
	if original.Volume.Phase != "available" || original.Volume.OperationID != "" || original.Volume.Error != nil {
		t.Fatalf("read-only restore source was marked failed: %+v", original.Volume)
	}
	if candidate.Volume.Phase != "failed" || candidate.Volume.Error == nil || candidate.Volume.Error.Code != "UNKNOWN_OUTCOME" {
		t.Fatalf("partially restored target was advertised as ready: %+v", candidate.Volume)
	}
}

func TestConfiguredVolumeBackupsRequirePasswordAtStartup(t *testing.T) {
	f := newCoreFixture(t)
	config := f.config
	config.DataDir = t.TempDir()
	config.VolumeBackups.PasswordEnv = "CELLBOX_TEST_MISSING_BACKUP_PASSWORD"
	t.Setenv(config.VolumeBackups.PasswordEnv, "")
	service, err := New(config, map[string]boxprovider.Provider{"docker": f.provider})
	if err == nil {
		service.Close()
		t.Fatal("advertised backups despite a missing repository password")
	}
	if !strings.Contains(err.Error(), "password environment is empty") {
		t.Fatalf("unexpected startup error: %v", err)
	}
}

// Opt-in filesystem/S3 integration. The endpoint must be isolated from any
// installation; every repository is placed under a fresh test-only prefix.
func TestVolumeBackupS3APIIntegration(t *testing.T) {
	endpoint, bucket := os.Getenv("CELLBOX_TEST_BACKUP_S3_ENDPOINT"), os.Getenv("CELLBOX_TEST_BACKUP_S3_BUCKET")
	binary := os.Getenv("CELLBOX_TEST_RESTIC")
	if endpoint == "" || bucket == "" || binary == "" {
		t.Skip("set isolated CELLBOX_TEST_BACKUP_S3_* and CELLBOX_TEST_RESTIC settings")
	}
	t.Setenv("CELLBOX_TEST_API_BACKUP_PASSWORD", "isolated-api-test-password")
	f, provider := volumeFixture(t)
	engine, err := volumebackup.New(volumebackup.Config{Binary: binary, PasswordEnv: "CELLBOX_TEST_API_BACKUP_PASSWORD", KeepLast: 3},
		objectstorage.Config{Endpoint: endpoint, Bucket: bucket, Region: "us-east-1", PathStyle: true,
			Prefix:       fmt.Sprintf("test/api-volume-backups/%d", time.Now().UnixNano()),
			AccessKeyEnv: "CELLBOX_TEST_BACKUP_S3_ACCESS_KEY", SecretKeyEnv: "CELLBOX_TEST_BACKUP_S3_SECRET_KEY"})
	if err != nil {
		t.Fatal(err)
	}
	f.service.backups = engine
	wait := func(id string) Operation {
		t.Helper()
		deadline := time.Now().Add(time.Minute)
		for time.Now().Before(deadline) {
			status, body := f.call(t, "GET", "/v1/operations/"+id, testClientToken, "", nil)
			wantStatus(t, status, 200, body)
			op := decodeResponse[Operation](t, body)
			if op.Status == "succeeded" {
				return op
			}
			if op.Status == "failed" {
				t.Fatalf("S3 operation failed: %+v", op)
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("S3 operation %s did not finish", id)
		return Operation{}
	}
	volume := createTestVolume(t, f, "api-s3-source")
	record, _ := f.service.store.VolumeRecord("client-a", volume.ID)
	root, _ := homevolume.Path(provider.base, record.UID)
	for version := 1; version <= 4; version++ {
		if err := os.WriteFile(filepath.Join(root, "outside-workspace"), []byte(fmt.Sprint(version)), 0640); err != nil {
			t.Fatal(err)
		}
		op, err := f.service.backupVolume("client-a", volume.ID)
		if err != nil {
			t.Fatal(err)
		}
		finished := wait(op.ID)
		if finished.Result["retentionError"] != "" {
			t.Fatalf("S3 retention failed: %+v", finished.Result)
		}
	}
	backups, err := f.service.listVolumeBackups(context.Background(), "client-a", volume.ID)
	if err != nil || len(backups) != 3 {
		t.Fatalf("three-version retention failed: backups=%d err=%v", len(backups), err)
	}
	op, err := f.service.deleteVolume("client-a", volume.ID)
	if err != nil {
		t.Fatal(err)
	}
	wait(op.ID)
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source data was not removed: %v", err)
	}
	// Both the oldest retained version and the newest must remain independently
	// restorable after pruning shared chunks and deleting the original disk.
	for _, index := range []int{2, 0} {
		backup := backups[index]
		op, err := f.service.restoreVolume("client-a", restoreVolumeRequest{SourceVolumeID: volume.ID, BackupID: backup.ID,
			OwnerKey: volume.OwnerKey, ID: fmt.Sprintf("api-s3-restored-%d", index)})
		if err != nil {
			t.Fatal(err)
		}
		finished := wait(op.ID)
		restored, _ := f.service.store.VolumeRecord("client-a", finished.TargetID)
		path, _ := homevolume.Path(provider.base, restored.UID)
		value, err := os.ReadFile(filepath.Join(path, "outside-workspace"))
		if err != nil || string(value) != fmt.Sprint(4-index) {
			t.Fatalf("restored version %d: %q %v", index, value, err)
		}
	}
	provider.mu.Lock()
	created := provider.createCalls
	provider.mu.Unlock()
	if created != 0 {
		t.Fatal("volume backup or restore created a Sandbox")
	}
}
