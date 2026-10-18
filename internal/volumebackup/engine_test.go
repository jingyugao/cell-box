package volumebackup

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/objectstorage"
)

func TestConfigValidateDefaultsAndBounds(t *testing.T) {
	cfg := Config{}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Binary != "restic" || cfg.KeepLast != 3 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	for _, keep := range []int{-1, 101} {
		cfg := Config{KeepLast: keep}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("KeepLast=%d accepted", keep)
		}
	}
}

func TestS3RepositoryURLAndVolumeIsolation(t *testing.T) {
	cfg := Config{PasswordEnv: "BACKUP_PASSWORD"}
	storage := objectstorage.Config{Endpoint: "http://127.0.0.1:9000/", Bucket: "backup-bucket", Prefix: "cellbox", Region: "us-east-1", PathStyle: true}
	engine, err := New(cfg, storage)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("client\x00volume"))
	want := "s3:http://127.0.0.1:9000/backup-bucket/cellbox/volume-backups/" + hex.EncodeToString(hash[:])
	if got := engine.repository("client", "volume"); got != want {
		t.Fatalf("repository URL=%q want %q", got, want)
	}
	if engine.repository("client", "volume") == engine.repository("client", "other") {
		t.Fatal("different volumes must use isolated repositories")
	}
	args := strings.Join(engine.baseArgs("client", "volume"), " ")
	if strings.Contains(args, "s3.endpoint=") || !strings.Contains(args, "s3.bucket-lookup=path") {
		t.Fatalf("unsupported or incorrect S3 options: %s", args)
	}
}

func TestRealResticIncrementalRetentionAndRestore(t *testing.T) {
	binary := requireRestic(t)
	ctx := context.Background()
	root := t.TempDir()
	repository := filepath.Join(root, "repository")
	cache := filepath.Join(root, "cache")
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(cache, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CELLBOX_TEST_VOLUME_BACKUP_PASSWORD", "test-only-volume-password")
	engine := localEngine(binary, repository, cache)
	large := make([]byte, 8<<20)
	if _, err := rand.Read(large); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "large.bin"), large, 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "executable"), []byte("#!/bin/sh\nexit 0\n"), 0751); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("large.bin", filepath.Join(source, "large-link")); err != nil {
		t.Fatal(err)
	}

	var results []Result
	for version := 1; version <= 4; version++ {
		if err := os.WriteFile(filepath.Join(source, "version"), []byte(fmt.Sprint(version)), 0600); err != nil {
			t.Fatal(err)
		}
		result, err := engine.Backup(ctx, "client-A", "volume-A", source)
		if err != nil {
			t.Fatalf("backup %d: %v", version, err)
		}
		if result.RetentionError != "" {
			t.Fatalf("backup %d retention failed: %s", version, result.RetentionError)
		}
		results = append(results, result)
	}

	if results[3].Snapshot.SizeBytes == nil || results[3].Snapshot.AddedBytes == nil {
		t.Fatal("restic summary size fields were not captured")
	}
	if *results[3].Snapshot.AddedBytes >= *results[3].Snapshot.SizeBytes {
		t.Fatalf("fourth snapshot did not demonstrate incremental reuse: added=%d total=%d", *results[3].Snapshot.AddedBytes, *results[3].Snapshot.SizeBytes)
	}

	snapshots, err := engine.List(ctx, "client-A", "volume-A")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 3 {
		t.Fatalf("got %d snapshots after four backups, want 3", len(snapshots))
	}
	for _, snapshot := range snapshots {
		if snapshot.ID == results[0].Snapshot.ID {
			t.Fatal("oldest snapshot was not pruned")
		}
	}

	target := filepath.Join(root, "restored")
	if err := os.Mkdir(target, 0750); err != nil {
		t.Fatal(err)
	}
	if err := engine.Restore(ctx, "client-A", "volume-A", results[3].Snapshot.ID, target); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(target, "version")); err != nil || string(got) != "4" {
		t.Fatalf("restored version=%q err=%v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(target, "large.bin")); err != nil || string(got) != string(large) {
		t.Fatalf("shared data failed to restore (bytes=%d err=%v)", len(got), err)
	}
	linkInfo, err := os.Lstat(filepath.Join(target, "large-link"))
	if err != nil || linkInfo.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("symlink was not restored: info=%v err=%v", linkInfo, err)
	}
	executableInfo, err := os.Stat(filepath.Join(target, "executable"))
	if err != nil || executableInfo.Mode().Perm() != 0751 {
		t.Fatalf("permissions were not restored: mode=%v err=%v", executableInfo, err)
	}
}

func TestFailedBackupDoesNotPruneCompletedSnapshots(t *testing.T) {
	restic := requireRestic(t)
	ctx := context.Background()
	root := t.TempDir()
	repository := filepath.Join(root, "repository")
	cache := filepath.Join(root, "cache")
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(source, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(cache, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CELLBOX_TEST_VOLUME_BACKUP_PASSWORD", "test-only-volume-password")
	engine := localEngine(restic, repository, cache)
	engine.config.KeepLast = 4
	for version := 1; version <= 4; version++ {
		if err := os.WriteFile(filepath.Join(source, "version"), []byte(fmt.Sprint(version)), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.Backup(ctx, "client-A", "volume-B", source); err != nil {
			t.Fatal(err)
		}
	}
	before, err := engine.List(ctx, "client-A", "volume-B")
	if err != nil || len(before) != 4 {
		t.Fatalf("before failure: snapshots=%d err=%v", len(before), err)
	}
	engine.config.KeepLast = 3

	failingBinary := filepath.Join(root, "restic-partial")
	t.Setenv("CELLBOX_TEST_RESTIC_DELEGATE", restic)
	script := "#!/bin/sh\n\"$CELLBOX_TEST_RESTIC_DELEGATE\" \"$@\"\nstatus=$?\nif [ \"$status\" -eq 0 ]; then\n  for arg in \"$@\"; do\n    if [ \"$arg\" = backup ]; then exit 3; fi\n  done\nfi\nexit \"$status\"\n"
	if err := os.WriteFile(failingBinary, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	engine.config.Binary = failingBinary
	if _, err := engine.Backup(ctx, "client-A", "volume-B", source); err == nil {
		t.Fatal("partial backup reported success")
	}
	engine.config.Binary = restic
	after, err := engine.List(ctx, "client-A", "volume-B")
	if err != nil || len(after) != len(before) {
		t.Fatalf("failed backup changed successful retention: before=%d after=%d err=%v", len(before), len(after), err)
	}
}

func TestDisabledAndRestoreValidation(t *testing.T) {
	cfg := Config{}
	storage := objectstorage.Config{Bucket: "bucket", Prefix: "cellbox"}
	engine, err := New(cfg, storage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.List(context.Background(), "client", "volume"); !errors.Is(err, ErrDisabled) {
		t.Fatalf("List without configured password returned %v", err)
	}
	t.Setenv("CELLBOX_TEST_VOLUME_BACKUP_PASSWORD", "test-only")
	engine.config.PasswordEnv = "CELLBOX_TEST_VOLUME_BACKUP_PASSWORD"
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, "existing"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := engine.Restore(context.Background(), "client", "volume", strings.Repeat("a", 64), target); !errors.Is(err, ErrInvalid) {
		t.Fatalf("restore to nonempty target returned %v", err)
	}
	if err := engine.Restore(context.Background(), "client", "volume", "short", target); !errors.Is(err, ErrInvalid) {
		t.Fatalf("short snapshot ID returned %v", err)
	}
}

func TestRealResticS3IncrementalRetentionAndRestore(t *testing.T) {
	endpoint := os.Getenv("CELLBOX_TEST_BACKUP_S3_ENDPOINT")
	bucket := os.Getenv("CELLBOX_TEST_BACKUP_S3_BUCKET")
	accessKey := os.Getenv("CELLBOX_TEST_BACKUP_S3_ACCESS_KEY")
	secretKey := os.Getenv("CELLBOX_TEST_BACKUP_S3_SECRET_KEY")
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		t.Skip("set isolated CELLBOX_TEST_BACKUP_S3_* settings to run the real S3 backend test")
	}
	binary := requireRestic(t)
	t.Setenv("CELLBOX_TEST_VOLUME_BACKUP_PASSWORD", "test-only-volume-password")
	root := t.TempDir()
	source := filepath.Join(root, "source")
	target := filepath.Join(root, "restored")
	if err := os.Mkdir(source, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(target, 0750); err != nil {
		t.Fatal(err)
	}
	storage := objectstorage.Config{
		Endpoint: endpoint, Bucket: bucket, Region: "us-east-1", Prefix: "test/volume-backups/" + fmt.Sprint(time.Now().UnixNano()),
		PathStyle: true, AccessKeyEnv: "CELLBOX_TEST_BACKUP_S3_ACCESS_KEY", SecretKeyEnv: "CELLBOX_TEST_BACKUP_S3_SECRET_KEY",
	}
	engine, err := New(Config{Binary: binary, PasswordEnv: "CELLBOX_TEST_VOLUME_BACKUP_PASSWORD", KeepLast: 3}, storage)
	if err != nil {
		t.Fatal(err)
	}
	large := make([]byte, 3<<20)
	if _, err := rand.Read(large); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "shared.bin"), large, 0600); err != nil {
		t.Fatal(err)
	}
	var latest Result
	for version := 1; version <= 4; version++ {
		if err := os.WriteFile(filepath.Join(source, "version"), []byte(fmt.Sprint(version)), 0600); err != nil {
			t.Fatal(err)
		}
		latest, err = engine.Backup(context.Background(), "s3-client", "s3-volume", source)
		if err != nil {
			t.Fatalf("S3 backup %d: %v", version, err)
		}
	}
	if latest.Snapshot.SizeBytes == nil || latest.Snapshot.AddedBytes == nil || *latest.Snapshot.AddedBytes >= *latest.Snapshot.SizeBytes {
		t.Fatalf("S3 backup did not demonstrate deduplication: %+v", latest.Snapshot)
	}
	snapshots, err := engine.List(context.Background(), "s3-client", "s3-volume")
	if err != nil || len(snapshots) != 3 {
		t.Fatalf("S3 retention returned %d snapshots, err=%v", len(snapshots), err)
	}
	if err := os.RemoveAll(source); err != nil {
		t.Fatal(err)
	}
	if err := engine.Restore(context.Background(), "s3-client", "s3-volume", latest.Snapshot.ID, target); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(target, "version")); err != nil || string(got) != "4" {
		t.Fatalf("S3 restore version=%q err=%v", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(target, "shared.bin")); err != nil || string(got) != string(large) {
		t.Fatalf("S3 restore shared file bytes=%d err=%v", len(got), err)
	}
}

func requireRestic(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("CELLBOX_TEST_RESTIC")
	if binary == "" {
		var err error
		binary, err = exec.LookPath("restic")
		if err != nil {
			t.Skip("set CELLBOX_TEST_RESTIC to run real-restic repository tests")
		}
	}
	return binary
}

func localEngine(binary, repository, cache string) *Engine {
	cfg := Config{Binary: binary, PasswordEnv: "CELLBOX_TEST_VOLUME_BACKUP_PASSWORD", KeepLast: 3}
	_ = cfg.Validate()
	return &Engine{config: cfg, storage: objectstorage.Config{Region: "us-east-1", Bucket: "local"}, repositoryOverride: repository, cacheDir: cache}
}
