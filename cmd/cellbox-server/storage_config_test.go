package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSharedStorageFileAllowsDisabledStorageAndRejectsPartialConfig(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	if err := os.WriteFile(config, []byte(`{"profiles":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, content, want string }{
		{"disabled", `{}`, "warm pool size must be between 0 and 32"},
		{"partial", `{"prefix":"test"}`, "objectStorage.bucket is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := filepath.Join(dir, tc.name+".json")
			if err := os.WriteFile(storage, []byte(tc.content), 0600); err != nil {
				t.Fatal(err)
			}
			// Stop at runtime validation so this CLI regression cannot start HTTP
			// handlers or contact a cluster. Empty Chart storage must get this far.
			err := run([]string{"--config", config, "--object-storage-config", storage, "--warm-pool-size", "33"})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run error=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestEnabledVolumeBackupsLoadSharedStorageBeforeRuntimeValidation(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	storage := filepath.Join(dir, "object-storage.json")
	if err := os.WriteFile(config, []byte(`{"profiles":[],"volumeBackups":{"enabled":true}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(storage, []byte(`{"bucket":"test-backups"}`), 0600); err != nil {
		t.Fatal(err)
	}

	err := run([]string{"--config", config, "--object-storage-config", storage, "--warm-pool-size", "33"})
	if err == nil || !strings.Contains(err.Error(), "warm pool size must be between 0 and 32") {
		t.Fatalf("run error=%v, want warm pool runtime validation", err)
	}
}
