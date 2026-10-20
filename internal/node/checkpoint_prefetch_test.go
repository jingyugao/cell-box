// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLoadRestorePolicyMissingAndValidation(t *testing.T) {
	policy, err := LoadRestorePolicy(filepath.Join(t.TempDir(), "missing"))
	if err != nil || policy != (RestorePolicy{}) {
		t.Fatalf("missing policy = %+v, %v; want zero policy", policy, err)
	}
	if policy, err := LoadRestorePolicy(""); err != nil || policy != (RestorePolicy{}) {
		t.Fatalf("empty base policy = %+v, %v; want zero policy", policy, err)
	}

	tests := []struct {
		name   string
		policy RestorePolicy
		valid  bool
	}{
		{name: "default", valid: true},
		{name: "eager", policy: RestorePolicy{PageLoading: "eager"}, valid: true},
		{name: "lazy", policy: RestorePolicy{PageLoading: "lazy"}, valid: true},
		{name: "lazy capture only", policy: RestorePolicy{PageLoading: "lazy", PrefetchBinary: "/usr/local/bin/checkpointprefetch"}, valid: true},
		{name: "eager capture only", policy: RestorePolicy{PageLoading: "eager", PrefetchBinary: "/usr/local/bin/checkpointprefetch"}, valid: true},
		{name: "prefetch enabled", policy: RestorePolicy{PageLoading: "lazy", Prefetch: true, PrefetchBinary: "/usr/local/bin/checkpointprefetch"}, valid: true},
		{name: "unknown mode", policy: RestorePolicy{PageLoading: "automatic"}},
		{name: "prefetch eager", policy: RestorePolicy{PageLoading: "eager", Prefetch: true, PrefetchBinary: "/bin/tool"}},
		{name: "prefetch no binary", policy: RestorePolicy{PageLoading: "lazy", Prefetch: true}},
		{name: "relative binary", policy: RestorePolicy{PageLoading: "lazy", PrefetchBinary: "bin/tool"}},
		{name: "unclean binary", policy: RestorePolicy{PageLoading: "lazy", PrefetchBinary: "/bin/../bin/tool"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateRestorePolicy(test.policy)
			if (err == nil) != test.valid {
				t.Fatalf("validateRestorePolicy(%+v) = %v, valid=%t", test.policy, err, test.valid)
			}
		})
	}
}

func TestLoadRestorePolicyRejectsSymlinkAndNonRootFile(t *testing.T) {
	base := t.TempDir()
	regular := filepath.Join(base, "policy-data")
	if err := os.WriteFile(regular, []byte(`{"pageLoading":"lazy"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(regular, filepath.Join(base, restorePolicyFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRestorePolicy(base); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("symlink policy accepted: %v", err)
	}

	if err := os.Remove(filepath.Join(base, restorePolicyFile)); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(regular, filepath.Join(base, restorePolicyFile)); err != nil {
		t.Fatal(err)
	}
	if os.Geteuid() == 0 {
		t.Skip("non-root ownership check is only meaningful when tests run unprivileged")
	}
	if _, err := LoadRestorePolicy(base); err == nil || !strings.Contains(err.Error(), "root-owned") {
		t.Fatalf("unowned policy accepted: %v", err)
	}
}

func TestLoadRestorePolicyReadsRootOwnedOptIn(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root-owned policy file")
	}
	base := t.TempDir()
	if err := os.Chmod(base, 0700); err != nil {
		t.Fatal(err)
	}
	contents := `{"pageLoading":"lazy","prefetch":true,"prefetchBinary":"/usr/local/bin/checkpointprefetch"}`
	if err := os.WriteFile(filepath.Join(base, restorePolicyFile), []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	policy, err := LoadRestorePolicy(base)
	if err != nil {
		t.Fatal(err)
	}
	want := RestorePolicy{PageLoading: "lazy", Prefetch: true, PrefetchBinary: "/usr/local/bin/checkpointprefetch"}
	if policy != want {
		t.Fatalf("LoadRestorePolicy() = %+v, want %+v", policy, want)
	}

	if err := os.WriteFile(filepath.Join(base, restorePolicyFile), []byte(`{"pageLoading":"lazy","unknown":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRestorePolicy(base); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field was accepted: %v", err)
	}
	if err := os.WriteFile(filepath.Join(base, restorePolicyFile), []byte(`{"pageLoading":"lazy"} {}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRestorePolicy(base); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("trailing JSON was accepted: %v", err)
	}
	if err := os.WriteFile(filepath.Join(base, restorePolicyFile), []byte(`null`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRestorePolicy(base); err == nil || !strings.Contains(err.Error(), "JSON object") {
		t.Fatalf("null policy was accepted: %v", err)
	}
}

func TestPrefetchManagerCaptureUsesOutsideSidecarAndExactIdentity(t *testing.T) {
	base := t.TempDir()
	checkpointDir, err := SnapshotPath(base, "owner", "snapshot-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(checkpointDir, 0700); err != nil {
		t.Fatal(err)
	}
	tool := fakePrefetchTool(t, `
manifest=
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-manifest" ]; then shift; manifest="$1"; fi
  shift
done
printf '{"checkpoint_id":"owner:snapshot-1","captured":true}\n' > "$manifest"
echo capture-ok
`)
	manager := NewPrefetchManager(base, false)
	manager.loadPolicy = func(string) (RestorePolicy, error) {
		return RestorePolicy{PageLoading: "lazy", PrefetchBinary: tool}, nil
	}
	manifest, err := manager.Capture(context.Background(), "owner", "snapshot-1")
	if err != nil {
		t.Fatal(err)
	}
	want := checkpointDir + ".hotset.json"
	if manifest != want {
		t.Fatalf("manifest path = %q, want %q", manifest, want)
	}
	if strings.HasPrefix(manifest, checkpointDir+string(os.PathSeparator)) {
		t.Fatalf("manifest is inside checkpoint directory: %q", manifest)
	}
	var contents struct {
		Captured     bool   `json:"captured"`
		CheckpointID string `json:"checkpoint_id"`
	}
	if err := ReadJSON(manifest, &contents); err != nil || !contents.Captured || contents.CheckpointID != "owner:snapshot-1" {
		t.Fatalf("capture manifest = %v, %v", contents, err)
	}
	if _, err := manager.Capture(context.Background(), "owner", "../snapshot-1"); err == nil {
		t.Fatal("unsafe snapshot ID accepted")
	}
}

func TestPrefetchManagerConcurrentStartCancelAndStatusOutsideCheckpoint(t *testing.T) {
	base := t.TempDir()
	checkpointDir, manager := preparePrefetchTest(t, base)
	startedMarker := filepath.Join(base, "tool-started")
	countPath := filepath.Join(base, "tool-count")
	tool := fakePrefetchTool(t, `
echo invocation >> "$COUNT_PATH"
echo tool-started > "$STARTED_MARKER"
echo prefetch-running
exec sleep 30
`)
	manager.loadPolicy = func(string) (RestorePolicy, error) {
		return RestorePolicy{PageLoading: "lazy", Prefetch: true, PrefetchBinary: tool}, nil
	}
	t.Setenv("COUNT_PATH", countPath)
	t.Setenv("STARTED_MARKER", startedMarker)

	const callers = 12
	var wg sync.WaitGroup
	errCh := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- manager.Start("owner", "snapshot-1")
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	waitForFile(t, startedMarker)
	count, err := os.ReadFile(countPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(count), "invocation"); got != 1 {
		t.Fatalf("tool invocation count = %d, want 1", got)
	}

	statusPath, err := PrefetchStatusPath(base, "owner", "snapshot-1")
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(statusPath, checkpointDir+string(os.PathSeparator)) {
		t.Fatalf("status is inside checkpoint directory: %q", statusPath)
	}
	if err := manager.Cancel("owner", "snapshot-1"); err != nil {
		t.Fatal(err)
	}
	status, found, err := manager.Status("owner", "snapshot-1")
	if err != nil || !found {
		t.Fatalf("Status() = %+v, %t, %v", status, found, err)
	}
	if !status.Canceled || status.State != "canceled" || status.EndedAt.IsZero() || status.ExitCode == 0 || !strings.Contains(status.ToolOutput, "prefetch-running") {
		t.Fatalf("cancel result is incomplete: %+v", status)
	}
	if err := manager.Start("owner", "snapshot-1"); err != nil {
		t.Fatal(err)
	}
	count, err = os.ReadFile(countPath)
	if err != nil || strings.Count(string(count), "invocation") != 1 {
		t.Fatalf("completed identity was relaunched: count=%q err=%v", count, err)
	}
}

func TestPrefetchManagerRejectsManifestForAnotherCheckpoint(t *testing.T) {
	base := t.TempDir()
	checkpointDir, manager := preparePrefetchTest(t, base)
	manifest := checkpointDir + ".hotset.json"
	if err := os.WriteFile(manifest, []byte(`{"checkpoint_id":"owner:other-snapshot"}`), 0600); err != nil {
		t.Fatal(err)
	}
	manager.loadPolicy = func(string) (RestorePolicy, error) {
		return RestorePolicy{PageLoading: "lazy", Prefetch: true, PrefetchBinary: "/does/not/need/to/launch"}, nil
	}
	if err := manager.Start("owner", "snapshot-1"); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("wrong-checkpoint manifest accepted: %v", err)
	}
}

func TestPrefetchManagerCloseCancelsAndReaps(t *testing.T) {
	base := t.TempDir()
	_, manager := preparePrefetchTest(t, base)
	startedMarker := filepath.Join(base, "tool-started")
	tool := fakePrefetchTool(t, `echo tool-started > "$STARTED_MARKER"; echo close-running; exec sleep 30`)
	manager.loadPolicy = func(string) (RestorePolicy, error) {
		return RestorePolicy{PageLoading: "lazy", Prefetch: true, PrefetchBinary: tool}, nil
	}
	t.Setenv("STARTED_MARKER", startedMarker)
	if err := manager.Start("owner", "snapshot-1"); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, startedMarker)
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	status, found, err := manager.Status("owner", "snapshot-1")
	if err != nil || !found {
		t.Fatalf("Status() = %+v, %t, %v", status, found, err)
	}
	if !status.Canceled || status.State != "canceled" || status.EndedAt.IsZero() || !strings.Contains(status.ToolOutput, "close-running") {
		t.Fatalf("Close() did not durably reap process: %+v", status)
	}
	if err := manager.Start("owner", "snapshot-1"); err != nil {
		t.Fatal(err)
	}
}

func TestPrefetchManagerDefaultOffAndNilReceiver(t *testing.T) {
	var manager PrefetchManager
	if err := manager.Start("owner", "snapshot"); err != nil {
		t.Fatal(err)
	}
	if path, err := manager.Capture(context.Background(), "owner", "snapshot"); err != nil || path != "" {
		t.Fatalf("zero manager Capture() = %q, %v", path, err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	var nilManager *PrefetchManager
	if err := nilManager.Cancel("owner", "snapshot"); err != nil {
		t.Fatal(err)
	}
}

func preparePrefetchTest(t *testing.T, base string) (string, *PrefetchManager) {
	t.Helper()
	checkpointDir, err := SnapshotPath(base, "owner", "snapshot-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(checkpointDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkpointDir, "checkpoint.img"), []byte("checkpoint"), 0600); err != nil {
		t.Fatal(err)
	}
	manifest := checkpointDir + ".hotset.json"
	if err := os.WriteFile(manifest, []byte(`{"checkpoint_id":"owner:snapshot-1"}`), 0600); err != nil {
		t.Fatal(err)
	}
	return checkpointDir, NewPrefetchManager(base, false)
}

func fakePrefetchTool(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "checkpointprefetch")
	contents := "#!/bin/sh\nset -eu\n" + script + "\n"
	if err := os.WriteFile(path, []byte(contents), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}
