package objectstorage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func newTestLocal(t *testing.T, dir string) *Local {
	t.Helper()
	local, err := NewLocal(dir)
	if err != nil {
		t.Fatalf("NewLocal: %v", err)
	}
	t.Cleanup(func() {
		if err := local.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return local
}

func localPut(t *testing.T, store *Local, key, value, condition string) string {
	t.Helper()
	etag, err := store.Put(context.Background(), key, strings.NewReader(value), int64(len(value)), condition)
	if err != nil {
		t.Fatalf("Put(%q): %v", key, err)
	}
	return etag
}

func localRead(t *testing.T, store *Local, key string) (string, string) {
	t.Helper()
	reader, size, etag, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read Get(%q): %v", key, err)
	}
	if int64(len(data)) != size {
		t.Fatalf("Get(%q) size = %d, want %d", key, len(data), size)
	}
	return string(data), etag
}

func TestLocalCrossInstanceCreateOnlyCAS(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "objects")
	first := newTestLocal(t, dir)
	second := newTestLocal(t, dir)

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i, store := range []*Local{first, second} {
		wg.Add(1)
		go func(i int, store *Local) {
			defer wg.Done()
			<-start
			value := []string{"one", "two"}[i]
			_, err := store.Put(context.Background(), "state/item", strings.NewReader(value), int64(len(value)), "*")
			results <- err
		}(i, store)
	}
	close(start)
	wg.Wait()
	close(results)

	succeeded, conflicted := 0, 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrConflict):
			conflicted++
		default:
			t.Fatalf("unexpected concurrent Put error: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("success/conflict = %d/%d, want 1/1", succeeded, conflicted)
	}
}

func TestLocalGetConcurrentWithAtomicReplacement(t *testing.T) {
	store := newTestLocal(t, filepath.Join(t.TempDir(), "objects"))
	values := []string{"small", strings.Repeat("larger-payload-", 4096)}
	localPut(t, store, "state/current", values[0], "")

	const iterations = 300
	start := make(chan struct{})
	errs := make(chan error, 4)
	var wg sync.WaitGroup
	for reader := 0; reader < 3; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < iterations; i++ {
				body, size, etag, err := store.Get(context.Background(), "state/current")
				if err != nil {
					errs <- err
					return
				}
				data, readErr := io.ReadAll(body)
				closeErr := body.Close()
				if readErr != nil {
					errs <- readErr
					return
				}
				if closeErr != nil {
					errs <- closeErr
					return
				}
				matched := false
				for _, want := range values {
					if string(data) == want && size == int64(len(want)) && etag == localETag([]byte(want)) {
						matched = true
						break
					}
				}
				if !matched {
					errs <- errors.New("Get returned a torn or mismatched object version")
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			value := values[i%len(values)]
			if _, err := store.Put(context.Background(), "state/current", strings.NewReader(value), int64(len(value)), ""); err != nil {
				errs <- err
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent Get/Put: %v", err)
	}
}

func TestLocalConditionalDeletePreservesNewerObject(t *testing.T) {
	store := newTestLocal(t, filepath.Join(t.TempDir(), "objects"))
	oldETag := localPut(t, store, "checkpoint/meta", "old", "")
	newETag := localPut(t, store, "checkpoint/meta", "new", "")
	if oldETag == newETag {
		t.Fatal("different payloads got the same ETag")
	}
	if err := store.DeleteIfMatch(context.Background(), "checkpoint/meta", oldETag); !errors.Is(err, ErrConflict) {
		t.Fatalf("DeleteIfMatch(old ETag) = %v, want ErrConflict", err)
	}
	got, gotETag := localRead(t, store, "checkpoint/meta")
	if got != "new" || gotETag != newETag {
		t.Fatalf("object after stale delete = (%q, %q), want newer value", got, gotETag)
	}
	if err := store.DeleteIfMatch(context.Background(), "checkpoint/missing", oldETag); err != nil {
		t.Fatalf("DeleteIfMatch(missing) = %v, want nil", err)
	}
}

func TestLocalImportKeepsETagAcrossRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "objects")
	const originalETag = `"multipart-opaque-tag-2"`
	first := newTestLocal(t, dir)
	if err := first.Import(context.Background(), "metadata/state.json.gz", strings.NewReader("exact bytes\x00\n"), int64(len("exact bytes\x00\n")), originalETag); err != nil {
		t.Fatalf("Import: %v", err)
	}
	if err := first.Import(context.Background(), "metadata/state.json.gz", strings.NewReader("exact bytes\x00\n"), int64(len("exact bytes\x00\n")), originalETag); err != nil {
		t.Fatalf("idempotent Import: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second := newTestLocal(t, dir)
	got, gotETag := localRead(t, second, "metadata/state.json.gz")
	if got != "exact bytes\x00\n" || gotETag != originalETag {
		t.Fatalf("imported object = (%q, %q), want exact bytes and original ETag", got, gotETag)
	}
	if err := second.Import(context.Background(), "metadata/state.json.gz", strings.NewReader("different"), int64(len("different")), originalETag); !errors.Is(err, ErrConflict) {
		t.Fatalf("Import with changed bytes = %v, want ErrConflict", err)
	}
}

func TestLocalRejectsTraversalAndSymlinks(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "objects")
	store := newTestLocal(t, dir)
	for _, key := range []string{"../escape", "a/../escape", "/absolute", `a\b`, ".lock"} {
		if _, err := store.Put(context.Background(), key, strings.NewReader("x"), 1, ""); err == nil {
			t.Errorf("Put(%q) unexpectedly succeeded", key)
		}
	}

	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "link")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, _, _, err := store.Get(context.Background(), "link"); err == nil {
		t.Fatal("Get followed an object symlink")
	}
	if _, err := store.List(context.Background(), ""); err == nil {
		t.Fatal("List accepted a symlink in the object tree")
	}
	if _, err := store.Put(context.Background(), "link/child", strings.NewReader("x"), 1, ""); err == nil {
		t.Fatal("Put traversed an object symlink")
	}
}

func TestLocalListAndDeletePrefixUseDirectoryBoundary(t *testing.T) {
	store := newTestLocal(t, filepath.Join(t.TempDir(), "objects"))
	localPut(t, store, "a/one", "1", "")
	localPut(t, store, "ab/two", "2", "")
	localPut(t, store, "a/three", "3", "")

	keys, err := store.List(context.Background(), "a/")
	if err != nil {
		t.Fatalf("List(a/): %v", err)
	}
	if strings.Join(keys, ",") != "a/one,a/three" {
		t.Fatalf("List(a/) = %v", keys)
	}
	if err = store.DeletePrefix(context.Background(), "a"); err != nil {
		t.Fatalf("DeletePrefix(a): %v", err)
	}
	keys, err = store.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List(all): %v", err)
	}
	if strings.Join(keys, ",") != "ab/two" {
		t.Fatalf("keys after DeletePrefix(a) = %v, want [ab/two]", keys)
	}
}
