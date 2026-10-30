package node

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Leases are written without fsync, so a power loss can leave one empty or cut
// short. That must not make every reap cycle fail forever, but a lease whose
// Pod still exists is not ours to delete.
func TestReapDropsAnUnreadableLeaseOnlyWhenNoPodHasThatUID(t *testing.T) {
	b := &Backend{Base: t.TempDir()}
	dir := filepath.Join(b.Base, "warm")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"gone.json", "alive.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(`{"uid":"x","na`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// The live Pod's lease stays a reported problem; the vanished one is not.
	err := b.ReapWarm(context.Background(), "cellbox", map[string]bool{"alive": true})
	if err == nil || !strings.Contains(err.Error(), "alive.json") || strings.Contains(err.Error(), "gone.json") {
		t.Fatalf("reap error = %v, want a failure for alive.json only", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "gone.json")); !os.IsNotExist(err) {
		t.Fatalf("a torn lease of a vanished Pod was kept: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "alive.json")); err != nil {
		t.Fatalf("a torn lease of a live Pod was removed: %v", err)
	}
}
