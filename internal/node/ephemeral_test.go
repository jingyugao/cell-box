package node

import (
	"os"
	"path/filepath"
	"testing"
)

// Readers must see either the old or the new record, never a partial one, and
// no temporary file may be left behind.
func TestEphemeralJSONReplacesAtomicallyAndLeavesNoTemporaryFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "warm", "slot.json")
	for _, phase := range []string{"waiting", "claimed"} {
		if err := EphemeralJSON(path, WarmLease{UID: "slot", Phase: phase}); err != nil {
			t.Fatal(err)
		}
		var got WarmLease
		if err := ReadJSON(path, &got); err != nil || got.Phase != phase {
			t.Fatalf("read %q after writing %q: %v", got.Phase, phase, err)
		}
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 1 {
		t.Fatalf("directory holds %d entries, want only the record: %v", len(entries), err)
	}
}

func TestCreateEphemeralIsExclusive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "claims", "pod")
	if err := CreateEphemeral(path, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := CreateEphemeral(path, []byte("second")); !os.IsExist(err) {
		t.Fatalf("second create = %v, want an already-exists error", err)
	}
	if data, _ := os.ReadFile(path); string(data) != "first" {
		t.Fatalf("claim overwritten: %q", data)
	}
}
