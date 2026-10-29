package adapter

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/jingyugao/cell-box/internal/guestapi"
)

func guestArgv(t *testing.T, instanceID string, env map[string]string) []string {
	t.Helper()
	config := guestapi.DefaultConfig()
	config.Env = map[string]string{}
	for k, v := range env {
		config.Env[k] = v
	}
	if instanceID != "" {
		config.Env[guestapi.InstanceIDEnv] = instanceID
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return []string{guestapi.Binary, "serve", "--config-base64", base64.StdEncoding.EncodeToString(data)}
}

func writeBundle(t *testing.T, args []string) string {
	t.Helper()
	dir := t.TempDir()
	spec := map[string]any{
		"ociVersion":  "1.0.0",
		"process":     map[string]any{"args": args, "cwd": "/home/agent/workspace"},
		"mounts":      []any{map[string]any{"destination": "/home/agent", "type": "bind"}},
		"x-extension": map[string]bool{"keep": true},
	}
	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func bundleSpec(t *testing.T, dir string) (args []string, rest map[string]json.RawMessage) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &rest); err != nil {
		t.Fatal(err)
	}
	var process struct {
		Args []string `json:"args"`
		Cwd  string   `json:"cwd"`
	}
	if err := json.Unmarshal(rest["process"], &process); err != nil || process.Cwd != "/home/agent/workspace" {
		t.Fatalf("process section damaged: %+v %v", process, err)
	}
	return process.Args, rest
}

// A warm slot is built from one Box and claimed by another. The container must
// carry the claiming Box's command line, exactly as its checkpoint saved it.
func TestWarmSlotContainerTakesTheClaimingBoxsArguments(t *testing.T) {
	built := guestArgv(t, "guest-built-from", nil)
	claimant := guestArgv(t, "guest-claimant", nil)
	dir := writeBundle(t, built)
	if err := setBundleGuestArgv(dir, claimant); err != nil {
		t.Fatal(err)
	}
	args, rest := bundleSpec(t, dir)
	if !reflect.DeepEqual(args, claimant) {
		t.Fatalf("args = %v, want the claimant's", args)
	}
	for _, key := range []string{"ociVersion", "mounts", "x-extension"} {
		if _, ok := rest[key]; !ok {
			t.Fatalf("%s was lost from the OCI spec", key)
		}
	}
}

// Boxes created before the marker existed have none; a slot built from a newer
// Box must still be usable by them.
func TestBoxWithoutAMarkerCanClaimASlotBuiltFromOneWithAMarker(t *testing.T) {
	older := guestArgv(t, "", nil)
	dir := writeBundle(t, guestArgv(t, "guest-built-from", nil))
	if err := setBundleGuestArgv(dir, older); err != nil {
		t.Fatal(err)
	}
	if args, _ := bundleSpec(t, dir); !reflect.DeepEqual(args, older) {
		t.Fatalf("args = %v, want %v", args, older)
	}
}

// Only the marker may differ. Anything else means the slot does not match the
// Box, and rewriting it would hide that.
func TestArgumentsThatDifferBeyondTheMarkerAreRefusedAndLeftAlone(t *testing.T) {
	dir := writeBundle(t, guestArgv(t, "guest-built-from", map[string]string{"TOOL": "a"}))
	before, _ := os.ReadFile(filepath.Join(dir, "config.json"))
	if err := setBundleGuestArgv(dir, guestArgv(t, "guest-claimant", map[string]string{"TOOL": "b"})); err == nil {
		t.Fatal("a slot with different guest settings was accepted")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "config.json"))
	if string(before) != string(after) {
		t.Fatal("the bundle was modified despite the refusal")
	}
}

func TestTicketsWithoutGuestArgumentsLeaveTheBundleAlone(t *testing.T) {
	dir := writeBundle(t, guestArgv(t, "guest-built-from", nil))
	before, _ := os.ReadFile(filepath.Join(dir, "config.json"))
	if err := setBundleGuestArgv(dir, nil); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "config.json"))
	if string(before) != string(after) {
		t.Fatal("a ticket from before this field existed changed the bundle")
	}
}
