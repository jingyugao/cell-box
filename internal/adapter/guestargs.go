package adapter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/jingyugao/cell-box/internal/guestapi"
	"github.com/jingyugao/cell-box/internal/node"
)

// setBundleGuestArgv gives a warm slot's pre-created Guest container the command
// line of the Box that claimed it. The slot was built from another Box with the
// same runtime profile, so the only legitimate difference is the instance
// marker; gVisor refuses to restore a container whose arguments differ from the
// checkpoint's. Anything else that differs is refused rather than rewritten.
func setBundleGuestArgv(bundle string, want []string) error {
	if len(want) == 0 {
		return nil // Tickets written before this field existed.
	}
	if bundle == "" {
		return fmt.Errorf("missing child bundle for Guest arguments")
	}
	path := filepath.Join(bundle, "config.json")
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("child OCI spec must be a regular file")
	}
	var spec map[string]json.RawMessage
	if err := node.ReadJSON(path, &spec); err != nil {
		return err
	}
	var process map[string]json.RawMessage
	if err := json.Unmarshal(spec["process"], &process); err != nil || process == nil {
		return fmt.Errorf("child OCI spec has no process")
	}
	var have []string
	if err := json.Unmarshal(process["args"], &have); err != nil {
		return fmt.Errorf("child OCI process has no arguments: %w", err)
	}
	if slices.Equal(have, want) {
		return nil
	}
	if !slices.Equal(guestapi.WithoutInstanceID(have), guestapi.WithoutInstanceID(want)) {
		return fmt.Errorf("refusing restore: Guest arguments differ beyond the instance marker")
	}
	if process["args"], err = json.Marshal(want); err != nil {
		return err
	}
	if spec["process"], err = json.Marshal(process); err != nil {
		return err
	}
	return node.EphemeralJSON(path, spec)
}
