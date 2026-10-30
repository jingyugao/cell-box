package adapter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jingyugao/cell-box/internal/homevolume"
	"github.com/jingyugao/cell-box/internal/node"
)

// bindHome updates the child OCI mounts before its Gofer is created. Deferred
// restore can bind after child create; runsc refreshes its saved mounts at restore.
// Preserve all unrelated OCI fields, including extensions unknown to Cellbox.
func (a Adapter) bindHome(bundle string, t node.Ticket) error {
	if bundle == "" {
		return fmt.Errorf("missing child bundle for persistent HOME")
	}
	authorized := ticketMounts(t)
	if len(authorized) == 0 {
		return nil
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
	if spec == nil {
		return fmt.Errorf("missing child OCI spec")
	}
	var mounts []json.RawMessage
	if raw, ok := spec["mounts"]; ok {
		if err := json.Unmarshal(raw, &mounts); err != nil {
			return fmt.Errorf("invalid OCI mounts: %w", err)
		}
	}
	managedDestinations := make(map[string]struct{}, len(authorized))
	for _, authorization := range authorized {
		managedDestinations[authorization.MountPath] = struct{}{}
	}
	kept := make([]json.RawMessage, 0, len(mounts)+len(authorized))
	for _, raw := range mounts {
		var mount struct {
			Destination string `json:"destination"`
		}
		if err := json.Unmarshal(raw, &mount); err != nil {
			return fmt.Errorf("invalid OCI mount: %w", err)
		}
		if _, managed := managedDestinations[mount.Destination]; !managed {
			kept = append(kept, raw)
		}
	}
	for _, authorization := range authorized {
		source, err := homevolume.Path(a.Base, authorization.UID)
		if err != nil {
			return err
		}
		wantRO := "rw"
		if authorization.ReadOnly {
			wantRO = "ro"
		}
		wantOptions := []string{"rbind", "rprivate", wantRO}
		mount, err := json.Marshal(map[string]any{
			"destination": authorization.MountPath, "source": source, "type": "bind",
			"options": wantOptions,
		})
		if err != nil {
			return err
		}
		kept = append(kept, mount)
	}
	spec["mounts"], err = json.Marshal(kept)
	if err != nil {
		return err
	}
	return node.EphemeralJSON(path, spec)
}

// A Box has exactly one business container. A second child ID must not obtain
// another writable Gofer for the same HOME, even after a failed OCI create.
func (a Adapter) claimHomeChild(sid, id string) error {
	path := filepath.Join(a.Base, "requests", sid+".json.child")
	err := node.CreateEphemeral(path, []byte(id))
	if os.IsExist(err) {
		b, err := os.ReadFile(path)
		if err != nil || string(b) != id {
			return fmt.Errorf("persistent HOME already assigned to another child")
		}
		return nil
	}
	return err
}
