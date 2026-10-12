package adapter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/node"
)

// bindHome runs before runsc create reads the child OCI spec. The root sandbox
// can already be waiting to restore: the child's Gofer is created at start.
// Preserve all unrelated OCI fields, including extensions unknown to Cellbox.
func (a Adapter) bindHome(bundle string, t node.Ticket) error {
	if bundle == "" {
		return fmt.Errorf("missing child bundle for persistent HOME")
	}
	source, err := homevolume.Path(a.Base, t.OwnerUID)
	if err != nil {
		return err
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
	found := false
	for _, raw := range mounts {
		var mount struct {
			Destination string          `json:"destination"`
			Source      string          `json:"source"`
			Type        string          `json:"type"`
			Options     []string        `json:"options"`
			UIDMappings json.RawMessage `json:"uidMappings"`
			GIDMappings json.RawMessage `json:"gidMappings"`
		}
		if err := json.Unmarshal(raw, &mount); err != nil {
			return fmt.Errorf("invalid OCI mount: %w", err)
		}
		dest := filepath.Clean(mount.Destination)
		if dest != homevolume.MountPath {
			if strings.HasPrefix(homevolume.MountPath, dest+"/") || dest == "/" || strings.HasPrefix(dest, homevolume.MountPath+"/") {
				return fmt.Errorf("OCI mount overlaps persistent HOME: %q", mount.Destination)
			}
			continue
		}
		// Cold Pods keep kubelet's existing HOME bind mount. It must refer to
		// exactly the authorized directory with the same restore-compatible
		// options used below; never overwrite an unexpected mount silently.
		slices.Sort(mount.Options)
		if found || mount.Destination != homevolume.MountPath || mount.Source != source || mount.Type != "bind" ||
			!slices.Equal(mount.Options, []string{"rbind", "rprivate", "rw"}) ||
			(len(mount.UIDMappings) > 0 && string(mount.UIDMappings) != "null" && string(mount.UIDMappings) != "[]") ||
			(len(mount.GIDMappings) > 0 && string(mount.GIDMappings) != "null" && string(mount.GIDMappings) != "[]") {
			return fmt.Errorf("OCI persistent HOME mount does not match authorization")
		}
		found = true
	}
	if found {
		return nil
	}
	mount, err := json.Marshal(map[string]any{
		"destination": homevolume.MountPath, "source": source, "type": "bind",
		"options": []string{"rbind", "rprivate", "rw"},
	})
	if err != nil {
		return err
	}
	mounts = append(mounts, mount)
	spec["mounts"], err = json.Marshal(mounts)
	if err != nil {
		return err
	}
	return node.AtomicJSON(path, spec)
}

// A Box has exactly one business container. A second child ID must not obtain
// another writable Gofer for the same HOME, even after a failed OCI create.
func (a Adapter) claimHomeChild(sid, id string) error {
	path := filepath.Join(a.Base, "requests", sid+".json.child")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if os.IsExist(err) {
		b, err := os.ReadFile(path)
		if err != nil || string(b) != id {
			return fmt.Errorf("persistent HOME already assigned to another child")
		}
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(id); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return node.SyncDir(filepath.Dir(path))
}
