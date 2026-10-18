// SPDX-License-Identifier: Apache-2.0

// Package adapter translates an authorized sandbox Start to native runsc Restore.
// It deliberately preserves runsc's inherited file descriptors using syscall.Exec.
package adapter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"syscall"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/node"
)

var sandboxID = regexp.MustCompile(`^[a-f0-9]{64}$`)

type Adapter struct{ Base, Runsc string }

func argValue(args []string, key string) string {
	for i, s := range args {
		if strings.HasPrefix(s, key+"=") {
			return strings.TrimPrefix(s, key+"=")
		}
		if s == key && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}
func (a Adapter) Rewrite(args []string) ([]string, error) {
	idx := -1
	for i, s := range args {
		if s == "create" || s == "start" || s == "delete" {
			idx = i
			break
		}
	}
	if idx < 0 || len(args) == 0 {
		return args, nil
	}
	id := args[len(args)-1]
	if !sandboxID.MatchString(id) {
		return nil, fmt.Errorf("invalid container ID")
	}
	request := filepath.Join(a.Base, "requests", id+".json")
	switch args[idx] {
	case "create":
		bundle := argValue(args[idx+1:], "--bundle")
		var spec struct {
			Annotations map[string]string `json:"annotations"`
		}
		if err := node.ReadJSON(filepath.Join(bundle, "config.json"), &spec); err != nil {
			return nil, err
		}
		if spec.Annotations["io.kubernetes.cri.container-type"] == "container" {
			return args, a.childCreate(id, spec.Annotations, bundle)
		}
		if spec.Annotations["io.kubernetes.cri.container-type"] != "sandbox" {
			return nil, fmt.Errorf("unknown OCI container type")
		}
		podUID := spec.Annotations["io.kubernetes.cri.sandbox-uid"]
		if node.ValidID(podUID) {
			if _, err := os.Stat(filepath.Join(a.Base, "warm", podUID+".json")); err == nil {
				_, err = node.BindWarm(a.Base, podUID, spec.Annotations["io.kubernetes.cri.sandbox-name"], spec.Annotations["io.kubernetes.cri.sandbox-namespace"], id)
				return args, err
			} else if !os.IsNotExist(err) {
				return nil, err
			}
		}
		uid := spec.Annotations[api.TicketAnnotation]
		if !node.ValidID(uid) {
			return nil, fmt.Errorf("missing/invalid controller authorization; refusing cold start")
		}
		var t node.Ticket
		if err := node.ReadJSON(filepath.Join(a.Base, "tickets", uid+".json"), &t); err != nil {
			return nil, fmt.Errorf("authorization unavailable: %w", err)
		}
		if t.PodUID != uid || spec.Annotations["io.kubernetes.cri.sandbox-uid"] != uid || spec.Annotations["io.kubernetes.cri.sandbox-name"] != t.PodName || spec.Annotations["io.kubernetes.cri.sandbox-namespace"] != t.Namespace {
			return nil, fmt.Errorf("authorization Pod identity mismatch")
		}
		if err := a.verifyVolumeAttachments(t); err != nil {
			return nil, fmt.Errorf("persistent volume attachment: %w", err)
		}
		if t.Snapshot != "" {
			path, err := node.SnapshotPath(a.Base, t.OwnerUID, t.Snapshot)
			if err != nil {
				return nil, err
			}
			m, err := node.VerifyPrepared(path, t.OwnerUID, t.SpecHash, a.Runsc)
			if err != nil {
				return nil, fmt.Errorf("refusing restore: %w", err)
			}
			if err := verifyTicketManifest(t, m); err != nil {
				return nil, err
			}
		}
		// A Pod UID gets one sandbox execution, including across kubelet retries.
		claim := filepath.Join(a.Base, "claims", uid)
		if err := os.MkdirAll(filepath.Dir(claim), 0700); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(claim, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			_, err = f.WriteString(id)
			if err == nil {
				err = f.Sync()
			}
			f.Close()
			if err == nil {
				err = node.SyncDir(filepath.Dir(claim))
			}
			if err != nil {
				return nil, err
			}
		} else if os.IsExist(err) {
			b, e := os.ReadFile(claim)
			if e != nil || string(b) != id {
				return nil, fmt.Errorf("Pod execution already claimed; controller retry required")
			}
		} else {
			return nil, err
		}
		if err = node.AtomicJSON(request, t); err != nil {
			return nil, err
		}
	case "start":
		// Requests exist only for sandbox IDs, never for business container IDs.
		var t node.Ticket
		err := node.ReadJSON(request, &t)
		if os.IsNotExist(err) {
			return args, a.childStart(id)
		}
		if err != nil {
			return nil, err
		}
		if _, warmErr := os.Stat(filepath.Join(a.Base, "warm", t.PodUID+".json")); warmErr == nil {
			t, err = node.WaitWarm(a.Base, t.PodUID, id)
			if err != nil {
				return nil, err
			}
			if err = node.AtomicJSON(request, t); err != nil {
				return nil, err
			}
		} else if !os.IsNotExist(warmErr) {
			return nil, warmErr
		}

		var live node.Ticket
		if err = node.ReadJSON(filepath.Join(a.Base, "tickets", t.PodUID+".json"), &live); err != nil {
			return nil, fmt.Errorf("authorization revoked: %w", err)
		}
		if !reflect.DeepEqual(live, t) {
			return nil, fmt.Errorf("authorization changed")
		}
		started := request + ".started"
		f, err := os.OpenFile(started, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return nil, fmt.Errorf("sandbox start already attempted: %w", err)
		}
		err = f.Sync()
		f.Close()
		if err != nil {
			return nil, err
		}
		if err = node.SyncDir(filepath.Dir(started)); err != nil {
			return nil, err
		}
		if t.Snapshot != "" {
			path, err := node.SnapshotPath(a.Base, t.OwnerUID, t.Snapshot)
			if err != nil {
				return nil, err
			}
			m, err := node.VerifyPrepared(path, t.OwnerUID, t.SpecHash, a.Runsc)
			if err != nil {
				return nil, err
			}
			if err := verifyTicketManifest(t, m); err != nil {
				return nil, err
			}
			if err = a.claimHome(t); err != nil {
				return nil, err
			}
			// Resume execution as soon as kernel state is restored. Memory pages
			// load in the background; callers must check application readiness.
			return append(append([]string{}, args[:idx]...), "restore", "--background", "--detach", "--image-path="+path, id), nil
		}
		if err = a.claimHome(t); err != nil {
			return nil, err
		}
	}
	return args, nil
}

func (a Adapter) claimHome(t node.Ticket) error {
	for _, mount := range uniqueTicketMounts(ticketMounts(t)) {
		var err error
		if mount.Legacy {
			err = homevolume.ClaimExecution(a.Base, mount.UID, mount.HomeID, t.PodUID, t.Snapshot)
		} else {
			err = homevolume.ClaimAttachedExecution(a.Base, mount.UID, t.NodeName, t.OwnerUID, mount.HomeID, t.PodUID, t.Snapshot)
		}
		if err != nil {
			return fmt.Errorf("claim volume %s: %w", mount.UID, err)
		}
	}
	return nil
}

func ticketHomeSpec(t node.Ticket) string {
	if t.HomeVolumeUID != "" {
		return homevolume.VolumeSpecHash
	}
	return t.SpecHash
}

func (a Adapter) verifyHomeAttachment(t node.Ticket) error {
	return a.verifyVolumeAttachments(t)
}

func ticketMounts(t node.Ticket) []node.VolumeMountAuthorization {
	if t.Mounts != nil {
		return t.Mounts
	}
	if t.HomeID == "" {
		return nil
	}
	uid := t.HomeVolumeUID
	legacy := uid == ""
	if legacy {
		uid = t.OwnerUID
	}
	return []node.VolumeMountAuthorization{{UID: uid, MountPath: homevolume.MountPath, HomeID: t.HomeID, Legacy: legacy}}
}

func uniqueTicketMounts(mounts []node.VolumeMountAuthorization) []node.VolumeMountAuthorization {
	seen := make(map[string]struct{}, len(mounts))
	unique := make([]node.VolumeMountAuthorization, 0, len(mounts))
	for _, mount := range mounts {
		if _, exists := seen[mount.UID]; exists {
			continue
		}
		seen[mount.UID] = struct{}{}
		unique = append(unique, mount)
	}
	return unique
}

func (a Adapter) verifyVolumeAttachments(t node.Ticket) error {
	for _, mount := range uniqueTicketMounts(ticketMounts(t)) {
		var identity homevolume.Identity
		var err error
		if mount.Legacy {
			identity, err = homevolume.Verify(a.Base, mount.UID, "", t.SpecHash, mount.HomeID)
		} else {
			if t.NodeName == "" || t.OwnerUID == "" {
				return fmt.Errorf("incomplete volume authorization")
			}
			identity, err = homevolume.VerifyAttachment(a.Base, mount.UID, t.NodeName, t.OwnerUID)
		}
		if err != nil {
			return fmt.Errorf("volume %s unavailable: %w", mount.UID, err)
		}
		if identity.ID != mount.HomeID {
			return fmt.Errorf("volume %s identity mismatch", mount.UID)
		}
		if t.Snapshot != "" {
			if err = homevolume.ValidateCheckpoint(a.Base, mount.UID, mount.HomeID, t.Snapshot); err != nil {
				return fmt.Errorf("volume %s checkpoint: %w", mount.UID, err)
			}
		}
	}
	return nil
}

func verifyTicketManifest(t node.Ticket, manifest *node.Manifest) error {
	if t.Mounts != nil {
		if manifest.HomeID != "" || !reflect.DeepEqual(manifest.Mounts, t.Mounts) {
			return fmt.Errorf("checkpoint volume mount identity mismatch")
		}
		return nil
	}
	if manifest.HomeID != t.HomeID || len(manifest.Mounts) != 0 {
		return fmt.Errorf("checkpoint persistent HOME identity mismatch")
	}
	return nil
}

func verifyVolumeExecution(base string, t node.Ticket) error {
	for _, mount := range uniqueTicketMounts(ticketMounts(t)) {
		spec := homevolume.VolumeSpecHash
		if mount.Legacy {
			spec = t.SpecHash
		}
		if err := homevolume.VerifyExecution(base, mount.UID, spec, mount.HomeID, t.PodUID); err != nil {
			return fmt.Errorf("volume %s: %w", mount.UID, err)
		}
	}
	return nil
}
func Main() {
	a := Adapter{Base: node.DefaultBase, Runsc: node.DefaultRunsc}
	args, err := a.Rewrite(os.Args[1:])
	if err == nil {
		args, err = a.DiagnosticArgs(args)
	}
	if err == nil {
		err = syscall.Exec(a.Runsc, append([]string{a.Runsc}, args...), os.Environ())
	}
	// OCI JSON log is consumed by the shim. Stderr may already be a closed pipe.
	entry, _ := json.Marshal(map[string]string{"level": "error", "msg": err.Error(), "time": time.Now().UTC().Format(time.RFC3339Nano)})
	paths := []string{filepath.Join(a.Base, "adapter.log"), argValue(os.Args[1:], "--log")}
	if len(os.Args) > 1 && sandboxID.MatchString(os.Args[len(os.Args)-1]) {
		dir := filepath.Join(a.Base, "logs", os.Args[len(os.Args)-1])
		if os.MkdirAll(dir, 0700) == nil {
			paths = append(paths, filepath.Join(dir, "adapter.log"))
		}
	}
	for _, path := range paths {
		if path == "" {
			continue
		}
		f, e := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
		if e == nil {
			f.Write(append(entry, '\n'))
			f.Close()
		}
	}
	os.Exit(1)
}

func (a Adapter) childCreate(id string, annotations map[string]string, bundle string) error {
	sid := annotations["io.kubernetes.cri.sandbox-id"]
	if !sandboxID.MatchString(sid) {
		return fmt.Errorf("invalid child sandbox ID")
	}
	var t, live node.Ticket
	if err := node.ReadJSON(filepath.Join(a.Base, "requests", sid+".json"), &t); err != nil {
		return err
	}
	if t.PodUID != annotations["io.kubernetes.cri.sandbox-uid"] || t.PodName != annotations["io.kubernetes.cri.sandbox-name"] || t.Namespace != annotations["io.kubernetes.cri.sandbox-namespace"] {
		return fmt.Errorf("child Pod identity mismatch")
	}
	if err := node.ReadJSON(filepath.Join(a.Base, "tickets", t.PodUID+".json"), &live); err != nil {
		return err
	}
	if !reflect.DeepEqual(live, t) {
		return fmt.Errorf("child sandbox revoked")
	}
	if len(ticketMounts(t)) > 0 {
		if err := a.verifyVolumeAttachments(t); err != nil {
			return fmt.Errorf("persistent volume attachment: %w", err)
		}
		if err := verifyVolumeExecution(a.Base, t); err != nil {
			return fmt.Errorf("persistent volume execution: %w", err)
		}
		if err := a.claimHomeChild(sid, id); err != nil {
			return err
		}
		if err := a.bindHome(bundle, t); err != nil {
			return err
		}
	}
	return node.AtomicJSON(filepath.Join(a.Base, "children", id+".json"), node.ChildRecord{Ticket: t, SandboxID: sid})
}
func (a Adapter) childStart(id string) error {
	var c node.ChildRecord
	if err := node.ReadJSON(filepath.Join(a.Base, "children", id+".json"), &c); err != nil {
		return fmt.Errorf("missing container identity: %w", err)
	}
	var live, parent node.Ticket
	if err := node.ReadJSON(filepath.Join(a.Base, "tickets", c.Ticket.PodUID+".json"), &live); err != nil {
		return err
	}
	if err := node.ReadJSON(filepath.Join(a.Base, "requests", c.SandboxID+".json"), &parent); err != nil {
		return err
	}
	if !reflect.DeepEqual(live, c.Ticket) || !reflect.DeepEqual(parent, c.Ticket) {
		return fmt.Errorf("child sandbox revoked")
	}
	if len(ticketMounts(c.Ticket)) > 0 {
		if err := a.verifyVolumeAttachments(c.Ticket); err != nil {
			return fmt.Errorf("persistent volume attachment: %w", err)
		}
		if err := verifyVolumeExecution(a.Base, c.Ticket); err != nil {
			return fmt.Errorf("persistent volume execution: %w", err)
		}
		if err := a.claimHomeChild(c.SandboxID, id); err != nil {
			return err
		}
	}
	path := filepath.Join(a.Base, "children", id+".json.started")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = f.Sync(); err != nil {
		return err
	}
	return node.SyncDir(filepath.Dir(path))
}
