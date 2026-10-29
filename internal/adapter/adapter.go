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
	"strconv"
	"strings"
	"syscall"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/node"
)

var sandboxID = regexp.MustCompile(`^[a-f0-9]{64}$`)

// Background load modes for runsc restore --background-load.
const (
	BackgroundLoadLazy  = "lazy"
	BackgroundLoadEager = "eager"
)

type Adapter struct {
	Base, Runsc     string
	DeferCheckpoint bool
	NoRootContainer bool
	PodUIDAuth      bool
	// BackgroundLoad selects how checkpoint pages are read after restore. The
	// zero value means lazy: pages load when the application faults on them.
	BackgroundLoad string
}

// backgroundLoadFlag returns the runsc flag for restores that bind a
// checkpoint. The deferred root restore carries no checkpoint, so it needs none.
func (a Adapter) backgroundLoadFlag() string {
	if a.BackgroundLoad == "" {
		return "--background-load=" + BackgroundLoadLazy
	}
	return "--background-load=" + a.BackgroundLoad
}

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

func markStartAttempt(request string) error {
	started := request + ".started"
	f, err := os.OpenFile(started, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return node.SyncDir(filepath.Dir(started))
}

func (a Adapter) Rewrite(args []string) ([]string, error) {
	if a.DeferCheckpoint && !a.NoRootContainer {
		return nil, fmt.Errorf("CELLBOX_DEFER_CHECKPOINT requires CELLBOX_NO_ROOT_CONTAINER")
	}
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
		if a.NoRootContainer && !hasNoRootContainerFlag(args[idx+1:]) {
			args = append(append(args[:len(args)-1], "--no-root-container"), id)
		}
		podUID := spec.Annotations["io.kubernetes.cri.sandbox-uid"]
		if node.ValidID(podUID) {
			if _, err := os.Stat(filepath.Join(a.Base, "warm", podUID+".json")); err == nil {
				_, err = node.BindWarm(a.Base, podUID, spec.Annotations["io.kubernetes.cri.sandbox-name"], spec.Annotations["io.kubernetes.cri.sandbox-namespace"], id)
				if err == nil && hasNoRootContainerFlag(args[idx+1:]) {
					err = normalizeNoRootBundle(bundle)
				}
				return args, err
			} else if !os.IsNotExist(err) {
				return nil, err
			}
		}
		uid := spec.Annotations[api.TicketAnnotation]
		if uid == "" && a.PodUIDAuth {
			uid = podUID
		}
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
		if err := a.verifyVolumeIdentities(t); err != nil {
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
		if hasNoRootContainerFlag(args[idx+1:]) {
			if err = normalizeNoRootBundle(bundle); err != nil {
				return nil, err
			}
		}
	case "start":
		// Requests exist only for sandbox IDs, never for business container IDs.
		var t node.Ticket
		err := node.ReadJSON(request, &t)
		if os.IsNotExist(err) {
			return a.childStartArgs(args, idx, id)
		}
		if err != nil {
			return nil, err
		}
		if _, warmErr := os.Stat(filepath.Join(a.Base, "warm", t.PodUID+".json")); warmErr == nil {
			if a.DeferCheckpoint {
				if err = node.MarkDeferredRestore(a.Base, t.PodUID, t.PodName, t.Namespace, id); err != nil {
					return nil, err
				}
				if err = markStartAttempt(request); err != nil {
					return nil, fmt.Errorf("sandbox start already attempted: %w", err)
				}
				return append(append([]string{}, args[:idx]...), "restore", "--defer-checkpoint", "--background", "--detach", id), nil
			}
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
		if err = markStartAttempt(request); err != nil {
			return nil, fmt.Errorf("sandbox start already attempted: %w", err)
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
			if err := node.ConsumeSnapshot(a.Base, t.OwnerUID, t.Snapshot); err != nil {
				return nil, fmt.Errorf("refusing snapshot replay: %w", err)
			}
			if err = a.claimHome(t); err != nil {
				return nil, err
			}
			// Resume execution as soon as kernel state is restored. Memory pages
			// load in the background; callers must check application readiness.
			return append(append([]string{}, args[:idx]...), "restore", "--background", a.backgroundLoadFlag(), "--detach", "--image-path="+path, id), nil
		}
		if err = a.claimHome(t); err != nil {
			return nil, err
		}
	}
	return args, nil
}

func hasNoRootContainerFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--no-root-container" {
			return true
		}
	}
	return false
}

// normalizeNoRootBundle removes fields that gVisor rejects for a no-root
// sandbox. The raw-message map preserves OCI extensions and all other fields.
func normalizeNoRootBundle(bundle string) error {
	if bundle == "" {
		return fmt.Errorf("missing root bundle for no-root container")
	}
	path := filepath.Join(bundle, "config.json")
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("root OCI spec must be a regular file")
	}
	var spec map[string]json.RawMessage
	if err := node.ReadJSON(path, &spec); err != nil {
		return err
	}
	if spec == nil {
		return fmt.Errorf("missing root OCI spec")
	}
	delete(spec, "process")
	delete(spec, "root")
	return node.AtomicJSON(path, spec)
}

func (a Adapter) claimHome(t node.Ticket) error {
	for _, mount := range uniqueTicketMounts(ticketMounts(t)) {
		if !mount.Legacy {
			// Volume identity is shared durable state. A sandbox's runtime/checkpoint
			// fences belong to its ticket and manifest, not to this volume.
			continue
		}
		var err error
		err = homevolume.ClaimExecution(a.Base, mount.UID, mount.HomeID, t.PodUID, t.Snapshot)
		if err != nil {
			return fmt.Errorf("claim volume %s: %w", mount.UID, err)
		}
	}
	return nil
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

func (a Adapter) verifyVolumeIdentities(t node.Ticket) error {
	for _, mount := range uniqueTicketMounts(ticketMounts(t)) {
		var identity homevolume.Identity
		var err error
		if mount.Legacy {
			identity, err = homevolume.Verify(a.Base, mount.UID, "", t.SpecHash, mount.HomeID)
		} else {
			if t.NodeName == "" || t.OwnerUID == "" {
				return fmt.Errorf("incomplete volume authorization")
			}
			identity, err = homevolume.Verify(a.Base, mount.UID, t.NodeName, homevolume.VolumeSpecHash, mount.HomeID)
		}
		if err != nil {
			return fmt.Errorf("volume %s unavailable: %w", mount.UID, err)
		}
		if identity.ID != mount.HomeID {
			return fmt.Errorf("volume %s identity mismatch", mount.UID)
		}
		if t.Snapshot != "" && mount.Legacy {
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
		if !mount.Legacy {
			continue
		}
		spec := t.SpecHash
		if err := homevolume.VerifyExecution(base, mount.UID, spec, mount.HomeID, t.PodUID); err != nil {
			return fmt.Errorf("volume %s: %w", mount.UID, err)
		}
	}
	return nil
}
func adapterFromEnv() (Adapter, error) {
	a := Adapter{Base: node.DefaultBase, Runsc: node.DefaultRunsc}
	if base := os.Getenv("CELLBOX_NODE_BASE"); base != "" {
		a.Base = base
	}
	if runsc := os.Getenv("CELLBOX_RUNSC"); runsc != "" {
		a.Runsc = runsc
	}
	if value := os.Getenv("CELLBOX_DEFER_CHECKPOINT"); value != "" {
		deferred, err := strconv.ParseBool(value)
		if err != nil {
			return a, fmt.Errorf("invalid CELLBOX_DEFER_CHECKPOINT: %w", err)
		}
		a.DeferCheckpoint = deferred
	}
	if value := os.Getenv("CELLBOX_NO_ROOT_CONTAINER"); value != "" {
		noRoot, err := strconv.ParseBool(value)
		if err != nil {
			return a, fmt.Errorf("invalid CELLBOX_NO_ROOT_CONTAINER: %w", err)
		}
		a.NoRootContainer = noRoot
	}
	if value := os.Getenv("CELLBOX_BACKGROUND_LOAD"); value != "" {
		if value != BackgroundLoadLazy && value != BackgroundLoadEager {
			return a, fmt.Errorf("invalid CELLBOX_BACKGROUND_LOAD %q: want %s or %s", value, BackgroundLoadLazy, BackgroundLoadEager)
		}
		a.BackgroundLoad = value
	}
	if value := os.Getenv("CELLBOX_POD_UID_AUTH"); value != "" {
		authorizedByUID, err := strconv.ParseBool(value)
		if err != nil {
			return a, fmt.Errorf("invalid CELLBOX_POD_UID_AUTH: %w", err)
		}
		a.PodUIDAuth = authorizedByUID
	}
	return a, nil
}

func Main() {
	a, err := adapterFromEnv()
	args := os.Args[1:]
	if err == nil {
		args, err = a.Rewrite(args)
	}
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
	deferredRestore := false
	if _, err := os.Stat(filepath.Join(a.Base, "warm", t.PodUID+".json")); err == nil {
		deferredRestore, err = node.DeferredRestore(a.Base, t.PodUID, t.PodName, t.Namespace, sid)
		if err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if deferredRestore {
		// Precreate the child without Box-specific mounts. Its start waits for
		// assignment and binds the volumes before runsc creates the Gofer.
		if bundle == "" {
			return fmt.Errorf("missing child bundle for deferred restore")
		}
		bundle, err := filepath.Abs(bundle)
		if err != nil {
			return err
		}
		if _, err := os.Stat(filepath.Join(a.Base, "requests", sid+".json.started")); err != nil {
			return fmt.Errorf("deferred root start was not recorded: %w", err)
		}
		if err := a.claimHomeChild(sid, id); err != nil {
			return err
		}
		return node.AtomicJSON(filepath.Join(a.Base, "children", id+".json"), node.ChildRecord{Ticket: t, SandboxID: sid, DeferredRestore: true, BundleDir: bundle})
	}
	if err := node.ReadJSON(filepath.Join(a.Base, "tickets", t.PodUID+".json"), &live); err != nil {
		return err
	}
	if !reflect.DeepEqual(live, t) {
		return fmt.Errorf("child sandbox revoked")
	}
	if len(ticketMounts(t)) > 0 {
		if err := a.verifyVolumeIdentities(t); err != nil {
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
	return node.AtomicJSON(filepath.Join(a.Base, "children", id+".json"), node.ChildRecord{Ticket: t, SandboxID: sid, DeferredRestore: deferredRestore})
}
func (a Adapter) childStart(id string) error {
	_, err := a.childStartArgs([]string{"start", id}, 0, id)
	return err
}

func (a Adapter) childStartArgs(args []string, idx int, id string) ([]string, error) {
	var c node.ChildRecord
	if err := node.ReadJSON(filepath.Join(a.Base, "children", id+".json"), &c); err != nil {
		return nil, fmt.Errorf("missing container identity: %w", err)
	}
	if c.DeferredRestore {
		t, err := node.WaitWarm(a.Base, c.Ticket.PodUID, c.SandboxID)
		if err != nil {
			return nil, err
		}
		if t.PodUID != c.Ticket.PodUID || t.PodName != c.Ticket.PodName || t.Namespace != c.Ticket.Namespace {
			return nil, fmt.Errorf("child Pod identity changed while waiting for warm authorization")
		}
		c.Ticket = t
		if err = node.AtomicJSON(filepath.Join(a.Base, "requests", c.SandboxID+".json"), t); err != nil {
			return nil, err
		}
	}
	var live, parent node.Ticket
	if err := node.ReadJSON(filepath.Join(a.Base, "tickets", c.Ticket.PodUID+".json"), &live); err != nil {
		return nil, err
	}
	if err := node.ReadJSON(filepath.Join(a.Base, "requests", c.SandboxID+".json"), &parent); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(live, c.Ticket) || !reflect.DeepEqual(parent, c.Ticket) {
		return nil, fmt.Errorf("child sandbox revoked")
	}
	var restorePath string
	if c.DeferredRestore {
		if c.Ticket.Snapshot == "" {
			return nil, fmt.Errorf("deferred warm restore has no checkpoint")
		}
		var err error
		restorePath, err = node.SnapshotPath(a.Base, c.Ticket.OwnerUID, c.Ticket.Snapshot)
		if err != nil {
			return nil, err
		}
		manifest, err := node.VerifyPrepared(restorePath, c.Ticket.OwnerUID, c.Ticket.SpecHash, a.Runsc)
		if err != nil {
			return nil, fmt.Errorf("refusing restore: %w", err)
		}
		if err = verifyTicketManifest(c.Ticket, manifest); err != nil {
			return nil, err
		}
		if err = node.ConsumeSnapshot(a.Base, c.Ticket.OwnerUID, c.Ticket.Snapshot); err != nil {
			return nil, fmt.Errorf("refusing snapshot replay: %w", err)
		}
		if err = a.claimHome(c.Ticket); err != nil {
			return nil, err
		}
	}
	if len(ticketMounts(c.Ticket)) > 0 {
		if err := a.verifyVolumeIdentities(c.Ticket); err != nil {
			return nil, fmt.Errorf("persistent volume attachment: %w", err)
		}
		if err := verifyVolumeExecution(a.Base, c.Ticket); err != nil {
			return nil, fmt.Errorf("persistent volume execution: %w", err)
		}
		if err := a.claimHomeChild(c.SandboxID, id); err != nil {
			return nil, err
		}
	}
	if c.DeferredRestore {
		if err := a.bindHome(c.BundleDir, c.Ticket); err != nil {
			return nil, err
		}
		if err := node.AtomicJSON(filepath.Join(a.Base, "children", id+".json"), c); err != nil {
			return nil, err
		}
	}
	path := filepath.Join(a.Base, "children", id+".json.started")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err = f.Sync(); err != nil {
		return nil, err
	}
	if err = node.SyncDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	if c.DeferredRestore {
		return append(append([]string{}, args[:idx]...), "restore", "--background", a.backgroundLoadFlag(), "--detach", "--image-path="+restorePath, id), nil
	}
	return args, nil
}
