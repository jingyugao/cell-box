// SPDX-License-Identifier: Apache-2.0

// Package adapter translates an authorized sandbox Start to native runsc Restore.
// It deliberately preserves runsc's inherited file descriptors using syscall.Exec.
package adapter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
			return args, a.childCreate(id, spec.Annotations)
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
		if t.HomeID != "" {
			if _, err := homevolume.Verify(a.Base, t.OwnerUID, "", t.SpecHash, t.HomeID); err != nil {
				return nil, fmt.Errorf("persistent HOME unavailable: %w", err)
			}
			if t.Snapshot != "" {
				if err := homevolume.ValidateCheckpoint(a.Base, t.OwnerUID, t.HomeID, t.Snapshot); err != nil {
					return nil, err
				}
			}
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
			if m.HomeID != t.HomeID {
				return nil, fmt.Errorf("checkpoint persistent HOME identity mismatch")
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
		if live != t {
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
			if m.HomeID != t.HomeID {
				return nil, fmt.Errorf("checkpoint persistent HOME identity mismatch")
			}
			if err = a.claimHome(t); err != nil {
				return nil, err
			}
			return append(append([]string{}, args[:idx]...), "restore", "--detach", "--image-path="+path, id), nil
		}
		if err = a.claimHome(t); err != nil {
			return nil, err
		}
	}
	return args, nil
}

func (a Adapter) claimHome(t node.Ticket) error {
	if t.HomeID == "" {
		return nil
	}
	// Consume the disk/checkpoint pairing before restored code can mutate HOME.
	// A crash after this fence requires inspection, never replay with newer files.
	return homevolume.ClaimExecution(a.Base, t.OwnerUID, t.HomeID, t.PodUID, t.Snapshot)
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

func (a Adapter) childCreate(id string, annotations map[string]string) error {
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
	if live != t {
		return fmt.Errorf("child sandbox revoked")
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
	if live != c.Ticket || parent != c.Ticket {
		return fmt.Errorf("child sandbox revoked")
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
