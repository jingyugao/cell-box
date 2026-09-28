// SPDX-License-Identifier: Apache-2.0

// Package adapter translates an authorized sandbox Start to native runsc Restore.
// It deliberately preserves runsc's inherited file descriptors using syscall.Exec.
package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
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
		if spec.Annotations["io.kubernetes.cri.container-type"] != "sandbox" {
			return args, nil
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
		if t.Snapshot != "" {
			path, err := node.SnapshotPath(a.Base, t.OwnerUID, t.Snapshot)
			if err != nil {
				return nil, err
			}
			if _, err = node.Verify(path, t.OwnerUID, t.SpecHash, a.Runsc); err != nil {
				return nil, fmt.Errorf("refusing restore: %w", err)
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
			// Child containers use ordinary Start, but a missing root request
			// must never turn a restore into a cold sandbox start.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, a.Runsc, append(append([]string{}, args[:idx]...), "state", id)...)
			command.WaitDelay = time.Second
			data, stateErr := command.Output()
			if stateErr != nil {
				return nil, fmt.Errorf("cannot verify Start container identity: %w", stateErr)
			}
			var state struct {
				Annotations map[string]string `json:"annotations"`
			}
			if err = json.Unmarshal(data, &state); err != nil {
				return nil, err
			}
			if state.Annotations["io.kubernetes.cri.container-type"] != "container" {
				return nil, fmt.Errorf("sandbox authorization record missing; refusing cold start")
			}
			return args, nil
		}
		if err != nil {
			return nil, err
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
			return append(append([]string{}, args[:idx]...), "restore", "--detach", "--image-path="+path, id), nil
		}
	}
	return args, nil
}
func Main() {
	a := Adapter{Base: node.DefaultBase, Runsc: node.DefaultRunsc}
	args, err := a.Rewrite(os.Args[1:])
	if err == nil {
		err = syscall.Exec(a.Runsc, append([]string{a.Runsc}, args...), os.Environ())
	}
	// OCI JSON log is consumed by the shim. Stderr may already be a closed pipe.
	entry, _ := json.Marshal(map[string]string{"level": "error", "msg": err.Error(), "time": time.Now().UTC().Format(time.RFC3339Nano)})
	for _, path := range []string{filepath.Join(a.Base, "adapter.log"), argValue(os.Args[1:], "--log")} {
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
