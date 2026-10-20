// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"cellbox.local/cellbox/internal/node"
)

// sandboxCreate removes the pause container's filesystem and process. The
// network namespace, resource policy, hooks and annotations still belong to the
// sandbox; the business child's OCI spec remains unchanged.
func (a Adapter) sandboxCreate(args []string, command int, bundle string, noRoot bool) ([]string, error) {
	if !noRoot {
		return args, nil
	}
	path := filepath.Join(bundle, "config.json")
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("sandbox OCI spec must be a regular file")
	}
	var spec map[string]json.RawMessage
	if err := node.ReadJSON(path, &spec); err != nil {
		return nil, err
	}
	if spec == nil {
		return nil, fmt.Errorf("missing sandbox OCI spec")
	}
	delete(spec, "process")
	delete(spec, "root")
	delete(spec, "mounts")
	if err := node.AtomicJSON(path, spec); err != nil {
		return nil, err
	}
	out := append([]string{}, args[:command+1]...)
	out = append(out, "--no-root-container")
	return append(out, args[command+1:]...), nil
}

// prepareWarmNoRoot completes network/restore preparation before the warm slot
// becomes claimable. It supplies no checkpoint and starts no guest process.
// This root-only command needs no inherited application FDs. The eventual child
// restore continues to use syscall.Exec through Main.
func (a Adapter) prepareWarmNoRoot(prefix []string, id, request string) error {
	f, err := os.OpenFile(request+".prepared", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("no-root preparation already attempted: %w", err)
	}
	err = f.Sync()
	f.Close()
	if err != nil {
		return err
	}
	if err := node.SyncDir(filepath.Dir(request)); err != nil {
		return err
	}
	args := append(append([]string{}, prefix...), "restore", "--background", "--detach", id)
	args, err = a.DiagnosticArgs(args)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, a.Runsc, args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("prepare no-root sandbox: %w", err)
	}
	return nil
}

func (a Adapter) rewriteChildStart(args []string, command int, id string) ([]string, error) {
	var child node.ChildRecord
	if err := node.ReadJSON(filepath.Join(a.Base, "children", id+".json"), &child); err != nil {
		return nil, fmt.Errorf("missing container identity: %w", err)
	}
	ticket := child.Ticket
	var path, pageLoading string
	if ticket.NoRoot && ticket.Snapshot != "" {
		var err error
		path, err = node.SnapshotPath(a.Base, ticket.OwnerUID, ticket.Snapshot)
		if err != nil {
			return nil, err
		}
		manifest, err := node.VerifyPrepared(path, ticket.OwnerUID, ticket.SpecHash, a.Runsc)
		if err != nil {
			return nil, err
		}
		if err := verifyTicketManifest(ticket, manifest); err != nil {
			return nil, err
		}
		policy, err := node.LoadRestorePolicy(a.Base)
		if err != nil {
			return nil, err
		}
		pageLoading = policy.PageLoading
	}
	// Validate the still-live assignment, HOME claim and one-attempt fence before
	// passing the child's inherited stdio and Gofer FDs to native runsc.
	if err := a.childStart(id); err != nil {
		return nil, err
	}
	if path == "" {
		return args, nil
	}
	prefix := append([]string{}, args[:command]...)
	if pageLoading != "" {
		prefix = append(prefix, "--restore-page-loading="+pageLoading)
	}
	return append(prefix, "restore", "--background", "--detach", "--image-path="+path, id), nil
}
