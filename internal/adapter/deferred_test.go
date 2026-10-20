// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/node"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func noRootFixture(t *testing.T) (Adapter, node.Ticket, string, string) {
	t.Helper()
	base := t.TempDir()
	runsc := filepath.Join(base, "runsc")
	if err := os.WriteFile(runsc, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	a := Adapter{Base: base, Runsc: runsc}
	ticket := node.Ticket{OwnerUID: "owner", PodUID: "pod", PodName: "pod", Namespace: "test", SpecHash: "spec", Snapshot: "snapshot", NoRoot: true}
	path, _ := node.SnapshotPath(base, ticket.OwnerUID, ticket.Snapshot)
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "checkpoint.img"), []byte("state"), 0600); err != nil {
		t.Fatal(err)
	}
	hash, err := node.Digest(runsc)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Seal(path, node.Manifest{OwnerUID: ticket.OwnerUID, SpecHash: ticket.SpecHash, RunscHash: hash, NoRoot: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := node.VerifyCached(path, ticket.OwnerUID, ticket.SpecHash, runsc); err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(base, "sandbox-bundle")
	annotations := map[string]string{"io.kubernetes.cri.container-type": "sandbox", "io.kubernetes.cri.sandbox-uid": ticket.PodUID, "io.kubernetes.cri.sandbox-name": ticket.PodName, "io.kubernetes.cri.sandbox-namespace": ticket.Namespace, api.TicketAnnotation: ticket.PodUID}
	if err := node.AtomicJSON(filepath.Join(bundle, "config.json"), map[string]any{
		"process": map[string]any{"args": []string{"/pause"}}, "root": map[string]any{"path": "/pause-rootfs"},
		"mounts":      []any{map[string]string{"destination": "/proc", "type": "proc"}},
		"annotations": annotations, "linux": map[string]any{"namespaces": []any{map[string]string{"type": "network", "path": "/netns"}}},
		"extension": "preserved",
	}); err != nil {
		t.Fatal(err)
	}
	return a, ticket, bundle, path
}

func TestNoRootDeferredCheckpointIsBoundByChild(t *testing.T) {
	a, ticket, bundle, path := noRootFixture(t)
	sid, childID := strings.Repeat("a", 64), strings.Repeat("b", 64)
	if err := node.AtomicJSON(filepath.Join(a.Base, "tickets", ticket.PodUID+".json"), ticket); err != nil {
		t.Fatal(err)
	}
	args, err := a.Rewrite([]string{"create", "--bundle", bundle, sid})
	if err != nil || !reflect.DeepEqual(args, []string{"create", "--no-root-container", "--bundle", bundle, sid}) {
		t.Fatalf("no-root create: %v, %v", args, err)
	}
	var spec map[string]any
	if err := node.ReadJSON(filepath.Join(bundle, "config.json"), &spec); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"process", "root", "mounts"} {
		if _, exists := spec[key]; exists {
			t.Fatalf("no-root sandbox retained %s", key)
		}
	}
	if spec["linux"] == nil || spec["annotations"] == nil || spec["extension"] != "preserved" {
		t.Fatalf("sandbox policy or extensions lost: %v", spec)
	}
	args, err = a.Rewrite([]string{"--root=/run/test", "start", sid})
	if err != nil || !reflect.DeepEqual(args, []string{"--root=/run/test", "--restore-page-loading=lazy", "restore", "--background", "--detach", sid}) {
		t.Fatalf("root must prepare without a checkpoint: %v, %v", args, err)
	}
	if err := node.ConsumeSnapshot(a.Base, ticket.OwnerUID, ticket.Snapshot); err == nil {
		t.Fatal("deferred assignment did not fence snapshot replay")
	}
	annotations := map[string]string{"io.kubernetes.cri.sandbox-id": sid, "io.kubernetes.cri.sandbox-uid": ticket.PodUID, "io.kubernetes.cri.sandbox-name": ticket.PodName, "io.kubernetes.cri.sandbox-namespace": ticket.Namespace}
	if err := a.childCreate(childID, annotations, ""); err != nil {
		t.Fatal(err)
	}
	args, err = a.Rewrite([]string{"--root=/run/test", "start", childID})
	want := []string{"--root=/run/test", "--restore-page-loading=lazy", "restore", "--background", "--detach", "--image-path=" + path, childID}
	if err != nil || !reflect.DeepEqual(args, want) {
		t.Fatalf("child must supply checkpoint: %v, %v", args, err)
	}
	if _, err := a.Rewrite([]string{"start", childID}); err == nil {
		t.Fatal("child restore replay was accepted")
	}
}

func TestNoRootModeMismatchIsRejectedBeforeClaim(t *testing.T) {
	a, ticket, bundle, _ := noRootFixture(t)
	ticket.NoRoot = false
	if err := node.AtomicJSON(filepath.Join(a.Base, "tickets", ticket.PodUID+".json"), ticket); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Rewrite([]string{"create", "--bundle", bundle, strings.Repeat("a", 64)}); err == nil || !strings.Contains(err.Error(), "sandbox mode mismatch") {
		t.Fatalf("mismatched checkpoint accepted: %v", err)
	}
	if _, err := os.Stat(filepath.Join(a.Base, "claims", ticket.PodUID)); !os.IsNotExist(err) {
		t.Fatalf("mode mismatch claimed execution: %v", err)
	}
}

func TestNoRootWarmPreparationPrecedesAssignment(t *testing.T) {
	a, ticket, bundle, _ := noRootFixture(t)
	sid := strings.Repeat("a", 64)
	lease := node.WarmLease{UID: ticket.PodUID, Name: ticket.PodName, Namespace: ticket.Namespace, Phase: "registered", NoRoot: true}
	leasePath := filepath.Join(a.Base, "warm", ticket.PodUID+".json")
	if err := node.AtomicJSON(leasePath, lease); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Rewrite([]string{"create", "--bundle", bundle, sid}); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		args []string
		err  error
	}
	done := make(chan outcome, 1)
	go func() { args, err := a.Rewrite([]string{"start", sid}); done <- outcome{args, err} }()
	b := &node.Backend{Base: a.Base}
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod", Name: ticket.PodName, Namespace: ticket.Namespace}}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var err error
		lease, err = b.WarmStatus(context.Background(), pod)
		if err != nil {
			t.Fatal(err)
		}
		if lease.Phase == "waiting" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("warm preparation never became claimable")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(a.Base, "requests", sid+".json.prepared")); err != nil {
		t.Fatal(err)
	}
	if err := node.AtomicJSON(filepath.Join(a.Base, "tickets", ticket.PodUID+".json"), ticket); err != nil {
		t.Fatal(err)
	}
	lease.Phase, lease.Ticket = "claimed", ticket
	if err := node.AtomicJSON(leasePath, lease); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil || len(result.args) != 0 {
			t.Fatalf("prepared root Start must finish without a second restore: %v %v", result.args, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("prepared root did not finish after assignment")
	}
	if _, err := a.Rewrite([]string{"start", sid}); err == nil {
		t.Fatal("warm root started twice")
	}
}

func TestNoRootFailedPreparationNeverBecomesClaimable(t *testing.T) {
	a, ticket, bundle, _ := noRootFixture(t)
	if err := os.WriteFile(a.Runsc, []byte("#!/bin/sh\nexit 42\n"), 0700); err != nil {
		t.Fatal(err)
	}
	sid := strings.Repeat("a", 64)
	lease := node.WarmLease{UID: ticket.PodUID, Name: ticket.PodName, Namespace: ticket.Namespace, Phase: "registered", NoRoot: true}
	leasePath := filepath.Join(a.Base, "warm", ticket.PodUID+".json")
	if err := node.AtomicJSON(leasePath, lease); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Rewrite([]string{"create", "--bundle", bundle, sid}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Rewrite([]string{"start", sid}); err == nil || !strings.Contains(err.Error(), "prepare no-root sandbox") {
		t.Fatalf("native preparation failure was lost: %v", err)
	}
	if err := node.ReadJSON(leasePath, &lease); err != nil {
		t.Fatal(err)
	}
	if lease.Phase != "created" || !lease.Deadline.IsZero() {
		t.Fatalf("failed sandbox became claimable: %+v", lease)
	}
	if _, err := a.Rewrite([]string{"start", sid}); err == nil || !strings.Contains(err.Error(), "already attempted") {
		t.Fatalf("failed native preparation was retried: %v", err)
	}
}
