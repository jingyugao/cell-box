// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/node"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuthorizationAndSingleExecution(t *testing.T) {
	base := t.TempDir()
	a := Adapter{Base: base, Runsc: "/unused"}
	bundle := filepath.Join(base, "bundle")
	uid := "pod-uid"
	id := strings.Repeat("a", 64)
	annotations := map[string]string{"io.kubernetes.cri.container-type": "sandbox", "io.kubernetes.cri.sandbox-uid": uid, "io.kubernetes.cri.sandbox-name": "pod", "io.kubernetes.cri.sandbox-namespace": "test", api.TicketAnnotation: uid}
	node.AtomicJSON(filepath.Join(bundle, "config.json"), map[string]any{"annotations": annotations})
	args := []string{"--root=/run/test", "create", "--bundle", bundle, id}
	if _, err := a.Rewrite(args); err == nil {
		t.Fatal("unauthorized create accepted")
	}
	ticket := node.Ticket{OwnerUID: "owner", PodUID: uid, PodName: "pod", Namespace: "test"}
	node.AtomicJSON(filepath.Join(base, "tickets", uid+".json"), ticket)
	if _, err := a.Rewrite(args); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Rewrite([]string{"start", id}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Rewrite([]string{"start", id}); err == nil {
		t.Fatal("duplicate start accepted")
	}
	args[len(args)-1] = strings.Repeat("b", 64)
	if _, err := a.Rewrite(args); err == nil {
		t.Fatal("second sandbox accepted")
	}
	os.Remove(filepath.Join(base, "claims", uid))
	annotations["io.kubernetes.cri.sandbox-uid"] = "victim"
	node.AtomicJSON(filepath.Join(bundle, "config.json"), map[string]any{"annotations": annotations})
	if _, err := a.Rewrite(args); err == nil {
		t.Fatal("stolen ticket accepted")
	}
}

func TestChildIdentityDoesNotUseStateOrAllowColdRootStart(t *testing.T) {
	base := t.TempDir()
	a := Adapter{Base: base, Runsc: "/must-not-execute-runsc-state"}
	sid, id := strings.Repeat("c", 64), strings.Repeat("d", 64)
	if _, err := a.Rewrite([]string{"start", sid}); err == nil {
		t.Fatal("missing root accepted")
	}
	ticket := node.Ticket{OwnerUID: "owner", PodUID: "pod", PodName: "name", Namespace: "test"}
	node.AtomicJSON(filepath.Join(base, "tickets", "pod.json"), ticket)
	node.AtomicJSON(filepath.Join(base, "requests", sid+".json"), ticket)
	annotations := map[string]string{"io.kubernetes.cri.sandbox-id": sid, "io.kubernetes.cri.sandbox-uid": "pod", "io.kubernetes.cri.sandbox-name": "name", "io.kubernetes.cri.sandbox-namespace": "test"}
	if err := a.childCreate(id, annotations); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Rewrite([]string{"start", id}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Rewrite([]string{"start", id}); err == nil {
		t.Fatal("duplicate child start accepted")
	}
	second := strings.Repeat("e", 64)
	if err := a.childCreate(second, annotations); err != nil {
		t.Fatal(err)
	}
	os.Remove(filepath.Join(base, "tickets", "pod.json"))
	if _, err := a.Rewrite([]string{"start", second}); err == nil {
		t.Fatal("revoked child accepted")
	}
}
