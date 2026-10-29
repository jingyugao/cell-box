// SPDX-License-Identifier: Apache-2.0

package adapter

import (
	"encoding/json"
	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/node"
	"os"
	"path/filepath"
	"reflect"
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
	if err := a.childCreate(id, annotations, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Rewrite([]string{"start", id}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Rewrite([]string{"start", id}); err == nil {
		t.Fatal("duplicate child start accepted")
	}
	second := strings.Repeat("e", 64)
	if err := a.childCreate(second, annotations, ""); err != nil {
		t.Fatal(err)
	}
	os.Remove(filepath.Join(base, "tickets", "pod.json"))
	if _, err := a.Rewrite([]string{"start", second}); err == nil {
		t.Fatal("revoked child accepted")
	}
}

func TestBackgroundLoadDefaultsToLazyAndRejectsUnknownModes(t *testing.T) {
	if got := (Adapter{}).backgroundLoadFlag(); got != "--background-load=lazy" {
		t.Fatalf("default flag = %q", got)
	}
	t.Setenv("CELLBOX_BACKGROUND_LOAD", "eager")
	a, err := adapterFromEnv()
	if err != nil || a.backgroundLoadFlag() != "--background-load=eager" {
		t.Fatalf("eager override: %q %v", a.backgroundLoadFlag(), err)
	}
	t.Setenv("CELLBOX_BACKGROUND_LOAD", "fast")
	if _, err = adapterFromEnv(); err == nil {
		t.Fatal("unknown mode accepted")
	}
}

// Timing marks are for measurement runs only: they must not appear unless asked
// for, and they must never turn a failed write into a failed sandbox start.
func TestTimingMarksAreOptInAndNeverFailTheStart(t *testing.T) {
	base := t.TempDir()
	Adapter{Base: base}.mark("sb", "pod", "child_start_enter")
	if _, err := os.Stat(filepath.Join(base, "timings.log")); !os.IsNotExist(err) {
		t.Fatalf("a mark was written while tracing is off: %v", err)
	}
	a := Adapter{Base: base, TraceTimings: true}
	a.mark("sb", "pod", "child_start_enter")
	a.mark("sb", "pod", "exec_restore")
	data, err := os.ReadFile(filepath.Join(base, "timings.log"))
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	var last int64
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var m struct {
			TS      int64  `json:"ts_ns"`
			Sandbox string `json:"sandbox"`
			PodUID  string `json:"pod_uid"`
			Event   string `json:"event"`
		}
		if err := json.Unmarshal([]byte(line), &m); err != nil || m.Sandbox != "sb" || m.PodUID != "pod" || m.TS < last {
			t.Fatalf("bad line %q: %v", line, err)
		}
		last = m.TS
		events = append(events, m.Event)
	}
	if !reflect.DeepEqual(events, []string{"child_start_enter", "exec_restore"}) {
		t.Fatalf("events: %v", events)
	}
	// An unwritable directory must be ignored, not reported.
	Adapter{Base: filepath.Join(base, "missing", "dir"), TraceTimings: true}.mark("sb", "pod", "x")
	t.Setenv("CELLBOX_TRACE_TIMINGS", "maybe")
	if _, err := adapterFromEnv(); err == nil {
		t.Fatal("invalid CELLBOX_TRACE_TIMINGS accepted")
	}
}
