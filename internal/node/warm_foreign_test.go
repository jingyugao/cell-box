package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// foreignFixture holds a warm lease and ticket whose sandbox CRI reports under
// the given runtime handler. The fake CRI has no stop or remove calls, so any
// attempt to terminate the sandbox panics.
func foreignFixture(t *testing.T, handler string, sandboxUID string) (*Backend, *core.Pod, string) {
	t.Helper()
	base := t.TempDir()
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod-uid", Name: "warm-pod", Namespace: "cellbox"}}
	ticket := Ticket{OwnerUID: "box-uid", PodUID: string(p.UID), Namespace: p.Namespace, PodName: p.Name, Snapshot: "s1", SpecHash: "h"}
	if err := AtomicJSON(filepath.Join(base, "tickets", string(p.UID)+".json"), ticket); err != nil {
		t.Fatal(err)
	}
	lease := WarmLease{UID: string(p.UID), Name: p.Name, Namespace: p.Namespace, SpecHash: "h", Phase: "waiting", Deadline: time.Now().Add(time.Minute)}
	if err := AtomicJSON(filepath.Join(base, "warm", string(p.UID)+".json"), lease); err != nil {
		t.Fatal(err)
	}
	runtime := &resumeGuestCRI{
		executionCRI: &executionCRI{},
		items: []*cri.PodSandbox{{Id: "sandbox-id", RuntimeHandler: handler,
			Metadata: &cri.PodSandboxMetadata{Name: p.Name, Namespace: p.Namespace, Uid: sandboxUID}}},
	}
	return &Backend{Base: base, Runtime: runtime, RuntimeHandler: "ours"}, p, base
}

func untouched(t *testing.T, base string, p *core.Pod) {
	t.Helper()
	var lease WarmLease
	if err := ReadJSON(filepath.Join(base, "warm", string(p.UID)+".json"), &lease); err != nil || lease.Phase != "waiting" {
		t.Fatalf("lease was changed: %+v %v", lease, err)
	}
	if _, err := os.Stat(filepath.Join(base, "tickets", string(p.UID)+".json")); err != nil {
		t.Fatalf("ticket was revoked: %v", err)
	}
}

// After a handler rename or on a shared node directory, leases of the old
// handler stay behind. Reaping must neither fail every cycle nor damage them.
func TestReapWarmLeavesALeaseOfAnotherRuntimeHandlerAlone(t *testing.T) {
	b, p, base := foreignFixture(t, "theirs", "pod-uid")
	for i := 0; i < 3; i++ {
		if err := b.ReapWarm(context.Background(), "cellbox", map[string]bool{}); err != nil {
			t.Fatalf("cycle %d reported a failure: %v", i, err)
		}
	}
	untouched(t, base, p)
}

func TestCleanupRefusesAnotherRuntimeHandlersSandboxBeforeChangingAnything(t *testing.T) {
	b, p, base := foreignFixture(t, "theirs", "pod-uid")
	err := b.Cleanup(context.Background(), p)
	if !errors.Is(err, ErrForeignRuntime) || !errors.Is(err, ErrExecutionLost) {
		t.Fatalf("Cleanup() = %v, want ErrForeignRuntime that is also ErrExecutionLost", err)
	}
	untouched(t, base, p)
}

// A sandbox of our own handler with the wrong identity is a real inconsistency
// and must still be reported rather than skipped.
func TestIdentityMismatchOfOurOwnSandboxIsStillAnError(t *testing.T) {
	b, p, _ := foreignFixture(t, "ours", "someone-else")
	_, err := b.sandboxes(context.Background(), p)
	if !errors.Is(err, ErrExecutionLost) || errors.Is(err, ErrForeignRuntime) {
		t.Fatalf("sandboxes() = %v, want ErrExecutionLost without ErrForeignRuntime", err)
	}
	if err := b.ReapWarm(context.Background(), "cellbox", map[string]bool{}); err == nil {
		t.Fatal("a mismatched sandbox of our own handler must not be skipped silently")
	}
}
