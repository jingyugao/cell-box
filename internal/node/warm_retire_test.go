package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTryRetireWarmProtectsClaimedAndRestoringLeases(t *testing.T) {
	for _, phase := range []string{"claimed", "restoring"} {
		t.Run(phase, func(t *testing.T) {
			base := t.TempDir()
			b := &Backend{Base: base}
			p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod-uid", Name: "warm-pod", Namespace: "cellbox"}}
			ticket := Ticket{OwnerUID: "box-uid", PodUID: string(p.UID), Namespace: p.Namespace, PodName: p.Name, Snapshot: "snapshot-1", SpecHash: "spec-hash"}
			if err := AtomicJSON(filepath.Join(base, "tickets", string(p.UID)+".json"), ticket); err != nil {
				t.Fatal(err)
			}
			lease := WarmLease{UID: string(p.UID), Name: p.Name, Namespace: p.Namespace, SpecHash: "spec-hash", Phase: phase, Deadline: time.Now().Add(time.Minute), Ticket: ticket}
			if err := AtomicJSON(filepath.Join(base, "warm", string(p.UID)+".json"), lease); err != nil {
				t.Fatal(err)
			}

			retired, err := b.TryRetireWarm(context.Background(), p)
			if err != nil || retired {
				t.Fatalf("TryRetireWarm() = %v, %v; want false, nil", retired, err)
			}
			var gotLease WarmLease
			if err := ReadJSON(filepath.Join(base, "warm", string(p.UID)+".json"), &gotLease); err != nil {
				t.Fatal(err)
			}
			if gotLease.UID != lease.UID || gotLease.Name != lease.Name || gotLease.Namespace != lease.Namespace ||
				gotLease.SpecHash != lease.SpecHash || gotLease.SID != lease.SID || gotLease.Phase != lease.Phase ||
				!gotLease.Deadline.Equal(lease.Deadline) || gotLease.Ticket != lease.Ticket {
				t.Fatalf("lease changed on protected phase: got %+v, want %+v", gotLease, lease)
			}
			var gotTicket Ticket
			if err := ReadJSON(filepath.Join(base, "tickets", string(p.UID)+".json"), &gotTicket); err != nil {
				t.Fatal(err)
			}
			if gotTicket != ticket {
				t.Fatalf("ticket changed: got %+v, want %+v", gotTicket, ticket)
			}
		})
	}
}

func TestTryRetireWarmRetiresWaitingLeaseAndAllowsMissingLease(t *testing.T) {
	base := t.TempDir()
	b := &Backend{Base: base}
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod-uid", Name: "warm-pod", Namespace: "cellbox"}}
	lease := WarmLease{UID: string(p.UID), Name: p.Name, Namespace: p.Namespace, SpecHash: "spec-hash", Phase: "waiting", Deadline: time.Now().Add(time.Minute)}
	if err := AtomicJSON(filepath.Join(base, "warm", string(p.UID)+".json"), lease); err != nil {
		t.Fatal(err)
	}

	retired, err := b.TryRetireWarm(context.Background(), p)
	if err != nil || !retired {
		t.Fatalf("TryRetireWarm() = %v, %v; want true, nil", retired, err)
	}
	var got WarmLease
	if err := ReadJSON(filepath.Join(base, "warm", string(p.UID)+".json"), &got); err != nil {
		t.Fatal(err)
	}
	if got.Phase != "retired" {
		t.Fatalf("lease phase = %q; want retired", got.Phase)
	}
	if _, err := WaitWarm(base, string(p.UID), "sandbox-id"); err == nil {
		t.Fatal("WaitWarm accepted a retired slot")
	}

	missing := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "not-registered", Name: "gated-pod", Namespace: p.Namespace}}
	retired, err = b.TryRetireWarm(context.Background(), missing)
	if err != nil || !retired {
		t.Fatalf("TryRetireWarm() for missing lease = %v, %v; want true, nil", retired, err)
	}
	invalid := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "bad/uid", Name: p.Name, Namespace: p.Namespace}}
	if _, err := b.TryRetireWarm(context.Background(), invalid); err == nil {
		t.Fatal("TryRetireWarm accepted an invalid Pod UID")
	}
	if _, err := os.Stat(filepath.Join(base, "warm", "not-registered.json")); !os.IsNotExist(err) {
		t.Fatalf("missing lease unexpectedly created: stat err=%v", err)
	}
}
