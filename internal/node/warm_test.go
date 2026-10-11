package node

import (
	"cellbox.local/cellbox/internal/homevolume"
	backend "cellbox.local/cellbox/internal/runtime"
	"context"
	"errors"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWarmPersistentHomeAuthorization(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	bin := filepath.Join(base, "runsc")
	if err := os.WriteFile(bin, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	b := &Backend{Base: base, Runsc: bin, Images: imageClient{id: "image-id"}}
	homes := map[string]string{}
	for _, owner := range []string{"a", "b"} {
		h, err := homevolume.Prepare(base, owner, "node", "spec", true)
		if err != nil {
			t.Fatal(err)
		}
		homes[owner] = h.ID
		if err := homevolume.ClaimExecution(base, owner, h.ID, "old-"+owner, ""); err != nil {
			t.Fatal(err)
		}
		if err := homevolume.Checkpoint(base, owner, h.ID, "old-"+owner, "snapshot"); err != nil {
			t.Fatal(err)
		}
		path := testCheckpoint(t, base, owner, "snapshot", "spec", "image-id", bin)
		if err := os.Remove(filepath.Join(path, "manifest.json")); err != nil {
			t.Fatal(err)
		}
		hash, err := Digest(bin)
		if err != nil {
			t.Fatal(err)
		}
		if err := Seal(path, Manifest{OwnerUID: owner, SpecHash: "spec", ImageID: "image-id", RunscHash: hash, HomeID: h.ID}); err != nil {
			t.Fatal(err)
		}
	}
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod", Name: "pod", Namespace: "test"}}
	if err := b.RegisterWarm(ctx, p, "spec"); err != nil {
		t.Fatal(err)
	}
	if _, err := BindWarm(base, "pod", "pod", "test", strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := warmUpdate(base, "pod", false, func(l *WarmLease) error { l.Phase = "waiting"; l.Deadline = time.Now().Add(time.Minute); return nil }); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, owner := range []string{"a", "b"} {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			w := testWorkload(owner, "snapshot", "spec", "image")
			w.Spec.PersistentHome = true
			w.Spec.NodeName = "node"
			_, err := b.ActivateWarm(ctx, w, p)
			results <- err
		}(owner)
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else {
			t.Logf("claim failed: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful owners=%d", successes)
	}
	var ticket Ticket
	if err := ReadJSON(filepath.Join(base, "tickets", "pod.json"), &ticket); err != nil {
		t.Fatal(err)
	}
	if ticket.HomeID != homes[ticket.OwnerUID] {
		t.Fatal("warm ticket does not pin winning HOME")
	}
	if err := homevolume.ClaimExecution(base, ticket.OwnerUID, ticket.HomeID, "pod", "snapshot"); err != nil {
		t.Fatal(err)
	}
	w := testWorkload(ticket.OwnerUID, "snapshot", "spec", "image")
	w.Spec.PersistentHome = true
	w.Spec.NodeName = "node"
	if ok, err := b.ActivateWarm(ctx, w, p); !ok || err != nil {
		t.Fatalf("idempotent activation after parent claimed HOME: %v %v", ok, err)
	}
	peer := "a"
	if ticket.OwnerUID == peer {
		peer = "b"
	}
	if err := homevolume.ValidateCheckpoint(base, peer, homes[peer], "snapshot"); err != nil {
		t.Fatalf("losing claim consumed peer HOME: %v", err)
	}
}

func TestWarmClaimRaceAndExpiry(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	bin := filepath.Join(base, "runsc")
	if err := os.WriteFile(bin, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	b := &Backend{Base: base, Runsc: bin, Images: imageClient{id: "image-id"}}
	for _, owner := range []string{"a", "b"} {
		path := testCheckpoint(t, base, owner, "snapshot", "spec", "image-id", bin)
		if _, err := VerifyCached(path, owner, "spec", bin); err != nil {
			t.Fatal(err)
		}
	}
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "pod", Name: "pod", Namespace: "test"}}
	if err := b.RegisterWarm(ctx, p, "spec"); err != nil {
		t.Fatal(err)
	}
	sid := strings.Repeat("a", 64)
	if _, err := BindWarm(base, "pod", "pod", "test", sid); err != nil {
		t.Fatal(err)
	}
	if _, err := BindWarm(base, "pod", "pod", "test", strings.Repeat("b", 64)); err == nil {
		t.Fatal("Pod UID rearmed")
	}
	_, err := warmUpdate(base, "pod", false, func(l *WarmLease) error { l.Phase = "waiting"; l.Deadline = time.Now().Add(time.Minute); return nil })
	if err != nil {
		t.Fatal(err)
	}
	// These claims race through separate flock descriptors, as controller and
	// adapter processes do. Exactly one owner may commit the assignment.
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, owner := range []string{"a", "b"} {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			_, err := b.ActivateWarm(ctx, testWorkload(owner, "snapshot", "spec", "image"), p)
			results <- err
		}(owner)
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		}
	}
	if success != 1 {
		t.Fatalf("successful owners=%d", success)
	}
	l, err := b.WarmStatus(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	var ticket Ticket
	if err = ReadJSON(filepath.Join(base, "tickets", "pod.json"), &ticket); err != nil {
		t.Fatal(err)
	}
	if ticket != l.Ticket {
		t.Fatal("losing claim overwrote winner ticket")
	}
	if err = RetireWarm(base, "pod"); err != nil {
		t.Fatal(err)
	}
	if _, err = WaitWarm(base, "pod", sid); err == nil {
		t.Fatal("retired slot restarted")
	}
	_, err = warmUpdate(base, "pod", false, func(l *WarmLease) error { l.Phase = "waiting"; l.Deadline = time.Now().Add(-time.Second); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.ActivateWarm(ctx, testWorkload("a", "snapshot", "spec", "image"), p); !errors.Is(err, backend.ErrWarmExpired) {
		t.Fatalf("expired slot: %v", err)
	}
}
