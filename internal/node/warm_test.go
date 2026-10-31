package node

import (
	"context"
	"errors"
	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/homevolume"
	backend "github.com/jingyugao/cell-box/internal/runtime"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"path/filepath"
	"reflect"
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
	volumes := map[string]string{}
	for _, owner := range []string{"a", "b"} {
		volumeUID := "volume-" + owner
		volumes[owner] = volumeUID
		h, err := homevolume.Prepare(base, volumeUID, "node", homevolume.VolumeSpecHash, true)
		if err != nil {
			t.Fatal(err)
		}
		homes[owner] = h.ID
		if err := homevolume.SyncVolume(base, volumeUID, "node", h.ID); err != nil {
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
	profile := testWorkload("profile", "snapshot", "spec", "image-id")
	profile.Spec.PersistentHome = true
	profile.Spec.NodeName = "node"
	if err := b.RegisterWarm(ctx, p, api.SpecFingerprint(profile)); err != nil {
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
			w := testWorkload(owner, "snapshot", "spec", "image-id")
			w.Spec.PersistentHome = true
			w.Spec.NodeName = "node"
			w.Spec.HomeVolumeUID = volumes[owner]
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
	if ticket.HomeID != homes[ticket.OwnerUID] || ticket.HomeVolumeUID != volumes[ticket.OwnerUID] {
		t.Fatal("warm ticket does not pin winning HOME")
	}
	w := testWorkload(ticket.OwnerUID, "snapshot", "spec", "image")
	w.Spec.PersistentHome = true
	w.Spec.NodeName = "node"
	w.Spec.HomeVolumeUID = volumes[ticket.OwnerUID]
	if ok, err := b.ActivateWarm(ctx, w, p); !ok || err != nil {
		t.Fatalf("idempotent activation after parent claimed HOME: %v %v", ok, err)
	}
	peer := "a"
	if ticket.OwnerUID == peer {
		peer = "b"
	}
	if _, err := homevolume.Verify(base, volumes[peer], "node", homevolume.VolumeSpecHash, homes[peer]); err != nil {
		t.Fatalf("losing claim damaged peer HOME identity: %v", err)
	}
}

func TestWarmRejectsDifferentProfileBeforeAttachingVolume(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	bin := filepath.Join(base, "runsc")
	if err := os.WriteFile(bin, []byte("runsc"), 0700); err != nil {
		t.Fatal(err)
	}
	b := &Backend{Base: base, Runsc: bin}
	profile := testWorkload("profile", "snapshot", "spec", "image-good")
	profile.Spec.PersistentHome = true
	profile.Spec.NodeName = "node"
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: "warm-pod", Name: "warm-pod", Namespace: "test"}}
	if err := b.RegisterWarm(ctx, p, api.SpecFingerprint(profile)); err != nil {
		t.Fatal(err)
	}
	if _, err := BindWarm(base, "warm-pod", p.Name, p.Namespace, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := warmUpdate(base, "warm-pod", false, func(l *WarmLease) error {
		l.Phase = "waiting"
		l.Deadline = time.Now().Add(time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	w := testWorkload("new-box", "snapshot", "spec", "image-wrong")
	w.Spec.PersistentHome = true
	w.Spec.NodeName = "node"
	w.Spec.HomeVolumeUID = "volume-not-attached"
	if _, err := b.ActivateWarm(ctx, w, p); err == nil || !strings.Contains(err.Error(), "incompatible warm restore") {
		t.Fatalf("wrong image profile was accepted: %v", err)
	}
	if _, err := homevolume.Path(base, w.Spec.HomeVolumeUID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "homes", w.Spec.HomeVolumeUID)); !os.IsNotExist(err) {
		t.Fatalf("profile mismatch touched/created the requested volume: %v", err)
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
	profile := testWorkload("profile", "snapshot", "spec", "image")
	if err := b.RegisterWarm(ctx, p, api.SpecFingerprint(profile)); err != nil {
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
	if !reflect.DeepEqual(ticket, l.Ticket) {
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

// TestWaitWarmReturnsClaimAmongOtherSlotEvents checks that filtering events of
// other slots does not hide the claim of this slot.
func TestWaitWarmReturnsClaimAmongOtherSlotEvents(t *testing.T) {
	base := t.TempDir()
	if _, err := warmUpdate(base, "pod", true, func(l *WarmLease) error {
		*l = WarmLease{UID: "pod", SID: "sid", Phase: "created"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	type result struct {
		ticket Ticket
		err    error
	}
	done := make(chan result, 1)
	go func() {
		ticket, err := WaitWarm(base, "pod", "sid")
		done <- result{ticket, err}
	}()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		var l WarmLease
		if err := ReadJSON(filepath.Join(base, "warm", "pod.json"), &l); err != nil {
			t.Fatal(err)
		}
		if l.Phase == "waiting" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("slot never started waiting")
		}
	}
	for i := 0; i < 50; i++ {
		if err := EphemeralJSON(filepath.Join(base, "warm", "other.json"), WarmLease{UID: "other", Name: strings.Repeat("x", i)}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case r := <-done:
		t.Fatalf("returned before the claim: %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
	ticket := Ticket{OwnerUID: "owner", PodUID: "pod", Snapshot: "snapshot"}
	if err := EphemeralJSON(filepath.Join(base, "tickets", "pod.json"), ticket); err != nil {
		t.Fatal(err)
	}
	if _, err := warmUpdate(base, "pod", false, func(l *WarmLease) error {
		l.Phase = "claimed"
		l.Ticket = ticket
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatal(r.err)
		}
		if !reflect.DeepEqual(r.ticket, ticket) {
			t.Fatalf("ticket = %+v, want %+v", r.ticket, ticket)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("claim was not observed")
	}
	var l WarmLease
	if err := ReadJSON(filepath.Join(base, "warm", "pod.json"), &l); err != nil {
		t.Fatal(err)
	}
	if l.Phase != "restoring" {
		t.Fatalf("phase = %q, want restoring", l.Phase)
	}
}
