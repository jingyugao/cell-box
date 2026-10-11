package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type lostUpgradeResponse struct {
	*fakeCoreProvider
	mu    sync.Mutex
	nonce string
	calls int
}

func (p *lostUpgradeResponse) Upgrade(_ context.Context, h boxprovider.Handle, spec boxprovider.Spec, nonce string) (boxprovider.Handle, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.nonce != "" && p.nonce != nonce {
		return h, errors.New("different upgrade nonce")
	}
	if p.nonce == "" {
		p.nonce = nonce
		p.setExecutionID("replacement")
		return h, errors.New("response lost after CR patch")
	}
	h.ImageID = spec.Image
	return h, nil
}
func (p *lostUpgradeResponse) callCount() int { p.mu.Lock(); defer p.mu.Unlock(); return p.calls }

func TestRestartAfterUpgradeRuntimeCommitPublishesOriginalOperation(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "upgrade-runtime-committed")
	b, err := f.service.rawBox(box.ID)
	if err != nil {
		t.Fatal(err)
	}
	b.Box.OperationID, b.Box.Phase = "same-request", "upgrading"
	b.Staged, b.RestoreComplete = true, true
	b.Upgrade = &upgradeIntent{Nonce: "same-request", RuntimeCommitted: true}
	op := Operation{ID: "same-request", Kind: "upgrade", TargetID: box.ID, Status: "running", Version: 1, CreatedAt: time.Now().UTC()}
	if err := f.service.store.Update(func(st *State) error {
		st.Boxes[box.ID] = b
		st.Operations[op.ID] = operationRecord{Operation: op, ClientID: b.ClientID}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// This provider deliberately has no Upgrade method: after its acknowledged
	// runtime commit, restart must only observe readiness and publish completion.
	f.reopen(t)
	waitForUpgradeState(t, f.service, box.ID, func(box boxRecord, operation Operation) bool {
		return operation.Status == "succeeded" && operation.Box != nil && operation.Box.Phase == "staged" &&
			operation.Box.OperationID == "" && box.Upgrade == nil && box.Box.OperationID == ""
	})
	creates, destroys := f.provider.counts()
	if creates != 1 || destroys != 0 {
		t.Fatalf("upgrade recovery recreated runtime: creates=%d destroys=%d", creates, destroys)
	}
}

func TestDiskUpgradeIntentSurvivesLostResponseAndServiceRestart(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "upgrade-journal")
	b, err := f.service.rawBox(box.ID)
	if err != nil {
		t.Fatal(err)
	}
	b.Profile.Image = "new-image"
	b.Box.Image = "new-image"
	b.Box.ImageID = "new-image"
	b.Box.Phase = "upgrading"
	b.Box.OperationID = "same-request"
	b.Staged = true
	b.RestoreComplete = true
	b.Upgrade = &upgradeIntent{Nonce: "same-request", PreviousImportedImageID: "old-import"}
	op := Operation{ID: "same-request", Kind: "upgrade", TargetID: box.ID, Status: "running", Version: 1, CreatedAt: time.Now().UTC()}
	if err := f.service.store.Update(func(st *State) error {
		st.Boxes[box.ID] = b
		st.Operations[op.ID] = operationRecord{Operation: op, ClientID: b.ClientID}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	p := &lostUpgradeResponse{fakeCoreProvider: f.provider}
	f.service.providers["docker"] = p
	f.service.launchWithCommit(op, func(ctx context.Context) (map[string]string, error) {
		_, err := f.service.commitDiskUpgrade(ctx, b)
		return nil, err
	}, nil)
	waitForUpgradeState(t, f.service, box.ID, func(box boxRecord, operation Operation) bool {
		return operation.Status == "failed" && box.Box.OperationID == op.ID && box.Upgrade != nil
	})
	f.reopen(t)
	f.service.providers["docker"] = p
	waitForUpgradeState(t, f.service, box.ID, func(box boxRecord, operation Operation) bool {
		return box.Upgrade == nil && box.Box.OperationID == "" && box.Box.Phase == "staged"
	})
	after, err := f.service.rawBox(box.ID)
	operation, opErr := f.service.store.Operation(b.ClientID, op.ID)
	if err != nil || opErr != nil || operation.Status != "failed" || after.Upgrade != nil || after.Handle.ImageID != "new-image" || after.ExecutionID != "replacement" || p.callCount() != 2 {
		t.Fatal(after, p.callCount(), err)
	}
	// A delayed observer holding the old intent must not send it again.
	if _, err := f.service.commitDiskUpgrade(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if p.callCount() != 2 {
		t.Fatal("stale observer replayed a completed intent")
	}
	creates, destroys := f.provider.counts()
	if creates != 1 || destroys != 0 {
		t.Fatal("upgrade replaced owned disk", creates, destroys)
	}
}

func waitForUpgradeState(t *testing.T, s *Service, id string, ready func(boxRecord, Operation) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b, boxErr := s.rawBox(id)
		if boxErr == nil {
			op, opErr := s.store.Operation(b.ClientID, "same-request")
			if opErr == nil && ready(b, op) {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	b, _ := s.rawBox(id)
	op, _ := s.store.Operation(b.ClientID, "same-request")
	t.Fatalf("upgrade did not reach expected state: box=%+v operation=%+v", b, op)
}
