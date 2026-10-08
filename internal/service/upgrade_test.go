package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"context"
	"errors"
	"testing"
)

type lostUpgradeResponse struct {
	*fakeCoreProvider
	nonce string
	calls int
}

func (p *lostUpgradeResponse) Upgrade(_ context.Context, h boxprovider.Handle, spec boxprovider.Spec, nonce string) (boxprovider.Handle, error) {
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
	b.Staged = true
	b.RestoreComplete = true
	b.Upgrade = &upgradeIntent{Nonce: "same-request", PreviousImportedImageID: "old-import"}
	if err := f.service.store.Update(func(st *State) error { st.Boxes[box.ID] = b; return nil }); err != nil {
		t.Fatal(err)
	}
	p := &lostUpgradeResponse{fakeCoreProvider: f.provider}
	f.service.providers["docker"] = p
	if _, err := f.service.commitDiskUpgrade(context.Background(), b); err == nil {
		t.Fatal("expected lost response")
	}
	f.reopen(t)
	f.service.providers["docker"] = p
	b, err = f.service.rawBox(box.ID)
	if err != nil || b.Upgrade == nil {
		t.Fatal("lost durable intent", err)
	}
	observed, err := f.service.observe(context.Background(), b)
	if err != nil || observed.ID != box.ID || observed.Phase != "staged" {
		t.Fatal(observed, err)
	}
	after, err := f.service.rawBox(box.ID)
	if err != nil || after.Upgrade != nil || after.Handle.ImageID != "new-image" || after.ExecutionID != "replacement" || p.calls != 2 {
		t.Fatal(after, p.calls, err)
	}
	// A delayed observer holding the old intent must not send it again.
	if _, err := f.service.commitDiskUpgrade(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if p.calls != 2 {
		t.Fatal("stale observer replayed a completed intent")
	}
	creates, destroys := f.provider.counts()
	if creates != 1 || destroys != 0 {
		t.Fatal("upgrade replaced owned disk", creates, destroys)
	}
}
