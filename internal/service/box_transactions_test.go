package service

import (
	"context"
	"testing"
	"time"
)

func TestOperationWaitWakesOnCommitAndHonorsOwnership(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.service.store.Update(func(st *State) error {
		st.Operations["op-wait"] = operationRecord{ClientID: "client-a", Operation: Operation{ID: "op-wait", Status: "running", Version: 1}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan Operation, 1)
	go func() { op, _ := f.service.waitOperation(ctx, "client-a", "op-wait", 10*time.Second); done <- op }()
	if _, err := f.service.waitOperation(ctx, "client-b", "op-wait", 10*time.Second); err == nil {
		t.Fatal("cross-client operation wait accepted")
	}
	if err := f.service.store.Update(func(st *State) error {
		op := st.Operations["op-wait"]
		op.Operation.Status = "succeeded"
		op.Operation.Version++
		st.Operations["op-wait"] = op
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case op := <-done:
		if op.Status != "succeeded" {
			t.Fatalf("completion notification lost: %+v", op)
		}
	case <-ctx.Done():
		t.Fatal("operation waiter did not wake")
	}
}
