package service

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jingyugao/cell-box/internal/guestapi"
)

func purgeService(store *Store) *Service {
	return &Service{ctx: context.Background(), store: store, streams: map[string]activeStream{}}
}

func seedPurge(t *testing.T, store *Store) {
	t.Helper()
	err := store.Write(context.Background(), func(tx *storeTx) error {
		box := coreBox("box")
		box.Box.Phase = "deleted"
		box.Box.OperationID = ""
		if err := tx.PutBox(box); err != nil {
			return err
		}
		if err := tx.PutOperation(operationRecord{ClientID: "owner", Operation: Operation{ID: "create", TargetID: "box", Kind: "create", Status: "succeeded"}}); err != nil {
			return err
		}
		if err := tx.PutOperation(operationRecord{ClientID: "owner", Operation: Operation{ID: "archive-op", TargetID: "box", Kind: "archive", Status: "succeeded"}}); err != nil {
			return err
		}
		if err := tx.PutExecution(executionRecord{Execution: Execution{ID: "exec", BoxID: "box", OperationID: "archive-op", State: "exited", Result: &guestapi.ExecResult{Stdout: "private-output"}}}); err != nil {
			return err
		}
		return tx.PutArchive(archiveRecord{ClientID: "owner", Archive: Archive{ID: "archive", SourceBoxID: "box"}})
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPurgeRejectsOtherTenantsLiveBoxesAndActiveOperations(t *testing.T) {
	store := newTestStore(t)
	seedPurge(t, store)
	s := purgeService(store)
	cases := []struct {
		name   string
		client string
		phase  string
		op     string
		want   string
	}{
		{name: "other tenant", client: "other", phase: "deleted", want: "NOT_FOUND"},
		{name: "not deleted", client: "owner", phase: "running", want: "CONFLICT"},
		{name: "operation running", client: "owner", phase: "deleted", op: "running", want: "BUSY"},
		{name: "exec running", client: "owner", phase: "deleted", op: "exec", want: "BUSY"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := store.Write(context.Background(), func(tx *storeTx) error {
				box, err := tx.Box("owner", "box")
				if err != nil {
					return err
				}
				box.Box.Phase = tc.phase
				box.Box.OperationID = ""
				if tc.op == "running" {
					box.Box.OperationID = "create"
					if err := tx.PutOperation(operationRecord{ClientID: "owner", Operation: Operation{ID: "create", TargetID: "box", Kind: "create", Status: "running"}}); err != nil {
						return err
					}
				}
				if err := tx.PutBox(box); err != nil {
					return err
				}
				if tc.op == "exec" {
					return tx.PutOperation(operationRecord{ClientID: "owner", Operation: Operation{ID: "active-exec", TargetID: "box", Kind: "exec", Status: "running"}})
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			err := s.purgeBox(context.Background(), tc.client, "box")
			var apiErr *APIError
			if !errors.As(err, &apiErr) || apiErr.Code != tc.want {
				t.Fatalf("purge error = %v, want %s", err, tc.want)
			}
			if _, err := store.BoxRecord("box"); err != nil {
				t.Fatal("refused purge removed the box:", err)
			}
		})
	}
}

func TestPurgeAtomicallyDeletesBoxHistoryAndPreservesArchivesAndNeighbors(t *testing.T) {
	store := newTestStore(t)
	seedPurge(t, store)
	if err := store.Write(context.Background(), func(tx *storeTx) error {
		if err := tx.PutBox(boxRecord{ClientID: "owner", Box: Box{ID: "neighbor", Phase: "running"}}); err != nil {
			return err
		}
		if err := tx.PutOperation(operationRecord{ClientID: "other", Operation: Operation{ID: "foreign", TargetID: "different-box", Status: "succeeded"}}); err != nil {
			return err
		}
		return tx.PutExecution(executionRecord{Execution: Execution{ID: "neighbor-exec", BoxID: "neighbor", State: "exited", Result: &guestapi.ExecResult{Stdout: "keep"}}})
	}); err != nil {
		t.Fatal(err)
	}
	if err := purgeService(store).purgeBox(context.Background(), "owner", "box"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BoxRecord("box"); err == nil {
		t.Fatal("purged box remains")
	}
	if _, err := store.Operation("owner", "create"); err == nil {
		t.Fatal("purged operation history remains")
	}
	if _, err := readRecord[executionRecord](context.Background(), store.db, "executions", "exec"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("purged execution remains: %v", err)
	}
	if _, err := readRecord[guestapi.ExecResult](context.Background(), store.db, "execution_results", "exec"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("purged execution output remains: %v", err)
	}
	if _, err := store.ArchiveRecord("owner", "archive"); err != nil {
		t.Fatal("purge removed independent archive metadata:", err)
	}
	if _, err := store.BoxRecord("neighbor"); err != nil {
		t.Fatal("purge removed another box:", err)
	}
	if _, err := store.Operation("other", "foreign"); err != nil {
		t.Fatal("purge removed another tenant's record:", err)
	}
	neighborExecution, err := readRecord[executionRecord](context.Background(), store.db, "executions", "neighbor-exec")
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.executionResult(context.Background(), neighborExecution)
	if err != nil || result.Result == nil || result.Result.Stdout != "keep" {
		t.Fatalf("purge damaged neighbor output: %+v err=%v", result, err)
	}
}

func TestRetentionExpiresCompletedOutputAndKeepsUnknownExecution(t *testing.T) {
	store := newTestStore(t)
	finished := time.Now().Add(-48 * time.Hour)
	if err := store.Write(context.Background(), func(tx *storeTx) error {
		if err := tx.PutOperation(operationRecord{ClientID: "owner", Operation: Operation{ID: "done", TargetID: "deleted-box", Status: "succeeded", FinishedAt: &finished}}); err != nil {
			return err
		}
		if err := tx.PutExecution(executionRecord{Execution: Execution{ID: "exec", BoxID: "deleted-box", OperationID: "done", State: "exited", Result: &guestapi.ExecResult{Stdout: "expire"}}}); err != nil {
			return err
		}
		if err := tx.PutOperation(operationRecord{ClientID: "owner", Operation: Operation{ID: "uncertain", TargetID: "other-box", Status: "failed", FinishedAt: &finished}}); err != nil {
			return err
		}
		return tx.PutExecution(executionRecord{Execution: Execution{ID: "unknown", BoxID: "other-box", OperationID: "uncertain", State: "unknown"}})
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Prune(context.Background(), time.Now(), time.Hour, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Operation("owner", "done"); err == nil {
		t.Fatal("expired operation remains")
	}
	if _, err := readRecord[executionRecord](context.Background(), store.db, "executions", "exec"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expired execution metadata remains: %v", err)
	}
	if _, err := readRecord[guestapi.ExecResult](context.Background(), store.db, "execution_results", "exec"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expired output remains: %v", err)
	}
	uncertain, err := store.Operation("owner", "uncertain")
	if err != nil || uncertain.ID != "uncertain" {
		t.Fatalf("unknown execution operation was pruned: %+v err=%v", uncertain, err)
	}
	unknown, err := readRecord[executionRecord](context.Background(), store.db, "executions", "unknown")
	if err != nil {
		t.Fatal("unknown execution metadata was pruned:", err)
	}
	if unknown.State != "unknown" || unknown.BoxID != "other-box" {
		t.Fatalf("unknown execution metadata changed: %+v", unknown)
	}
	if _, err := readRecord[guestapi.ExecResult](context.Background(), store.db, "execution_results", "unknown"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unknown execution output should not be synthesized: %v", err)
	}
}
