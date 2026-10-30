package service

import (
	"context"
	"database/sql"
	"errors"
	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/guestapi"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// updateRecords is a test fixture helper. Production code uses Store.Write.
func updateRecords(t *testing.T, s *Store, fn func(*State)) {
	t.Helper()
	if err := s.Update(func(st *State) error { fn(st); return nil }); err != nil {
		t.Fatal(err)
	}
}

func coreBox(id string) boxRecord {
	return boxRecord{ClientID: "owner", Box: Box{ID: id, Phase: "running", ProfileID: "default", Generation: 7},
		Profile: Profile{ID: "default", Provider: "resumable-k8s-pod", Image: "prepared@sha256:abc", Guest: guestapi.DefaultConfig()},
		Handle:  boxprovider.Handle{ID: "cr-" + id, Provider: "resumable-k8s-pod"}, ExecutionID: "pod-" + id}
}

func TestSQLiteStoreWriteRollsBackBoxAndOperationTogether(t *testing.T) {
	store := newTestStore(t)
	abort := errors.New("abort transaction")
	err := store.Write(context.Background(), func(tx *storeTx) error {
		if err := tx.PutBox(coreBox("box")); err != nil {
			return err
		}
		if err := tx.PutOperation(operationRecord{ClientID: "owner", Operation: Operation{ID: "op", TargetID: "box", Status: "running"}}); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatalf("transaction error = %v, want abort", err)
	}
	if _, err := store.BoxRecord("box"); err == nil {
		t.Fatal("box write survived rollback")
	}
	if _, err := store.Operation("owner", "op"); err == nil {
		t.Fatal("operation write survived rollback")
	}
}

func TestSQLiteStorePersistsRecordsAndExecutionOutputAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(context.Background(), func(tx *storeTx) error {
		if err := tx.PutBox(coreBox("box")); err != nil {
			return err
		}
		if err := tx.PutOperation(operationRecord{ClientID: "owner", Operation: Operation{ID: "op", TargetID: "box", Status: "succeeded"}}); err != nil {
			return err
		}
		return tx.PutExecution(executionRecord{Execution: Execution{ID: "exec", BoxID: "box", OperationID: "op", State: "exited", Result: &guestapi.ExecResult{Stdout: "persisted output"}}})
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	box, err := reopened.BoxRecord("box")
	if err != nil || box.Box.ID != "box" || box.Handle.ID != "cr-box" {
		t.Fatalf("box not persisted: %+v err=%v", box, err)
	}
	op, err := reopened.Operation("owner", "op")
	if err != nil || op.Status != "succeeded" {
		t.Fatalf("operation not persisted: %+v err=%v", op, err)
	}
	execution, err := reopened.ExecutionRecord("owner", "exec")
	if err != nil || execution.Result != nil {
		t.Fatalf("execution metadata read included output: %+v err=%v", execution, err)
	}
	result, err := reopened.executionResult(context.Background(), execution)
	if err != nil || result.Result == nil || result.Result.Stdout != "persisted output" {
		t.Fatalf("execution result not persisted: %+v err=%v", result, err)
	}
}

func TestSQLiteStoreChangeBoxAndObservationNoOpUseDirectRows(t *testing.T) {
	store := newTestStore(t)
	if err := store.Write(context.Background(), func(tx *storeTx) error { return tx.PutBox(coreBox("box")) }); err != nil {
		t.Fatal(err)
	}
	base, err := store.BoxRecord("box")
	if err != nil {
		t.Fatal(err)
	}
	store.db.SetMaxOpenConns(1)
	var changes func() int64
	changes = func() int64 {
		var count int64
		if err := store.db.QueryRow("SELECT total_changes()").Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	before := changes()
	if _, err := store.UpdateObservedBox(base, func(*boxRecord) (bool, error) { return false, nil }); err != nil {
		t.Fatal(err)
	}
	if after := changes(); after != before {
		t.Fatalf("no-op observation wrote SQL rows: total_changes %d -> %d", before, after)
	}
	if err := store.ChangeBox(context.Background(), "box", func(box *boxRecord) error {
		box.Box.OwnerKey = "changed"
		box.Box.Version++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	box, err := store.BoxRecord("box")
	if err != nil || box.Box.OwnerKey != "changed" {
		t.Fatalf("ChangeBox did not persist direct update: %+v err=%v", box, err)
	}
	if _, err := store.UpdateObservedBox(base, func(*boxRecord) (bool, error) { return true, nil }); !errors.Is(err, errBoxObservationConflict) {
		t.Fatalf("stale observation was accepted: %v", err)
	}
}

func TestSQLiteStoreRejectsSecondOwnerOfDataDirectory(t *testing.T) {
	dir := t.TempDir()
	first, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := OpenStore(dir); err == nil {
		_ = second.Close()
		t.Fatal("second store opened the same data directory")
	}
}

func TestSQLiteStoreConcurrentBoxWritesDoNotLoseUpdates(t *testing.T) {
	store := newTestStore(t)
	if err := store.Write(context.Background(), func(tx *storeTx) error {
		if err := tx.PutBox(coreBox("a")); err != nil {
			return err
		}
		return tx.PutBox(coreBox("b"))
	}); err != nil {
		t.Fatal(err)
	}
	const updates = 20
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, id := range []string{"a", "b"} {
		go func(id string) {
			<-start
			for range updates {
				err := store.Write(context.Background(), func(tx *storeTx) error {
					box, err := tx.Box("owner", id)
					if err != nil {
						return err
					}
					box.Box.Generation++
					return tx.PutBox(box)
				})
				if err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}(id)
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"a", "b"} {
		box, err := store.BoxRecord(id)
		if err != nil || box.Box.Generation != 7+updates {
			t.Fatalf("box %s generation=%d err=%v", id, box.Box.Generation, err)
		}
	}
}

func TestSQLiteStoreRelaxesOnlyTheResumePool(t *testing.T) {
	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, pool := range []struct {
		db   *sql.DB
		want int
	}{{store.db, 2}, {store.relaxed, 1}} {
		var level int
		if err := pool.db.QueryRow("PRAGMA synchronous").Scan(&level); err != nil {
			t.Fatal(err)
		}
		if level != pool.want {
			t.Fatalf("synchronous = %d, want %d", level, pool.want)
		}
	}
	var mode string
	if err := store.relaxed.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("relaxed journal mode = %q err=%v", mode, err)
	}
}
