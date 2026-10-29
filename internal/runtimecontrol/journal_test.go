package runtimecontrol

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	_ "modernc.org/sqlite"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func openTestJournal(t *testing.T) (*sql.DB, *Journal, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cellbox.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(4)
	_, err = db.Exec(`CREATE TABLE runtime_records (
        kind TEXT NOT NULL, id TEXT NOT NULL, client_id TEXT NOT NULL DEFAULT '',
        box_id TEXT NOT NULL DEFAULT '', record TEXT NOT NULL, PRIMARY KEY(kind,id))`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, NewJournal(db), path
}

func journalRecord(name string) *Record {
	return &Record{Resource: &api.ResumablePod{ObjectMeta: meta.ObjectMeta{
		Namespace: "sandbox", Name: name, UID: types.UID("box-" + name),
	}}}
}

func TestJournalPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	db, journal, path := openTestJournal(t)
	record := journalRecord("demo")
	record.Pending = true
	record.Pod = &core.Pod{ObjectMeta: meta.ObjectMeta{Namespace: "sandbox", Name: "warm-1", UID: "pod-1"}}
	if err := journal.Save(ctx, record, 0); err != nil {
		t.Fatal(err)
	}
	if record.Revision != 1 {
		t.Fatalf("revision = %d, want 1", record.Revision)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	loaded, err := NewJournal(db2).Load(ctx, client.ObjectKey{Namespace: "sandbox", Name: "demo"})
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil || loaded.Revision != 1 || !loaded.Pending || loaded.Pod == nil || loaded.Pod.UID != "pod-1" || loaded.Resource.UID != "box-demo" {
		t.Fatalf("unexpected persisted record: %#v", loaded)
	}
}

func TestJournalRejectsStaleRevision(t *testing.T) {
	ctx := context.Background()
	_, journal, _ := openTestJournal(t)
	record := journalRecord("demo")
	if err := journal.Save(ctx, record, 0); err != nil {
		t.Fatal(err)
	}
	update := journalRecord("demo")
	update.Complete = true
	if err := journal.Save(ctx, update, 1); err != nil {
		t.Fatal(err)
	}
	stale := journalRecord("demo")
	if err := journal.Save(ctx, stale, 1); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("stale Save error = %v, want ErrJournalConflict", err)
	}
	if stale.Revision != 0 {
		t.Fatalf("failed Save mutated caller revision to %d", stale.Revision)
	}
}

func TestJournalOnlyOnePendingRecordCanClaimPod(t *testing.T) {
	ctx := context.Background()
	_, journal, _ := openTestJournal(t)
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, name := range []string{"one", "two"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			<-start
			record := journalRecord(name)
			record.Pending = true
			record.Pod = &core.Pod{ObjectMeta: meta.ObjectMeta{Namespace: "sandbox", Name: "warm-1", UID: "same-pod-uid"}}
			results <- journal.Save(ctx, record, 0)
		}(name)
	}
	close(start)
	wg.Wait()
	close(results)
	wins := 0
	claimed := 0
	for err := range results {
		if err == nil {
			wins++
		} else if errors.Is(err, ErrPodClaimed) {
			claimed++
		} else {
			t.Fatalf("unexpected Save error: %v", err)
		}
	}
	if wins != 1 || claimed != 1 {
		t.Fatalf("successful claims=%d, rejected claims=%d; want 1 each", wins, claimed)
	}
}

func TestJournalReleasesPodClaimWhenPendingCleared(t *testing.T) {
	ctx := context.Background()
	_, journal, _ := openTestJournal(t)
	first := journalRecord("one")
	first.Pending = true
	first.Pod = &core.Pod{ObjectMeta: meta.ObjectMeta{Namespace: "sandbox", Name: "warm-1", UID: "pod-1"}}
	if err := journal.Save(ctx, first, 0); err != nil {
		t.Fatal(err)
	}
	first.Pending = false
	if err := journal.Save(ctx, first, first.Revision); err != nil {
		t.Fatal(err)
	}
	second := journalRecord("two")
	second.Pending = true
	second.Pod = first.Pod.DeepCopy()
	if err := journal.Save(ctx, second, 0); err != nil {
		t.Fatalf("claim after pending cleared: %v", err)
	}
	list, err := journal.ListPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Resource.Name != "two" {
		t.Fatalf("pending records = %#v, want only second record", list)
	}
}
