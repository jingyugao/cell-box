package runtimecontrol

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"
)

const writeBehindDelay = 100 * time.Millisecond

// writeBehind makes memory the source of truth for the journal. A commit is one
// unsynced append to a log, so it survives a process crash without touching
// SQLite. The log is replayed on open and emptied once SQLite has caught up.
// Power loss can lose it; the durable consumed marker then refuses the replay.
type writeBehind struct {
	mu      sync.Mutex
	records map[string]*Record
	// A nil value is a deletion.
	dirty map[string][]byte
	log   *os.File
	size  int64

	flushMu sync.Mutex
	delay   time.Duration
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
}

type writeBehindEntry struct {
	ID     string          `json:"id"`
	Record json.RawMessage `json:"record,omitempty"`
}

// OpenRelaxedJournal loads every record into memory. db and durable are pools on
// one WAL database; db does not wait for the disk and durable is synchronous=FULL.
func OpenRelaxedJournal(ctx context.Context, db, durable *sql.DB, logPath string) (*Journal, error) {
	return openRelaxedJournal(ctx, db, durable, logPath, writeBehindDelay)
}

func openRelaxedJournal(ctx context.Context, db, durable *sql.DB, logPath string, delay time.Duration) (*Journal, error) {
	j := &Journal{db: db, durable: durable}
	stored, err := j.list(ctx, false)
	if err != nil {
		return nil, err
	}
	w := &writeBehind{records: make(map[string]*Record, len(stored)), dirty: map[string][]byte{}, delay: delay,
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	for i := range stored {
		w.records[journalID(clientKey(stored[i].Resource))] = &stored[i]
	}
	if err := w.replay(logPath); err != nil {
		return nil, err
	}
	if w.log, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600); err != nil {
		return nil, err
	}
	info, err := w.log.Stat()
	if err != nil {
		w.log.Close()
		return nil, err
	}
	w.size = info.Size()
	j.wb = w
	// Replayed commits reach SQLite before anything newer can be appended.
	if err := j.flush(ctx); err != nil {
		w.log.Close()
		return nil, err
	}
	go j.flushLoop()
	return j, nil
}

// replay stops at the first damaged line. Only power loss or a crash during an
// append produces one, and nothing after it can have been acknowledged safely.
func (w *writeBehind) replay(path string) error {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for len(data) > 0 {
		end := bytes.IndexByte(data, '\n')
		if end < 0 {
			slog.Warn("ignoring a partial runtime journal log entry")
			return nil
		}
		var sum uint32
		var entry writeBehindEntry
		line := data[:end]
		data = data[end+1:]
		if len(line) < 9 {
			slog.Warn("ignoring a damaged runtime journal log entry")
			return nil
		}
		if _, err := fmt.Sscanf(string(line[:8]), "%08x", &sum); err != nil || sum != crc32.ChecksumIEEE(line[9:]) || json.Unmarshal(line[9:], &entry) != nil {
			slog.Warn("ignoring a damaged runtime journal log entry")
			return nil
		}
		if entry.Record == nil {
			delete(w.records, entry.ID)
			w.dirty[entry.ID] = nil
			continue
		}
		var record Record
		if err := json.Unmarshal(entry.Record, &record); err != nil || record.Resource == nil {
			slog.Warn("ignoring a damaged runtime journal log entry")
			return nil
		}
		w.records[entry.ID] = &record
		w.dirty[entry.ID] = entry.Record
	}
	return nil
}

func copyRecord(r *Record) *Record {
	c := *r
	c.Base, c.Resource, c.Pod = r.Base.DeepCopy(), r.Resource.DeepCopy(), r.Pod.DeepCopy()
	return &c
}

// appendLocked commits one change. A failed append leaves memory untouched.
func (w *writeBehind) appendLocked(id string, record []byte) error {
	data, err := json.Marshal(writeBehindEntry{ID: id, Record: record})
	if err != nil {
		return err
	}
	line := fmt.Appendf(nil, "%08x %s\n", crc32.ChecksumIEEE(data), data)
	if _, err := w.log.Write(line); err != nil {
		return errors.Join(fmt.Errorf("append runtime journal log: %w", err), w.log.Truncate(w.size))
	}
	w.size += int64(len(line))
	w.dirty[id] = record
	select {
	case w.wake <- struct{}{}:
	default:
	}
	return nil
}

func (w *writeBehind) load(id string) *Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	if r := w.records[id]; r != nil {
		return copyRecord(r)
	}
	return nil
}

func (w *writeBehind) save(id string, record *Record, expected uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	current := w.records[id]
	if (expected == 0) != (current == nil) || (current != nil && current.Revision != expected) {
		return ErrJournalConflict
	}
	if record.Pending && record.Pod != nil && record.Pod.UID != "" {
		for other, r := range w.records {
			if other != id && r.Pending && r.Pod != nil && r.Pod.Namespace == record.Pod.Namespace && r.Pod.UID == record.Pod.UID {
				return ErrPodClaimed
			}
		}
	}
	stored := copyRecord(record)
	stored.Revision = expected + 1
	data, err := json.Marshal(stored)
	if err != nil {
		return fmt.Errorf("encode runtime control journal record: %w", err)
	}
	if err := w.appendLocked(id, data); err != nil {
		return err
	}
	w.records[id] = stored
	record.Revision = stored.Revision
	return nil
}

func (w *writeBehind) delete(id string, expected uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	current := w.records[id]
	if current == nil || current.Revision != expected {
		return ErrJournalConflict
	}
	if err := w.appendLocked(id, nil); err != nil {
		return err
	}
	delete(w.records, id)
	return nil
}

func (w *writeBehind) list(pendingOnly bool) []Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	ids := make([]string, 0, len(w.records))
	for id, r := range w.records {
		if !pendingOnly || r.Pending {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	records := make([]Record, 0, len(ids))
	for _, id := range ids {
		records = append(records, *copyRecord(w.records[id]))
	}
	return records
}

func (j *Journal) flushLoop() {
	w := j.wb
	defer close(w.done)
	for {
		select {
		case <-w.stop:
			return
		case <-w.wake:
		}
		select {
		case <-w.stop:
			return
		case <-time.After(w.delay):
		}
		if err := j.flush(context.Background()); err != nil {
			slog.Error("cannot write the runtime journal to SQLite; will retry", "error", err)
			select {
			case w.wake <- struct{}{}:
			default:
			}
		}
	}
}

// flush copies the latest commits to SQLite, then empties the log if nothing
// newer arrived. A crash between the two replays commits that are already stored,
// which ends in the same state.
func (j *Journal) flush(ctx context.Context) error {
	w := j.wb
	w.flushMu.Lock()
	defer w.flushMu.Unlock()
	w.mu.Lock()
	batch := w.dirty
	w.dirty = map[string][]byte{}
	w.mu.Unlock()
	err := j.writeBatch(ctx, batch)
	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil {
		for id, data := range batch {
			if _, newer := w.dirty[id]; !newer {
				w.dirty[id] = data
			}
		}
		return err
	}
	if len(w.dirty) == 0 && w.size > 0 {
		if err := w.log.Truncate(0); err != nil {
			return fmt.Errorf("empty runtime journal log: %w", err)
		}
		w.size = 0
	}
	return nil
}

func (j *Journal) writeBatch(ctx context.Context, batch map[string][]byte) error {
	if len(batch) == 0 {
		return nil
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin runtime control journal transaction: %w", err)
	}
	defer tx.Rollback()
	for id, data := range batch {
		if data == nil {
			_, err = tx.ExecContext(ctx, `DELETE FROM runtime_records WHERE kind=? AND id=?`, journalKind, id)
		} else {
			_, err = tx.ExecContext(ctx, `INSERT INTO runtime_records(kind,id,client_id,box_id,record) VALUES(?,?,'',json_extract(?,'$.resource.metadata.uid'),?)
 ON CONFLICT(kind,id) DO UPDATE SET client_id='', box_id=excluded.box_id, record=excluded.record`, journalKind, id, string(data), string(data))
		}
		if err != nil {
			return fmt.Errorf("write runtime control journal record: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit runtime control journal records: %w", err)
	}
	return nil
}

// Close stores all commits in SQLite. Stop the runtime workers first.
func (j *Journal) Close(ctx context.Context) error {
	w := j.wb
	if w == nil {
		return nil
	}
	close(w.stop)
	<-w.done
	return errors.Join(j.flush(ctx), w.log.Close())
}
