package service

import (
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/telemetry"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

// Store owns the single local database. Operations read and write only their records.
type Store struct {
	db       *sql.DB
	dir      string
	lock     *os.File
	writerMu sync.Mutex
	mu       sync.Mutex
	changes  chan struct{}
}

func lockDataDirectory(dir string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("data directory must be a real directory")
	}
	fd, err := unix.Open(filepath.Join(dir, "service.lock"), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "service.lock")
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another Cellbox service owns this data directory: %w", err)
	}
	return f, nil
}

func OpenStore(dir string) (*Store, error) {
	lock, err := lockDataDirectory(dir)
	if err != nil {
		return nil, err
	}
	failed := true
	defer func() {
		if failed {
			lock.Close()
		}
	}()
	for _, old := range []string{"state.json", "catalog.db"} {
		if _, err := os.Lstat(filepath.Join(dir, old)); err == nil {
			return nil, fmt.Errorf("legacy metadata %s is not supported; use a new data directory", old)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}
	name, err := filepath.Abs(filepath.Join(dir, "cellbox.db"))
	if err != nil {
		return nil, err
	}
	if info, err := os.Lstat(name); err == nil && !info.Mode().IsRegular() {
		return nil, errors.New("database must be a regular file")
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	// Configure every pooled connection. Immediate transactions serialize writers
	// before reading their snapshot; WAL allows catalog reads during admission.
	uri := url.URL{Scheme: "file", Path: name}
	query := url.Values{"_pragma": {"busy_timeout(5000)", "foreign_keys(1)", "synchronous(FULL)"}, "_txlock": {"immediate"}}
	uri.RawQuery = query.Encode()
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, err
	}
	defer func() {
		if failed {
			db.Close()
		}
	}()
	db.SetMaxOpenConns(4)
	if _, err = db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return nil, err
	}
	if err = os.Chmod(name, 0600); err != nil {
		return nil, err
	}
	var version int
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return nil, err
	}
	if version != 0 && version != 3 {
		return nil, fmt.Errorf("unsupported database schema %d", version)
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS runtime_records (
 kind TEXT NOT NULL, id TEXT NOT NULL, client_id TEXT NOT NULL DEFAULT '', box_id TEXT NOT NULL DEFAULT '', record TEXT NOT NULL,
 PRIMARY KEY(kind,id));
 CREATE INDEX IF NOT EXISTS runtime_owner ON runtime_records(kind,client_id);
 CREATE INDEX IF NOT EXISTS runtime_box ON runtime_records(kind,box_id);
 INSERT OR IGNORE INTO runtime_records(kind,id,client_id,record)
 SELECT 'boxes_ids',id,client_id,'{}' FROM runtime_records WHERE kind='boxes';
 PRAGMA user_version=3;`)
	if err != nil {
		return nil, err
	}
	failed = false
	return &Store{db: db, dir: dir, lock: lock}, nil
}

func (s *Store) Close() error {
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	err := s.db.Close()
	if s.lock != nil {
		err = errors.Join(err, s.lock.Close())
		s.lock = nil
	}
	return err
}

type sqlReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// storeTx is a SQLite transaction, not a snapshot of all business records.
type storeTx struct {
	ctx     context.Context
	tx      *sql.Tx
	changed bool
}

func (s *Store) Write(ctx context.Context, fn func(*storeTx) error) (retErr error) {
	ctx, span := telemetry.Start(ctx, "cellbox.storage.transaction", telemetry.String("storage.backend", "sqlite"))
	defer func() { telemetry.End(span, retErr) }()
	s.writerMu.Lock()
	defer s.writerMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	writer := &storeTx{ctx: ctx, tx: tx}
	if err = fn(writer); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if writer.changed {
		s.mu.Lock()
		s.notifyLocked()
		s.mu.Unlock()
	}
	return nil
}

func (t *storeTx) put(kind, id, client, boxID string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	result, err := t.tx.ExecContext(t.ctx, `INSERT INTO runtime_records(kind,id,client_id,box_id,record) VALUES(?,?,?,?,?)
        ON CONFLICT(kind,id) DO UPDATE SET client_id=excluded.client_id,box_id=excluded.box_id,record=excluded.record
        WHERE record!=excluded.record OR client_id!=excluded.client_id OR box_id!=excluded.box_id`, kind, id, client, boxID, string(data))
	if err == nil {
		n, _ := result.RowsAffected()
		t.changed = t.changed || n > 0
	}
	return err
}
func (t *storeTx) exec(query string, args ...any) error {
	result, err := t.tx.ExecContext(t.ctx, query, args...)
	if err == nil {
		n, _ := result.RowsAffected()
		t.changed = t.changed || n > 0
	}
	return err
}
func (t *storeTx) Delete(kind, id string) error {
	return t.exec("DELETE FROM runtime_records WHERE kind=? AND id=?", kind, id)
}
func (t *storeTx) Box(client, id string) (boxRecord, error) {
	b, err := readRecord[boxRecord](t.ctx, t.tx, "boxes", id)
	if errors.Is(err, sql.ErrNoRows) || err == nil && client != "" && b.ClientID != client {
		return boxRecord{}, apiError("NOT_FOUND", "Box not found")
	}
	return b, err
}
func (t *storeTx) PutBox(b boxRecord) error {
	if err := t.put("boxes", b.Box.ID, b.ClientID, b.Box.ID, b); err != nil {
		return err
	}
	if b.Box.Phase == "deleted" {
		return finishLegacyVolume(t, b)
	}
	return nil
}
func (t *storeTx) Operation(id string) (operationRecord, error) {
	return readRecord[operationRecord](t.ctx, t.tx, "operations", id)
}
func (t *storeTx) PutOperation(r operationRecord) error {
	boxID := r.Operation.TargetID
	if r.Operation.Kind == "volume-create" || r.Operation.Kind == "volume-delete" {
		boxID = "" // A volume can share a public name with a Box; purge must not touch its work.
	}
	return t.put("operations", r.Operation.ID, r.ClientID, boxID, r)
}
func (t *storeTx) Execution(id string) (executionRecord, error) {
	return readRecord[executionRecord](t.ctx, t.tx, "executions", id)
}
func (t *storeTx) PutExecution(e executionRecord) error {
	if e.Result != nil {
		if err := t.put("execution_results", e.ID, "", e.BoxID, e.Result); err != nil {
			return err
		}
	}
	if e.State == "expired" {
		if err := t.Delete("execution_results", e.ID); err != nil {
			return err
		}
	}
	e.Result = nil
	return t.put("executions", e.ID, "", e.BoxID, e)
}
func (t *storeTx) HasActiveExecution(id string) (bool, error) {
	var yes bool
	err := t.tx.QueryRowContext(t.ctx, `SELECT EXISTS(SELECT 1 FROM runtime_records WHERE kind='operations' AND box_id=?
        AND json_extract(record,'$.operation.kind')='exec' AND json_extract(record,'$.operation.status') IN ('queued','running'))`, id).Scan(&yes)
	return yes, err
}
func (t *storeTx) Archive(client, id string) (archiveRecord, error) {
	a, err := readRecord[archiveRecord](t.ctx, t.tx, "archives", id)
	if errors.Is(err, sql.ErrNoRows) || err == nil && a.ClientID != client {
		return archiveRecord{}, apiError("NOT_FOUND", "Archive not found")
	}
	return a, err
}
func (t *storeTx) PutArchive(a archiveRecord) error {
	return t.put("archives", a.Archive.ID, a.ClientID, "", a)
}
func (t *storeTx) ArchiveInUse(id string) (bool, error) {
	var yes bool
	err := t.tx.QueryRowContext(t.ctx, `SELECT EXISTS(SELECT 1 FROM runtime_records b JOIN runtime_records o
        ON o.kind='operations' AND o.id=json_extract(b.record,'$.box.operationId')
        WHERE b.kind='boxes' AND json_extract(b.record,'$.restoreArchiveId')=?
        AND json_extract(o.record,'$.operation.status') IN ('queued','running'))`, id).Scan(&yes)
	return yes, err
}
func (s *Store) ChangeBox(ctx context.Context, id string, fn func(*boxRecord) error) error {
	return s.Write(ctx, func(t *storeTx) error {
		b, err := t.Box("", id)
		if err != nil {
			return err
		}
		if err = fn(&b); err != nil {
			return err
		}
		return t.PutBox(b)
	})
}
func (s *Store) ArchiveRecord(client, id string) (archiveRecord, error) {
	a, err := readRecord[archiveRecord](context.Background(), s.db, "archives", id)
	if errors.Is(err, sql.ErrNoRows) || err == nil && a.ClientID != client {
		return archiveRecord{}, apiError("NOT_FOUND", "Archive not found")
	}
	return a, err
}
func (s *Store) ArchiveRecords(client string) ([]archiveRecord, error) {
	return listRecords[archiveRecord](context.Background(), s.db, "archives", "AND client_id=?", client)
}
func (s *Store) ExecutionRecord(client, id string) (executionRecord, error) {
	var data []byte
	err := s.db.QueryRow(`SELECT e.record FROM runtime_records e JOIN runtime_records b ON b.kind='boxes' AND b.id=e.box_id
        WHERE e.kind='executions' AND e.id=? AND b.client_id=?`, id, client).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return executionRecord{}, apiError("NOT_FOUND", "Execution not found")
	}
	var e executionRecord
	if err == nil {
		err = json.Unmarshal(data, &e)
	}
	return e, err
}

// Interrupted work is not replayed. Runtime observation may still reveal its effects.
func (s *Store) InterruptOperations(ctx context.Context) error {
	return s.Write(ctx, func(t *storeTx) error {
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if err := t.exec(`UPDATE runtime_records SET record=json_set(record,'$.state','unknown')
            WHERE kind='executions' AND json_extract(record,'$.state')='running'`); err != nil {
			return err
		}
		if err := t.exec(`UPDATE runtime_records SET record=json_set(record,'$.operation.status','failed',
            '$.operation.error',json('{"code":"UNKNOWN_OUTCOME","message":"Service restarted before completion; inspect the resource before retrying"}'),
            '$.operation.finishedAt',?,'$.operation.version',json_extract(record,'$.operation.version')+1)
            WHERE kind='operations' AND json_extract(record,'$.operation.status') IN ('queued','running')`, now); err != nil {
			return err
		}
		volumes, err := listRecords[volumeRecord](t.ctx, t.tx, "volumes", `AND (json_extract(record,'$.volume.phase') IN ('creating','deleting','backing-up')
              OR coalesce(json_extract(record,'$.volume.operationId'),'')!='')`)
		if err != nil {
			return err
		}
		for _, volume := range volumes {
			volume.Volume.OperationID = ""
			volume.Volume.Version++
			if volume.Volume.Phase == "creating" || volume.Volume.Phase == "deleting" {
				volume.Volume.Phase = "failed"
				volume.Volume.Error = &APIError{Code: "UNKNOWN_OUTCOME", Message: "Service restarted before completion; inspect the volume before deleting or retrying"}
			} else {
				// Backup sources and restore repository reservations never write
				// this filesystem. Keep their original availability on restart.
				restoreBackupSourceState(&volume)
			}
			if err := t.PutVolume(volume); err != nil {
				return err
			}
		}
		return t.exec(`UPDATE runtime_records SET record=json_set(record,'$.box.operationId','',
            '$.box.resourceVersion',json_extract(record,'$.box.resourceVersion')+1,
            '$.box.phase',CASE WHEN json_extract(record,'$.box.phase') IN ('creating','restoring') THEN 'failed' ELSE json_extract(record,'$.box.phase') END)
            WHERE kind='boxes' AND coalesce(json_extract(record,'$.box.operationId'),'')!=''`)
	})
}

func readRecord[T any](ctx context.Context, db sqlReader, kind, id string) (T, error) {
	var out T
	var data []byte
	err := db.QueryRowContext(ctx, "SELECT record FROM runtime_records WHERE kind=? AND id=?", kind, id).Scan(&data)
	if err == nil {
		err = json.Unmarshal(data, &out)
	}
	return out, err
}

func listRecords[T any](ctx context.Context, db sqlReader, kind string, where string, args ...any) ([]T, error) {
	rows, err := db.QueryContext(ctx, "SELECT record FROM runtime_records WHERE kind=? "+where, append([]any{kind}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []T{}
	for rows.Next() {
		var data []byte
		var value T
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(data, &value); err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, rows.Err()
}

func (s *Store) BoxRecords(client string, ids []string) ([]boxRecord, error) {
	if len(ids) == 0 {
		return listRecords[boxRecord](context.Background(), s.db, "boxes", "AND client_id=?", client)
	}
	out := make([]boxRecord, 0, len(ids))
	for _, id := range ids {
		record, err := s.BoxRecord(id)
		if err != nil {
			return nil, err
		}
		if record.ClientID != client {
			return nil, apiError("NOT_FOUND", "Box not found")
		}
		out = append(out, record)
	}
	return out, nil
}
func (s *Store) RuntimeBoxRecords() ([]boxRecord, error) {
	return listRecords[boxRecord](context.Background(), s.db, "boxes", "AND json_extract(record,'$.handle.id')!='' AND json_extract(record,'$.box.phase')!='deleted'")
}
func (s *Store) BoxRecord(id string) (boxRecord, error) {
	b, err := readRecord[boxRecord](context.Background(), s.db, "boxes", id)
	if errors.Is(err, sql.ErrNoRows) {
		err = apiError("NOT_FOUND", "Box not found")
	}
	return b, err
}
func (s *Store) Operation(client, id string) (Operation, error) {
	r, err := readRecord[operationRecord](context.Background(), s.db, "operations", id)
	if errors.Is(err, sql.ErrNoRows) || err == nil && r.ClientID != client {
		return Operation{}, apiError("NOT_FOUND", "Operation not found")
	}
	return r.Operation, err
}

var errBoxObservationConflict = errors.New("box changed during observation")

func sameObservationBase(a, b boxRecord) bool {
	return a.Box.Version == b.Box.Version && a.Box.Generation == b.Box.Generation && a.Box.Phase == b.Box.Phase &&
		a.Box.OperationID == b.Box.OperationID && a.Handle == b.Handle && a.ExecutionID == b.ExecutionID &&
		a.TransitionID == b.TransitionID && a.ObservedTransitionID == b.ObservedTransitionID &&
		a.Staged == b.Staged && a.RestoreArchiveID == b.RestoreArchiveID && a.RestoreComplete == b.RestoreComplete
}
func (s *Store) UpdateObservedBox(base boxRecord, fn func(*boxRecord) (bool, error)) (boxRecord, error) {
	var current boxRecord
	err := s.Write(context.Background(), func(t *storeTx) error {
		var err error
		current, err = t.Box("", base.Box.ID)
		if err != nil {
			return err
		}
		if !sameObservationBase(base, current) {
			return errBoxObservationConflict
		}
		changed, err := fn(&current)
		if err != nil || !changed {
			return err
		}
		return t.PutBox(current)
	})
	return current, err
}

func (s *Store) executionResult(ctx context.Context, e executionRecord) (Execution, error) {
	if e.State == "expired" {
		return Execution{}, apiError("RESULT_EXPIRED", "Execution output retention window has expired")
	}
	if e.Result == nil && e.State == "exited" {
		result, err := readRecord[guestapi.ExecResult](ctx, s.db, "execution_results", e.ID)
		if err != nil {
			return Execution{}, err
		}
		e.Result = &result
	}
	return e.Execution, nil
}
