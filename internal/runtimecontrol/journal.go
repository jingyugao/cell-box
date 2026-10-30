package runtimecontrol

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	core "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const journalKind = "runtime_control"

var (
	// ErrJournalConflict means the caller's expected revision is no longer current.
	ErrJournalConflict = errors.New("runtime control journal revision conflict")
	// ErrPodClaimed means another pending record already owns this Pod UID.
	ErrPodClaimed = errors.New("pod is already claimed by another pending runtime control record")
)

// Record is the durable local view of one Cellbox operation. Revision belongs
// to this journal and is deliberately independent of Kubernetes ResourceVersion.
type Record struct {
	Base     *api.ResumablePod `json:"base,omitempty"`
	Resource *api.ResumablePod `json:"resource"`
	Pod      *core.Pod         `json:"pod,omitempty"`
	Revision uint64            `json:"revision"`
	Pending  bool              `json:"pending"`
	Complete bool              `json:"complete"`
}

// Journal stores resume records in the service's shared runtime_records table.
// Without a write-behind it keeps no process-local copy and every operation
// observes SQLite state. The running server opens it with OpenRelaxedJournal.
type Journal struct {
	db      *sql.DB
	durable *sql.DB
	wb      *writeBehind
}

func NewJournal(db *sql.DB) *Journal { return &Journal{db: db} }

// Barrier makes every earlier commit survive power loss, because a FULL commit
// syncs the whole WAL. Call it before publishing local state outside this node.
func (j *Journal) Barrier(ctx context.Context) error {
	if j.wb != nil {
		if err := j.flush(ctx); err != nil {
			return err
		}
	}
	if j.durable == nil {
		return nil
	}
	if _, err := j.durable.ExecContext(ctx, `INSERT OR REPLACE INTO runtime_records(kind,id,record) VALUES('runtime_barrier','wal','{}')`); err != nil {
		return fmt.Errorf("sync runtime control journal: %w", err)
	}
	return nil
}

func journalID(key client.ObjectKey) string { return key.Namespace + "/" + key.Name }

func clientKey(w *api.ResumablePod) client.ObjectKey { return client.ObjectKeyFromObject(w) }

func (j *Journal) Load(ctx context.Context, key client.ObjectKey) (*Record, error) {
	if j.wb != nil {
		return j.wb.load(journalID(key)), nil
	}
	var raw string
	err := j.db.QueryRowContext(ctx, `SELECT record FROM runtime_records WHERE kind=? AND id=?`, journalKind, journalID(key)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load runtime control journal: %w", err)
	}
	var record Record
	if err := json.Unmarshal([]byte(raw), &record); err != nil {
		return nil, fmt.Errorf("decode runtime control journal record: %w", err)
	}
	return &record, nil
}

func (j *Journal) Save(ctx context.Context, record *Record, expectedRevision uint64) error {
	if record == nil || record.Resource == nil {
		return errors.New("runtime control journal resource is required")
	}
	key := client.ObjectKeyFromObject(record.Resource)
	if key.Namespace == "" || key.Name == "" {
		return errors.New("runtime control journal resource namespace and name are required")
	}
	id := journalID(key)
	if j.wb != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		return j.wb.save(id, record, expectedRevision)
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin runtime control journal transaction: %w", err)
	}
	defer tx.Rollback()

	var current string
	err = tx.QueryRowContext(ctx, `SELECT record FROM runtime_records WHERE kind=? AND id=?`, journalKind, id).Scan(&current)
	if expectedRevision == 0 {
		if err == nil {
			return ErrJournalConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read runtime control journal revision: %w", err)
		}
	} else {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrJournalConflict
		}
		if err != nil {
			return fmt.Errorf("read runtime control journal revision: %w", err)
		}
		var existing Record
		if err := json.Unmarshal([]byte(current), &existing); err != nil {
			return fmt.Errorf("decode runtime control journal revision: %w", err)
		}
		if existing.Revision != expectedRevision {
			return ErrJournalConflict
		}
	}

	if record.Pending && record.Pod != nil && record.Pod.UID != "" {
		var claimed int
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(
            SELECT 1 FROM runtime_records
            WHERE kind=? AND id<>? AND json_extract(record,'$.pending')=1
              AND json_extract(record,'$.pod.metadata.namespace')=?
              AND json_extract(record,'$.pod.metadata.uid')=?
        )`, journalKind, id, record.Pod.Namespace, string(record.Pod.UID)).Scan(&claimed)
		if err != nil {
			return fmt.Errorf("check runtime control Pod claim: %w", err)
		}
		if claimed != 0 {
			return ErrPodClaimed
		}
	}

	stored := *record
	stored.Revision = expectedRevision + 1
	data, err := json.Marshal(&stored)
	if err != nil {
		return fmt.Errorf("encode runtime control journal record: %w", err)
	}
	boxID := string(stored.Resource.UID)
	if expectedRevision == 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO runtime_records(kind,id,client_id,box_id,record) VALUES(?,?, '', ?, ?)`, journalKind, id, boxID, string(data))
	} else {
		result, updateErr := tx.ExecContext(ctx, `UPDATE runtime_records SET client_id='', box_id=?, record=? WHERE kind=? AND id=?`, boxID, string(data), journalKind, id)
		err = updateErr
		if err == nil {
			var count int64
			count, err = result.RowsAffected()
			if err == nil && count != 1 {
				err = ErrJournalConflict
			}
		}
	}
	if err != nil {
		if errors.Is(err, ErrJournalConflict) {
			return err
		}
		return fmt.Errorf("write runtime control journal record: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit runtime control journal record: %w", err)
	}
	record.Revision = stored.Revision
	return nil
}

func (j *Journal) Delete(ctx context.Context, key client.ObjectKey, expectedRevision uint64) error {
	if j.wb != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		return j.wb.delete(journalID(key), expectedRevision)
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin runtime control journal transaction: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `DELETE FROM runtime_records WHERE kind=? AND id=? AND json_extract(record,'$.revision')=?`, journalKind, journalID(key), expectedRevision)
	if err != nil {
		return fmt.Errorf("delete runtime control journal record: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("check deleted runtime control journal record: %w", err)
	}
	if n != 1 {
		return ErrJournalConflict
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit runtime control journal deletion: %w", err)
	}
	return nil
}

func (j *Journal) ListPending(ctx context.Context) ([]Record, error) {
	return j.list(ctx, true)
}

func (j *Journal) list(ctx context.Context, pendingOnly bool) ([]Record, error) {
	if j.wb != nil {
		return j.wb.list(pendingOnly), nil
	}
	query := `SELECT record FROM runtime_records WHERE kind=?`
	if pendingOnly {
		query += ` AND json_extract(record,'$.pending')=1`
	}
	rows, err := j.db.QueryContext(ctx, query+` ORDER BY id`, journalKind)
	if err != nil {
		return nil, fmt.Errorf("list pending runtime control records: %w", err)
	}
	defer rows.Close()
	var records []Record
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("read pending runtime control record: %w", err)
		}
		var record Record
		if err := json.Unmarshal([]byte(raw), &record); err != nil {
			return nil, fmt.Errorf("decode pending runtime control record: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending runtime control records: %w", err)
	}
	return records, nil
}
