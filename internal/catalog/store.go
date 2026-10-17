package catalog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

type Manager struct {
	db         *sql.DB
	config     Config
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.Mutex // All local mutations and work admission are serialized.
	closed     bool
	fault      error
	active     map[string]context.CancelFunc
	wake       chan struct{}
	client     func(*http.Request) string
	clientOnce sync.Once
	images     *semaphore.Weighted // Serialize image publication against legacy physical deletion.
	lock       *os.File
}

const schema = `
CREATE TABLE templates (
 id TEXT PRIMARY KEY, client_id TEXT NOT NULL, name TEXT NOT NULL,
 record TEXT NOT NULL, UNIQUE(client_id,name)
);
CREATE TABLE template_revisions (
 id TEXT PRIMARY KEY, client_id TEXT NOT NULL, template_id TEXT NOT NULL REFERENCES templates(id),
 user_version TEXT NOT NULL, revision INTEGER NOT NULL,
 record TEXT NOT NULL, profile TEXT NOT NULL,
 UNIQUE(template_id,user_version,revision)
);
CREATE INDEX revision_owner ON template_revisions(client_id,template_id);
CREATE TABLE operations (
 id TEXT PRIMARY KEY, client_id TEXT NOT NULL, template_id TEXT NOT NULL,
 idempotency_key TEXT NOT NULL, request_hash TEXT NOT NULL,
 status TEXT NOT NULL, created_at TEXT NOT NULL, record TEXT NOT NULL, input TEXT NOT NULL,
 UNIQUE(client_id,template_id,idempotency_key)
);
CREATE INDEX operation_owner ON operations(client_id,template_id,created_at);
PRAGMA user_version=1;
`

func Open(config Config) (*Manager, error) {
	if config.Dir == "" {
		return nil, errors.New("catalog directory is required")
	}
	if err := os.MkdirAll(filepath.Join(config.Dir, "logs"), 0700); err != nil {
		return nil, err
	}
	fd, err := unix.Open(filepath.Join(config.Dir, "catalog.lock"), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), "catalog.lock")
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("catalog already open: %w", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(config.Dir, "catalog.db"))
	if err != nil {
		lock.Close()
		return nil, err
	}
	db.SetMaxOpenConns(1)
	cleanup := func(err error) (*Manager, error) { db.Close(); lock.Close(); return nil, err }
	for _, pragma := range []string{"PRAGMA busy_timeout=5000", "PRAGMA foreign_keys=ON", "PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL"} {
		if _, err = db.Exec(pragma); err != nil {
			return cleanup(err)
		}
	}
	if err = os.Chmod(filepath.Join(config.Dir, "catalog.db"), 0600); err != nil {
		return cleanup(err)
	}
	var version int
	if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return cleanup(err)
	}
	if version == 0 {
		tx, e := db.Begin()
		if e != nil {
			return cleanup(e)
		}
		if _, e = tx.Exec(schema); e != nil {
			tx.Rollback()
			return cleanup(e)
		}
		if e = tx.Commit(); e != nil {
			return cleanup(e)
		}
	} else if version != 1 {
		return cleanup(fmt.Errorf("unsupported catalog schema %d", version))
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{db: db, config: config, ctx: ctx, cancel: cancel, lock: lock, active: map[string]context.CancelFunc{}, wake: make(chan struct{}, 1), images: semaphore.NewWeighted(1)}
	if config.Builder == nil {
		m.config.Builder = &imageBuilder{options: config.ImageBuild, insecure: config.InsecureRegistry}
	}
	if err = m.interruptPending(); err != nil {
		cancel()
		return cleanup(err)
	}
	m.wg.Add(1)
	go func() { defer m.wg.Done(); m.worker() }()
	return m, nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.cancel()
	for _, cancel := range m.active {
		cancel()
	}
	m.mu.Unlock()
	m.wg.Wait()
	err := m.db.Close()
	return errors.Join(err, m.lock.Close())
}

func newID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b[:])
}
func encode(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}
func missing(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return failure("NOT_FOUND", "Resource not found")
	}
	return err
}
func terminal(status string) bool {
	return status == "succeeded" || status == "failed" || status == "cancelled"
}

type querier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getTemplate(ctx context.Context, db querier, client, id string) (Template, error) {
	var v Template
	var raw string
	err := db.QueryRowContext(ctx, "SELECT record FROM templates WHERE client_id=? AND id=?", client, id).Scan(&raw)
	if err != nil {
		return v, missing(err)
	}
	err = json.Unmarshal([]byte(raw), &v)
	return v, err
}
func getRevision(ctx context.Context, db querier, client, id string) (Revision, json.RawMessage, error) {
	var v Revision
	var raw, profile string
	err := db.QueryRowContext(ctx, "SELECT record,profile FROM template_revisions WHERE client_id=? AND id=?", client, id).Scan(&raw, &profile)
	if err != nil {
		return v, nil, missing(err)
	}
	err = json.Unmarshal([]byte(raw), &v)
	return v, json.RawMessage(profile), err
}
func getOperation(ctx context.Context, db querier, client, id string) (Operation, BuildInput, error) {
	var v Operation
	var input BuildInput
	var raw, in string
	err := db.QueryRowContext(ctx, "SELECT record,input FROM operations WHERE client_id=? AND id=?", client, id).Scan(&raw, &in)
	if err != nil {
		return v, input, missing(err)
	}
	if err = json.Unmarshal([]byte(raw), &v); err != nil {
		return v, input, err
	}
	err = json.Unmarshal([]byte(in), &input)
	return v, input, err
}

func (m *Manager) RevisionSnapshot(ctx context.Context, client, id string) (Revision, json.RawMessage, error) {
	return getRevision(ctx, m.db, client, id)
}

// ImageReferenced prevents legacy image cleanup from deleting a registry
// artifact that the local catalog owns. Pending builds conservatively protect
// images while a mutable source tag is being resolved. Never expose this
// cross-client registry ownership check as a public usage listing.
func (m *Manager) ImageReferenced(ctx context.Context, imageRef string) (bool, error) {
	var count int
	err := m.db.QueryRowContext(ctx, `SELECT
	 (SELECT count(*) FROM template_revisions
	  WHERE json_extract(record,'$.image')=? OR json_extract(record,'$.resolvedBaseImage')=?) +
	 (SELECT count(*) FROM operations WHERE status IN ('queued','running','cancelling'))`, imageRef, imageRef).Scan(&count)
	return count > 0, err
}

// DeleteImage fences physical registry cleanup against a build publishing and
// registering the same content digest. It is used only by the legacy image API.
func (m *Manager) DeleteImage(ctx context.Context, imageRef string, remove func() error) error {
	if err := m.images.Acquire(ctx, 1); err != nil {
		return err
	}
	defer m.images.Release(1)
	used, err := m.ImageReferenced(ctx, imageRef)
	if err != nil {
		return err
	}
	if used {
		return failure("CONFLICT", "Image is referenced by the template catalog or a pending build")
	}
	return remove()
}

func (m *Manager) Resolve(ctx context.Context, client, templateID, revisionID string) (Revision, json.RawMessage, error) {
	if (templateID == "") == (revisionID == "") {
		return Revision{}, nil, failure("INVALID_REQUEST", "Specify exactly one of templateId or revisionId")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if templateID != "" {
		t, err := getTemplate(ctx, m.db, client, templateID)
		if err != nil {
			return Revision{}, nil, err
		}
		revisionID = t.StableRevisionID
		if revisionID == "" {
			return Revision{}, nil, failure("CONFLICT", "Template has no stable revision")
		}
	}
	r, profile, err := getRevision(ctx, m.db, client, revisionID)
	if err == nil && r.Status != "active" {
		err = failure("CONFLICT", "Revision is retired")
	}
	return r, profile, err
}

func (m *Manager) interruptPending() error {
	rows, err := m.db.Query("SELECT id,record FROM operations WHERE status IN ('queued','running','cancelling')")
	if err != nil {
		return err
	}
	var ops []Operation
	for rows.Next() {
		var id, raw string
		if err = rows.Scan(&id, &raw); err != nil {
			rows.Close()
			return err
		}
		var op Operation
		if err = json.Unmarshal([]byte(raw), &op); err != nil {
			rows.Close()
			return err
		}
		ops = append(ops, op)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, op := range ops {
		now := time.Now().UTC()
		op.Status = "failed"
		op.Stage = "interrupted"
		op.Version++
		op.UpdatedAt = now
		op.FinishedAt = &now
		op.Error = &Error{Code: "OPERATION_INTERRUPTED", Message: "Service stopped before this build completed; inspect its log and submit a new request to rebuild"}
		if _, err = m.db.Exec("UPDATE operations SET status=?,record=? WHERE id=?", op.Status, encode(op), op.ID); err != nil {
			return err
		}
	}
	return nil
}
