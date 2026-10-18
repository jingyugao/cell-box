package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/image"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$`)
var versionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$`)

// Register installs only local management routes. Runtime operations remain with
// the service; it dispatches build-* GET operation requests to ServeOperation.
func (m *Manager) Register(mux *http.ServeMux, client func(*http.Request) string) {
	m.clientOnce.Do(func() { m.client = client })
	mux.HandleFunc("POST /v1/templates", m.respond(m.createTemplate))
	mux.HandleFunc("GET /v1/templates", m.respond(m.listTemplates))
	mux.HandleFunc("GET /v1/templates/{id}", m.respond(m.readTemplate))
	mux.HandleFunc("PATCH /v1/templates/{id}", m.respond(m.updateTemplate))
	mux.HandleFunc("DELETE /v1/templates/{id}", m.respond(m.deleteTemplate))
	mux.HandleFunc("POST /v1/templates/{id}/revisions", m.respond(m.submitBuild))
	mux.HandleFunc("GET /v1/templates/{id}/revisions", m.respond(m.listRevisions))
	mux.HandleFunc("GET /v1/template-revisions/{id}", m.respond(m.readRevision))
	mux.HandleFunc("PATCH /v1/template-revisions/{id}", m.respond(m.updateRevision))
	mux.HandleFunc("GET /v1/operations", m.respond(m.listOperations))
	mux.HandleFunc("GET /v1/operations/{id}/logs", m.respond(m.logs))
	mux.HandleFunc("POST /v1/operations/{action}", m.respond(m.cancelOperation))
}

type endpoint func(*http.Request) (any, int, error)

func (m *Manager) respond(f endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		m.mu.Lock()
		unavailable := m.closed || m.fault != nil
		m.mu.Unlock()
		var out any
		var status int
		var err error
		if unavailable {
			err = failure("UNAVAILABLE", "Local catalog is unavailable")
		} else {
			out, status, err = f(r)
		}
		if err != nil {
			var e *Error
			if !errors.As(err, &e) {
				e = &Error{Code: "INTERNAL", Message: "Local catalog operation failed"}
			}
			switch e.Code {
			case "NOT_FOUND":
				status = 404
			case "INVALID_REQUEST":
				status = 400
			case "CONFLICT":
				status = 409
			case "UNSUPPORTED_CAPABILITY":
				status = 422
			case "UNAVAILABLE":
				status = 503
			default:
				status = 500
			}
			out = map[string]any{"error": e}
		}
		if status == 0 {
			status = 200
		}
		if status == 204 {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(out)
	}
}
func decode(r *http.Request, v any) error {
	d := json.NewDecoder(io.LimitReader(r.Body, (1<<20)+1))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return failure("INVALID_REQUEST", "Invalid JSON request")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return failure("INVALID_REQUEST", "Expected one JSON object")
	}
	return nil
}
func slots(n int) bool { return n >= 0 && n <= 1024 }
func page(r *http.Request) (int, int, error) {
	limit, offset := 100, 0
	for k, p := range map[string]*int{"limit": &limit, "offset": &offset} {
		if raw := r.URL.Query().Get(k); raw != "" {
			v, e := strconv.Atoi(raw)
			if e != nil || v < 0 {
				return 0, 0, failure("INVALID_REQUEST", "Invalid pagination")
			}
			*p = v
		}
	}
	if limit < 1 || limit > 500 {
		return 0, 0, failure("INVALID_REQUEST", "limit must be 1..500")
	}
	return limit, offset, nil
}
func scanRecords[T any](rows *sql.Rows, err error) ([]T, error) {
	out := []T{}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		var v T
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(raw), &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (m *Manager) createTemplate(r *http.Request) (any, int, error) {
	var in struct {
		Name             string `json:"name"`
		Description      string `json:"description"`
		DefaultWarmSlots int    `json:"defaultWarmSlots"`
	}
	if err := decode(r, &in); err != nil {
		return nil, 0, err
	}
	if !namePattern.MatchString(in.Name) || len(in.Description) > 8192 || !slots(in.DefaultWarmSlots) {
		return nil, 0, failure("INVALID_REQUEST", "Invalid name, description or defaultWarmSlots")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	client := m.client(r)
	var count int
	if err := m.db.QueryRowContext(r.Context(), "SELECT count(*) FROM templates WHERE client_id=? AND name=?", client, in.Name).Scan(&count); err != nil {
		return nil, 0, err
	}
	if count != 0 {
		return nil, 0, failure("CONFLICT", "Template name already exists")
	}
	now := time.Now().UTC()
	t := Template{ID: newID("tpl-"), Name: in.Name, Description: in.Description, DefaultWarmSlots: in.DefaultWarmSlots, Generation: 1, CreatedAt: now, UpdatedAt: now}
	_, err := m.db.ExecContext(r.Context(), "INSERT INTO templates(id,client_id,name,record) VALUES(?,?,?,?)", t.ID, client, t.Name, encode(t))
	return t, 201, err
}
func (m *Manager) readTemplate(r *http.Request) (any, int, error) {
	v, e := getTemplate(r.Context(), m.db, m.client(r), r.PathValue("id"))
	return v, 200, e
}
func (m *Manager) listTemplates(r *http.Request) (any, int, error) {
	limit, offset, err := page(r)
	if err != nil {
		return nil, 0, err
	}
	query := "SELECT record FROM templates WHERE client_id=?"
	args := []any{m.client(r)}
	if name := r.URL.Query().Get("name"); name != "" {
		query += " AND name=?"
		args = append(args, name)
	}
	query += " ORDER BY name,id LIMIT ? OFFSET ?"
	args = append(args, limit, offset)
	out, err := scanRecords[Template](m.db.QueryContext(r.Context(), query, args...))
	return out, 200, err
}
func (m *Manager) updateTemplate(r *http.Request) (any, int, error) {
	var in struct {
		Description        *string `json:"description"`
		DefaultWarmSlots   *int    `json:"defaultWarmSlots"`
		StableRevisionID   *string `json:"stableRevisionId"`
		ExpectedGeneration uint64  `json:"expectedGeneration"`
	}
	if err := decode(r, &in); err != nil {
		return nil, 0, err
	}
	if in.ExpectedGeneration == 0 || (in.Description != nil && len(*in.Description) > 8192) || (in.DefaultWarmSlots != nil && !slots(*in.DefaultWarmSlots)) {
		return nil, 0, failure("INVALID_REQUEST", "Valid expectedGeneration and settings are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	client := m.client(r)
	t, err := getTemplate(r.Context(), m.db, client, r.PathValue("id"))
	if err != nil {
		return nil, 0, err
	}
	if t.Generation != in.ExpectedGeneration {
		return nil, 0, failure("CONFLICT", "Template generation changed")
	}
	if in.StableRevisionID != nil && *in.StableRevisionID != "" {
		v, _, err := getRevision(r.Context(), m.db, client, *in.StableRevisionID)
		if err != nil {
			return nil, 0, err
		}
		if v.TemplateID != t.ID || v.Status != "active" {
			return nil, 0, failure("CONFLICT", "Stable must select an active revision of this template")
		}
	}
	if in.Description != nil {
		t.Description = *in.Description
	}
	if in.DefaultWarmSlots != nil {
		t.DefaultWarmSlots = *in.DefaultWarmSlots
	}
	if in.StableRevisionID != nil {
		t.StableRevisionID = *in.StableRevisionID
	}
	t.Generation++
	t.UpdatedAt = time.Now().UTC()
	_, err = m.db.ExecContext(r.Context(), "UPDATE templates SET record=? WHERE id=?", encode(t), t.ID)
	return t, 200, err
}
func (m *Manager) deleteTemplate(r *http.Request) (any, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	client, id := m.client(r), r.PathValue("id")
	if _, err := getTemplate(r.Context(), m.db, client, id); err != nil {
		return nil, 0, err
	}
	var count int
	if err := m.db.QueryRowContext(r.Context(), "SELECT (SELECT count(*) FROM template_revisions WHERE template_id=?) + (SELECT count(*) FROM operations WHERE template_id=? AND status IN ('queued','running','cancelling'))", id, id).Scan(&count); err != nil {
		return nil, 0, err
	}
	if count != 0 {
		return nil, 0, failure("CONFLICT", "Template still has revisions or pending builds")
	}
	_, err := m.db.ExecContext(r.Context(), "DELETE FROM templates WHERE id=?", id)
	return nil, 204, err
}
func (m *Manager) readRevision(r *http.Request) (any, int, error) {
	v, _, e := getRevision(r.Context(), m.db, m.client(r), r.PathValue("id"))
	return v, 200, e
}
func (m *Manager) listRevisions(r *http.Request) (any, int, error) {
	limit, offset, err := page(r)
	if err != nil {
		return nil, 0, err
	}
	client, id := m.client(r), r.PathValue("id")
	if _, err = getTemplate(r.Context(), m.db, client, id); err != nil {
		return nil, 0, err
	}
	q := "SELECT record FROM template_revisions WHERE client_id=? AND template_id=?"
	args := []any{client, id}
	if v := r.URL.Query().Get("userVersion"); v != "" {
		q += " AND user_version=?"
		args = append(args, v)
	}
	q += " ORDER BY user_version,revision DESC,id LIMIT ? OFFSET ?"
	args = append(args, limit, offset)
	out, err := scanRecords[Revision](m.db.QueryContext(r.Context(), q, args...))
	return out, 200, err
}
func (m *Manager) updateRevision(r *http.Request) (any, int, error) {
	var in struct {
		Description        *string `json:"description"`
		Status             *string `json:"status"`
		WarmSlots          *int    `json:"warmSlots"`
		ExpectedGeneration uint64  `json:"expectedGeneration"`
	}
	if err := decode(r, &in); err != nil {
		return nil, 0, err
	}
	if in.ExpectedGeneration == 0 || (in.WarmSlots != nil && !slots(*in.WarmSlots)) || (in.Description != nil && len(*in.Description) > 8192) || (in.Status != nil && *in.Status != "active" && *in.Status != "retired") {
		return nil, 0, failure("INVALID_REQUEST", "Invalid revision settings or expectedGeneration")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	client := m.client(r)
	v, _, err := getRevision(r.Context(), m.db, client, r.PathValue("id"))
	if err != nil {
		return nil, 0, err
	}
	if v.Generation != in.ExpectedGeneration {
		return nil, 0, failure("CONFLICT", "Revision generation changed")
	}
	if in.Status != nil && *in.Status == "retired" {
		t, e := getTemplate(r.Context(), m.db, client, v.TemplateID)
		if e != nil {
			return nil, 0, e
		}
		if t.StableRevisionID == v.ID {
			return nil, 0, failure("CONFLICT", "Move or clear stable before retiring this revision")
		}
	}
	if in.Description != nil {
		v.Description = *in.Description
	}
	if in.Status != nil {
		v.Status = *in.Status
	}
	if in.WarmSlots != nil {
		v.WarmSlots = *in.WarmSlots
	}
	v.Generation++
	v.UpdatedAt = time.Now().UTC()
	_, err = m.db.ExecContext(r.Context(), "UPDATE template_revisions SET record=? WHERE id=?", encode(v), v.ID)
	return v, 200, err
}

func (m *Manager) submitBuild(r *http.Request) (any, int, error) {
	var in BuildRequest
	if err := decode(r, &in); err != nil {
		return nil, 0, err
	}
	if !versionPattern.MatchString(in.UserVersion) || in.ProfileID == "" || len(in.BaseImage) > 2048 || len(in.RunCommand) > 65536 || (in.WarmSlots != nil && !slots(*in.WarmSlots)) {
		return nil, 0, failure("INVALID_REQUEST", "Valid userVersion, baseImage and profileId are required")
	}
	if _, err := image.ParseRegistryReference(in.BaseImage, m.config.InsecureRegistry); err != nil {
		return nil, 0, failure("INVALID_REQUEST", err.Error())
	}
	if err := image.ValidateBuildCommand(in.BuildCommand); err != nil {
		return nil, 0, failure("INVALID_REQUEST", err.Error())
	}
	if _, err := image.ParseRunCommand(in.RunCommand); err != nil {
		return nil, 0, failure("INVALID_REQUEST", err.Error())
	}
	if in.Platform == "" {
		in.Platform = "linux/amd64"
	}
	if in.Platform != "linux/amd64" {
		return nil, 0, failure("INVALID_REQUEST", "Only linux/amd64 builds are supported")
	}
	client, templateID := m.client(r), r.PathValue("id")
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.fault != nil {
		return nil, 0, failure("UNAVAILABLE", "Catalog is stopping")
	}
	t, err := getTemplate(r.Context(), m.db, client, templateID)
	if err != nil {
		return nil, 0, err
	}
	if m.config.ResolveProfile == nil {
		return nil, 0, failure("UNSUPPORTED_CAPABILITY", "Profile resolution is not configured")
	}
	if b, ok := m.config.Builder.(*imageBuilder); ok && b.options.Address == "" {
		return nil, 0, failure("UNSUPPORTED_CAPABILITY", "Image building is not configured")
	}
	profile, err := m.config.ResolveProfile(client, in.ProfileID)
	if err != nil {
		return nil, 0, err
	}
	if !json.Valid(profile.Snapshot) {
		return nil, 0, failure("INVALID_REQUEST", "Invalid resolved profile")
	}
	if in.WarmSlots == nil {
		n := t.DefaultWarmSlots
		in.WarmSlots = &n
	}
	now := time.Now().UTC()
	op := Operation{ID: newID("build-"), Kind: "template-build", TargetID: t.ID, Status: "queued", Stage: "queued", Version: 1, Input: in, CreatedAt: now, UpdatedAt: now}
	input := BuildInput{BuildRequest: in, Profile: profile, TemplateID: t.ID, OperationID: op.ID}
	_, err = m.db.ExecContext(r.Context(), "INSERT INTO operations(id,client_id,template_id,status,created_at,record,input) VALUES(?,?,?,?,?,?,?)", op.ID, client, t.ID, op.Status, now.Format(time.RFC3339Nano), encode(op), encode(input))
	if err == nil {
		select {
		case m.wake <- struct{}{}:
		default:
		}
	}
	return op, 202, err
}

func (m *Manager) ServeOperation(w http.ResponseWriter, r *http.Request) {
	m.respond(m.readOperation)(w, r)
}
func (m *Manager) readOperation(r *http.Request) (any, int, error) {
	// Match the existing SDK's bounded long-poll option using only SQLite.
	wait := 0
	if raw := r.URL.Query().Get("waitMs"); raw != "" {
		v, e := strconv.Atoi(raw)
		if e != nil || v < 0 || v > 10000 {
			return nil, 0, failure("INVALID_REQUEST", "waitMs must be 0..10000")
		}
		wait = v
	}
	deadline := time.Now().Add(time.Duration(wait) * time.Millisecond)
	for {
		op, _, err := getOperation(r.Context(), m.db, m.client(r), r.PathValue("id"))
		if err != nil || terminal(op.Status) || !time.Now().Before(deadline) {
			return op, 200, err
		}
		timer := time.NewTimer(min(50*time.Millisecond, time.Until(deadline)))
		select {
		case <-r.Context().Done():
			timer.Stop()
			return nil, 0, r.Context().Err()
		case <-timer.C:
		}
	}
}
func (m *Manager) listOperations(r *http.Request) (any, int, error) {
	limit, offset, err := page(r)
	if err != nil {
		return nil, 0, err
	}
	q := "SELECT record FROM operations WHERE client_id=?"
	args := []any{m.client(r)}
	for _, pair := range [][2]string{{"templateId", "template_id"}, {"status", "status"}} {
		if v := r.URL.Query().Get(pair[0]); v != "" {
			q += " AND " + pair[1] + "=?"
			args = append(args, v)
		}
	}
	q += " ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?"
	args = append(args, limit, offset)
	out, err := scanRecords[Operation](m.db.QueryContext(r.Context(), q, args...))
	return out, 200, err
}
func (m *Manager) cancelOperation(r *http.Request) (any, int, error) {
	id, action, ok := strings.Cut(r.PathValue("action"), ":")
	if !ok || action != "cancel" {
		return nil, 0, failure("NOT_FOUND", "Endpoint not found")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	op, _, err := getOperation(r.Context(), m.db, m.client(r), id)
	if err != nil {
		return nil, 0, err
	}
	if terminal(op.Status) {
		return op, 200, nil
	}
	now := time.Now().UTC()
	op.Version++
	op.UpdatedAt = now
	if op.Status == "queued" {
		op.Status = "cancelled"
		op.Stage = "cancelled"
		op.FinishedAt = &now
	} else {
		op.Status = "cancelling"
	}
	_, err = m.db.ExecContext(r.Context(), "UPDATE operations SET status=?,record=? WHERE id=?", op.Status, encode(op), id)
	if err == nil {
		if stop := m.active[id]; stop != nil {
			stop()
		}
	}
	return op, 200, err
}
func (m *Manager) logs(r *http.Request) (any, int, error) {
	op, _, err := getOperation(r.Context(), m.db, m.client(r), r.PathValue("id"))
	if err != nil {
		return nil, 0, err
	}
	offset := int64(0)
	limit := 64 << 10
	if raw := r.URL.Query().Get("offset"); raw != "" {
		offset, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || offset < 0 {
			return nil, 0, failure("INVALID_REQUEST", "Invalid log offset")
		}
	}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 1<<20 {
			return nil, 0, failure("INVALID_REQUEST", "Log limit must be 1..1048576")
		}
	}
	out := Logs{NextOffset: offset, EOF: terminal(op.Status)}
	f, err := os.Open(m.logPath(op.ID))
	if errors.Is(err, os.ErrNotExist) {
		if offset != 0 {
			return nil, 0, failure("INVALID_REQUEST", "Log offset exceeds size")
		}
		return out, 200, nil
	}
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	if offset > st.Size() {
		return nil, 0, failure("INVALID_REQUEST", "Log offset exceeds size")
	}
	b := make([]byte, min(int64(limit), st.Size()-offset))
	n, err := f.ReadAt(b, offset)
	if err != nil && err != io.EOF {
		return nil, 0, err
	}
	out.Text = string(b[:n])
	out.NextOffset = offset + int64(n)
	out.EOF = terminal(op.Status) && out.NextOffset >= st.Size()
	return out, 200, nil
}

// Operation returns local build metadata without observing its runtime target.
func (m *Manager) Operation(ctx context.Context, client, id string) (Operation, error) {
	op, _, err := getOperation(ctx, m.db, client, id)
	return op, err
}
