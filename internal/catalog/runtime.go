package catalog

import (
	"context"
	"encoding/json"
)

// RevisionRecord is an internal snapshot for background runtime reconciliation.
// ClientID is never exposed through the public catalog API.
type RevisionRecord struct {
	ClientID string
	Revision Revision
	Profile  json.RawMessage
}

// RuntimeRevisions reads all revisions in one SQLite snapshot. Retired and
// zero-slot revisions are included so reconciliation can withdraw old demand.
func (m *Manager) RuntimeRevisions(ctx context.Context) ([]RevisionRecord, error) {
	return m.runtimeRevisions(ctx, "", nil)
}
func (m *Manager) TemplateRuntimeRevisions(ctx context.Context, client, id string) ([]RevisionRecord, error) {
	return m.runtimeRevisions(ctx, " WHERE client_id=? AND template_id=?", []any{client, id})
}
func (m *Manager) runtimeRevisions(ctx context.Context, where string, args []any) ([]RevisionRecord, error) {
	rows, err := m.db.QueryContext(ctx, "SELECT client_id,record,profile FROM template_revisions"+where+" ORDER BY id", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RevisionRecord{}
	for rows.Next() {
		var r RevisionRecord
		var raw, profile string
		if err := rows.Scan(&r.ClientID, &raw, &profile); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(raw), &r.Revision); err != nil {
			return nil, err
		}
		r.Profile = json.RawMessage(profile)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (m *Manager) TemplateSnapshot(ctx context.Context, client, id string) (Template, error) {
	return getTemplate(ctx, m.db, client, id)
}
