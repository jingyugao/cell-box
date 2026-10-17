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
	rows, err := m.db.QueryContext(ctx, "SELECT client_id,record,profile FROM template_revisions ORDER BY id")
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
