package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/inventory"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

func sortInventory(boxes []Box) {
	sort.Slice(boxes, func(i, j int) bool {
		if boxes[i].CreatedAt.Equal(boxes[j].CreatedAt) {
			return boxes[i].ID < boxes[j].ID
		}
		return boxes[i].CreatedAt.Before(boxes[j].CreatedAt)
	})
}

type boxListOptions struct {
	ids        []string
	templateID string
	revisionID string
	phase      string
	limit      int
	offset     int
}

func parseBoxListOptions(r *http.Request) (boxListOptions, error) {
	q := r.URL.Query()
	var out boxListOptions
	out.ids = q["id"]
	for _, key := range []string{"templateId", "revisionId", "phase", "limit", "offset"} {
		if len(q[key]) > 1 {
			return out, apiError("INVALID_REQUEST", key+" may be supplied only once")
		}
	}
	out.templateID = q.Get("templateId")
	out.revisionID = q.Get("revisionId")
	out.phase = q.Get("phase")
	if _, ok := q["templateId"]; ok && out.templateID == "" {
		return out, apiError("INVALID_REQUEST", "templateId must not be empty")
	}
	if _, ok := q["revisionId"]; ok && out.revisionID == "" {
		return out, apiError("INVALID_REQUEST", "revisionId must not be empty")
	}
	if _, ok := q["phase"]; ok && out.phase == "" {
		return out, apiError("INVALID_REQUEST", "phase must not be empty")
	}
	if value := q.Get("limit"); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 1 || limit > 500 {
			return out, apiError("INVALID_REQUEST", "limit must be 1..500")
		}
		out.limit = limit
	} else if _, ok := q["limit"]; ok {
		return out, apiError("INVALID_REQUEST", "limit must be 1..500")
	}
	if value := q.Get("offset"); value != "" {
		offset, err := strconv.Atoi(value)
		if err != nil || offset < 0 {
			return out, apiError("INVALID_REQUEST", "offset must be >= 0")
		}
		out.offset = offset
	} else if _, ok := q["offset"]; ok {
		return out, apiError("INVALID_REQUEST", "offset must be >= 0")
	}
	return out, nil
}

func (o boxListOptions) apply(boxes []Box) []Box {
	filtered := boxes[:0]
	for _, box := range boxes {
		if o.templateID != "" && box.TemplateID != o.templateID ||
			o.revisionID != "" && box.RevisionID != o.revisionID ||
			o.phase != "" && box.Phase != o.phase {
			continue
		}
		filtered = append(filtered, box)
	}
	boxes = filtered
	sortInventory(boxes)
	if o.offset >= len(boxes) {
		return []Box{}
	}
	boxes = boxes[o.offset:]
	if o.limit > 0 && len(boxes) > o.limit {
		boxes = boxes[:o.limit]
	}
	return boxes
}

func (s *Service) listBoxes(w http.ResponseWriter, r *http.Request) {
	observation := r.URL.Query().Get("observation")
	if observation != "" && observation != "resource" {
		fail(w, apiError("INVALID_REQUEST", "observation must be resource when supplied"))
		return
	}
	resourcesOnly := observation == "resource"
	options, err := parseBoxListOptions(r)
	if err != nil {
		fail(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	namespaces := map[string]boxprovider.InventoryProvider{}
	for _, profile := range s.config.Profiles {
		provider, ok := s.providers[profile.Provider].(boxprovider.InventoryProvider)
		if !ok {
			if resourcesOnly {
				fail(w, apiError("UNSUPPORTED_CAPABILITY", "Provider does not support resource inventory"))
				return
			}
			s.listLocalBoxes(w, r)
			return
		}
		if resourcesOnly {
			if _, ok := provider.(boxprovider.ResourceInventoryProvider); !ok {
				fail(w, apiError("UNSUPPORTED_CAPABILITY", "Provider does not support resource inventory"))
				return
			}
		}
		namespaces[profile.Provider+"/"+profile.Namespace] = provider
	}
	wanted := map[string]bool{}
	for _, id := range options.ids {
		wanted[id] = true
	}
	filtered := len(wanted) > 0
	boxes := []Box{}
	for key, provider := range namespaces {
		namespace := strings.SplitN(key, "/", 2)[1]
		var rows []inventory.Record
		var err error
		if resourcesOnly {
			rows, err = provider.(boxprovider.ResourceInventoryProvider).ListResources(ctx, namespace, clientID(r))
		} else {
			rows, err = provider.List(ctx, namespace, clientID(r))
		}
		if err != nil {
			fail(w, err)
			return
		}
		for _, row := range rows {
			var box Box
			if json.Unmarshal(row.Box, &box) != nil || box.ID == "" {
				fail(w, errors.New("invalid runtime inventory box"))
				return
			}
			if !filtered || wanted[box.ID] {
				boxes = append(boxes, box)
				delete(wanted, box.ID)
			}
		}
	}
	// Preserve explicit ID lookup authorization semantics, without consulting state.
	if len(wanted) > 0 {
		fail(w, apiError("NOT_FOUND", "Box not found"))
		return
	}
	writeJSON(w, 200, options.apply(boxes))
}
func (s *Service) listCheckpoints(w http.ResponseWriter, r *http.Request) {
	records, err := s.store.ListCheckpoints(r.Context(), clientID(r))
	if err != nil {
		fail(w, err)
		return
	}
	boxes := []Box{}
	for _, record := range records {
		var box Box
		if json.Unmarshal(record.Box, &box) != nil || box.ID == "" {
			fail(w, errors.New("invalid checkpoint box metadata"))
			return
		}
		boxes = append(boxes, box)
	}
	sortInventory(boxes)
	writeJSON(w, 200, boxes)
}

func (s *Store) PutCheckpoint(ctx context.Context, record inventory.Record) error {
	if record.RuntimeID == "" || record.Snapshot == "" || record.ClientID == "" {
		return errors.New("invalid checkpoint record")
	}
	var box Box
	if err := json.Unmarshal(record.Box, &box); err != nil || box.ID == "" {
		return errors.New("invalid checkpoint box")
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	// Reconciliation publishes the index again from the custom resource, so a
	// record lost on power failure comes back. Deletions stay synchronous: a
	// running Box does not remove a stale index entry.
	_, err = s.relaxed.ExecContext(ctx, `INSERT INTO runtime_records(kind,id,client_id,box_id,record) VALUES('checkpoints',?,?,?,?)
 ON CONFLICT(kind,id) DO UPDATE SET client_id=excluded.client_id,box_id=excluded.box_id,record=excluded.record WHERE record!=excluded.record`, record.RuntimeID, record.ClientID, box.ID, string(data))
	return err
}
func (s *Store) GetCheckpoint(ctx context.Context, id string) (inventory.Record, error) {
	record, err := readRecord[inventory.Record](ctx, s.db, "checkpoints", id)
	if errors.Is(err, sql.ErrNoRows) {
		err = inventory.ErrNotFound
	}
	return record, err
}
func (s *Store) DeleteCheckpoint(ctx context.Context, id, snapshot string) error {
	return s.deleteCheckpoint(ctx, s.db, id, snapshot)
}

// InvalidateCheckpoint removes a checkpoint whose consumed marker is on disk.
// That marker refuses a replay even if power loss brings this record back.
func (s *Store) InvalidateCheckpoint(ctx context.Context, id, snapshot string) error {
	return s.deleteCheckpoint(ctx, s.relaxed, id, snapshot)
}

func (s *Store) deleteCheckpoint(ctx context.Context, db *sql.DB, id, snapshot string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	record, err := readRecord[inventory.Record](ctx, tx, "checkpoints", id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if snapshot != "" && record.Snapshot != snapshot {
		return errors.New("checkpoint snapshot changed")
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM runtime_records WHERE kind='checkpoints' AND id=?", id); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) ListCheckpoints(ctx context.Context, client string) ([]inventory.Record, error) {
	return listRecords[inventory.Record](ctx, s.db, "checkpoints", "AND client_id=?", client)
}
