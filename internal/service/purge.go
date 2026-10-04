package service

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/objectstorage"
)

// Conditional removal plus a read-back makes an ambiguous storage response
// retryable. The durable journal stays until every known object is absent.
func deleteKnownObject(ctx context.Context, objects objectstorage.Objects, key string) error {
	body, _, etag, err := objects.Get(ctx, key)
	if errors.Is(err, objectstorage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	body.Close()
	if err := objects.DeleteIfMatch(ctx, key, etag); err != nil && !errors.Is(err, objectstorage.ErrNotFound) {
		return err
	}
	body, _, _, err = objects.Get(ctx, key)
	if errors.Is(err, objectstorage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	body.Close()
	return objectstorage.ErrConflict
}

// Retirement is monotonic. Snapshot under the writer lock, then permit
// ordinary transactions to continue while storage garbage is removed.
func (s *Store) retiredRecords() map[string]bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	retired := map[string]bool{}
	if s.records != nil {
		for key := range s.records.retired {
			retired[key] = true
		}
	}
	return retired
}

func (s *Store) cleanupRetired(ctx context.Context) error {
	for key := range s.retiredRecords() {
		if err := deleteKnownObject(ctx, s.objects, key); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) purgeBox(ctx context.Context, client, id string) error {
	s.gcMu.Lock()
	defer s.gcMu.Unlock()
	s.mu.Lock()
	err := s.store.Update(func(st *State) error {
		if pending, ok := st.Purges[id]; ok {
			if pending.ClientID != client {
				return apiError("NOT_FOUND", "Box not found")
			}
			return nil
		}
		box, exists := st.Boxes[id]
		if !exists {
			related := false
			for _, op := range st.Operations {
				if op.Operation.TargetID == id || op.Operation.Result["boxId"] == id {
					if op.ClientID != client {
						return apiError("NOT_FOUND", "Box not found")
					}
					related = true
				}
			}
			for _, execution := range st.Executions {
				if execution.BoxID == id {
					related = true
				}
			}
			if !related {
				files, err := s.captureTemps(id)
				if err != nil {
					return err
				}
				records := []string{}
				if s.store.records != nil {
					key := recordKey("boxes", id)
					if s.store.records.etags[key] != "" || s.store.records.retired[key] {
						records = append(records, key)
					}
				}
				if len(files) == 0 && len(records) == 0 {
					return nil
				}
				if st.Purges == nil {
					st.Purges = map[string]purgeRecord{}
				}
				st.Purges[id] = purgeRecord{ClientID: client, Records: records}
				return nil
			}
			box = boxRecord{ClientID: client, Box: Box{ID: id, Phase: "deleted"}}
		}
		if box.ClientID != client {
			return apiError("NOT_FOUND", "Box not found")
		}
		if box.Box.Phase != "deleted" {
			return apiError("CONFLICT", "Only a deleted box can be purged")
		}
		if err := s.streamBusy(id); err != nil {
			return err
		}
		operations := map[string]bool{}
		for oid, op := range st.Operations {
			if op.Operation.TargetID == id || op.Operation.Result["boxId"] == id {
				if op.ClientID != client {
					return apiError("CONFLICT", "Box has foreign operation references")
				}
				if op.Operation.Status == "queued" || op.Operation.Status == "running" {
					return apiError("CONFLICT", "Box has an unfinished operation")
				}
				operations[oid] = true
			}
		}
		pending := purgeRecord{ClientID: client, Records: []string{recordKey("boxes", id)}}
		// Operations and keys normally live inside the box record. Only retire
		// standalone keys that were actually published; the box fence covers all
		// grouped history, including a delayed old writer.
		retirePublished := func(key string) {
			if s.store.records != nil && (s.store.records.etags[key] != "" || s.store.records.data[key] != nil || s.store.records.retired[key]) {
				pending.Records = append(pending.Records, key)
			}
		}
		for eid, execution := range st.Executions {
			if execution.BoxID != id {
				continue
			}
			if op, exists := st.Operations[execution.OperationID]; exists {
				if op.ClientID != client || op.Operation.Status == "queued" || op.Operation.Status == "running" {
					return apiError("CONFLICT", "Box has an unfinished execution")
				}
				operations[execution.OperationID] = true
			}
			if execution.ResultObject != "" {
				pending.Objects = append(pending.Objects, execution.ResultObject)
			}
			delete(st.Executions, eid)
		}
		for oid := range operations {
			delete(st.Operations, oid)
			retirePublished(recordKey("operations", oid))
		}
		for key, fence := range st.Keys {
			if operations[fence.OperationID] {
				st.Keys[key] = keyRecord{Expired: true}
				retirePublished(recordKey("keys", key))
			}
		}
		for lid, lease := range st.Leases {
			if lease.BoxID == id {
				delete(st.Leases, lid)
			}
		}
		for rid, route := range st.Routes {
			if route.BoxID == id {
				delete(st.Routes, rid)
			}
		}
		delete(st.Boxes, id)
		if st.Purges == nil {
			st.Purges = map[string]purgeRecord{}
		}
		// Result keys from older versions contain only a digest. Expired
		// outputs cannot be assigned to a box, so include unreferenced global
		// results in this journal while holding the metadata writer lock.
		if s.objects != nil {
			referenced := map[string]bool{}
			for _, execution := range st.Executions {
				referenced[execution.ResultObject] = true
			}
			keys, err := s.objects.List(ctx, resultPrefix)
			if err != nil {
				return err
			}
			objects := map[string]bool{}
			for _, key := range pending.Objects {
				objects[key] = true
			}
			for _, key := range keys {
				if strings.HasPrefix(key, resultPrefix+"/") && !referenced[key] {
					objects[key] = true
				}
			}
			pending.Objects = pending.Objects[:0]
			for key := range objects {
				pending.Objects = append(pending.Objects, key)
			}
			sort.Strings(pending.Objects)
		}
		st.Purges[id] = pending
		return nil
	})
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return s.finishPurge(ctx, client, id)
}

// Caller holds gcMu; this is also used by periodic recovery after a crash.
func (s *Service) finishPurge(ctx context.Context, client, id string) error {
	var pending purgeRecord
	var exists bool
	if err := s.store.View(func(st State) error { pending, exists = st.Purges[id]; return nil }); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if pending.ClientID != client {
		return apiError("NOT_FOUND", "Box not found")
	}
	if s.objects != nil {
		for _, key := range pending.Objects {
			if !strings.HasPrefix(key, resultPrefix+"/") {
				return errors.New("invalid purge result reference")
			}
			if err := deleteKnownObject(ctx, s.objects, key); err != nil {
				return err
			}
		}
		retired := s.store.retiredRecords()
		for _, key := range pending.Records {
			if !retired[key] {
				return errors.New("purge metadata is not retired")
			}
			if err := deleteKnownObject(ctx, s.objects, key); err != nil {
				return err
			}
		}
	}
	if err := s.cleanupCaptureTemps(id); err != nil {
		return err
	}
	return s.store.Update(func(st *State) error { delete(st.Purges, id); return nil })
}

func (s *Service) recoverPurges(ctx context.Context) error {
	var pending map[string]purgeRecord
	if err := s.store.View(func(st State) error { pending = st.Purges; return nil }); err != nil {
		return err
	}
	ids := make([]string, 0, len(pending))
	for id := range pending {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := s.finishPurge(ctx, pending[id].ClientID, id); err != nil {
			return err
		}
	}
	return s.store.cleanupRetired(ctx)
}

func (s *Service) purgeBoxHandler(w http.ResponseWriter, r *http.Request, id string) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := s.purgeBox(ctx, clientID(r), id); err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}
