package service

import (
	"context"
	"errors"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/objectstorage"
)

// Expiry follows completion, not submission: a long-running or unknown
// operation is never silently forgotten while its outcome is unresolved.
func pruneState(st *State, now time.Time, operations, outputs time.Duration) []string {
	garbage := []string{}
	for id, execution := range st.Executions {
		op, exists := st.Operations[execution.OperationID]
		if !exists || op.Operation.FinishedAt == nil || op.Operation.Status == "running" || op.Operation.Status == "queued" {
			continue
		}
		if now.Sub(*op.Operation.FinishedAt) >= outputs && execution.State != "running" && execution.State != "unknown" {
			if execution.ResultObject != "" {
				garbage = append(garbage, execution.ResultObject)
			}
			execution.Result = nil
			execution.ResultObject = ""
			execution.State = "expired"
			st.Executions[id] = execution
		}
	}
	protected := map[string]bool{}
	for _, execution := range st.Executions {
		if execution.State == "unknown" || execution.State == "running" {
			protected[execution.OperationID] = true
		}
	}
	for _, box := range st.Boxes {
		protected[box.Box.OperationID] = true
	}
	removed := map[string]bool{}
	for id, record := range st.Operations {
		op := record.Operation
		if op.FinishedAt == nil || now.Sub(*op.FinishedAt) < operations || (op.Status != "succeeded" && op.Status != "failed") {
			continue
		}
		// An unknown execution may have mutated the workspace. Keep its compact
		// deduplication record; never turn a retry into an automatic replay.
		if protected[id] {
			continue
		}
		delete(st.Operations, id)
		removed[id] = true
	}
	for key, value := range st.Keys {
		if removed[value.OperationID] {
			// Keep only the replay fence, not the historical request or result.
			st.Keys[key] = keyRecord{Expired: true}
		}
	}
	for eid, value := range st.Executions {
		if removed[value.OperationID] {
			if value.ResultObject != "" {
				garbage = append(garbage, value.ResultObject)
			}
			delete(st.Executions, eid)
		}
	}
	for id, lease := range st.Leases {
		if !lease.ExpiresAt.After(now) {
			delete(st.Leases, id)
		}
	}
	referencedBoxes := map[string]bool{}
	for _, op := range st.Operations {
		referencedBoxes[op.Operation.TargetID] = true
	}
	for _, lease := range st.Leases {
		referencedBoxes[lease.BoxID] = true
	}
	deletedBoxes := map[string]bool{}
	for id, box := range st.Boxes {
		if box.Box.Phase != "deleted" || box.Box.OperationID != "" {
			continue
		}
		if !referencedBoxes[id] {
			delete(st.Boxes, id)
			deletedBoxes[id] = true
		}
	}
	for id, route := range st.Routes {
		if deletedBoxes[route.BoxID] {
			delete(st.Routes, id)
		}
	}
	return garbage
}

func (s *Service) pruneMetadata(now time.Time) error {
	s.gcMu.Lock()
	defer s.gcMu.Unlock()
	var garbage []string
	err := s.store.Update(func(st *State) error {
		garbage = pruneState(st, now, time.Duration(s.config.OperationRetentionSeconds)*time.Second, time.Duration(s.config.ExecutionRetentionSeconds)*time.Second)
		return nil
	})
	if err != nil || s.objects == nil || s.store.records == nil {
		return err
	}
	// Snapshot references after pending writers finish. Expired executions never
	// publish again, and orphan candidates get a grace period longer than the
	// writer deadline, so network cleanup need not hold the foreground write lock.
	s.store.writeMu.Lock()
	headETag := s.store.etag
	referenced := map[string]bool{}
	err = s.store.View(func(st State) error {
		for _, e := range st.Executions {
			referenced[e.ResultObject] = true
		}
		return nil
	})
	s.store.writeMu.Unlock()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	// A failed deletion or a crash before publishing a result leaves an orphan.
	// Scan again on every sweep, and give newly observed orphans more than the
	// 30-second transaction timeout before deleting them. Result keys include the
	// unique execution identity, so a later execution cannot reuse a retired key.
	_, headVersion, err := readObject(ctx, s.objects, stateObjectKey)
	if err != nil {
		return err
	}
	if headVersion != headETag {
		return objectstorage.ErrConflict
	}
	keys, err := s.objects.List(ctx, resultPrefix)
	if err != nil {
		return err
	}
	if s.store.records.orphans == nil {
		s.store.records.orphans = map[string]time.Time{}
	}
	for _, key := range garbage {
		s.store.records.orphans[key] = now.Add(-2 * time.Minute)
	}
	for _, key := range keys {
		if referenced[key] || !strings.HasPrefix(key, resultPrefix+"/") {
			delete(s.store.records.orphans, key)
			continue
		}
		first, exists := s.store.records.orphans[key]
		if !exists {
			s.store.records.orphans[key] = now
			continue
		}
		if now.Sub(first) < 2*time.Minute {
			continue
		}
		_, etag, err := readObject(ctx, s.objects, key)
		if errors.Is(err, objectstorage.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if err := s.objects.DeleteIfMatch(ctx, key, etag); err != nil && !errors.Is(err, objectstorage.ErrNotFound) {
			return err
		}
		delete(s.store.records.orphans, key)
	}
	return nil
}

func (s *Service) startRetention() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case now := <-ticker.C:
				_ = s.pruneMetadata(now)
			}
		}
	}()
}
