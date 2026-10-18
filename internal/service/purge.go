package service

import (
	"context"
	"net/http"
	"time"
)

// Purge deletes only a stopped, deleted sandbox's local history. Archives are independent.
func (s *Service) purgeBox(ctx context.Context, client, id string) error {
	unlock := s.admissions.lock(id)
	defer unlock()
	s.mu.Lock()
	err := s.streamBusy(id)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return s.store.Write(ctx, func(tx *storeTx) error {
		b, err := tx.Box(client, id)
		if err != nil {
			return err
		}
		if b.Box.Phase != "deleted" {
			return apiError("CONFLICT", "Only a deleted box can be purged")
		}
		if err = busy(tx, b); err != nil {
			return err
		}
		active, err := tx.HasActiveExecution(id)
		if err != nil {
			return err
		}
		if active {
			return apiError("BUSY", "Box has an active execution")
		}
		return tx.exec("DELETE FROM runtime_records WHERE box_id=? AND kind IN ('boxes','operations','executions','execution_results')", id)
	})
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
