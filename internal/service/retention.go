package service

import (
	"context"
	"time"
)

// Retention removes terminal history, never running or unknown execution outcomes.
func (s *Service) pruneMetadata(now time.Time) error {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	if err := s.pruneVolumeExports(ctx, now); err != nil {
		return err
	}
	return s.store.Prune(ctx, now, time.Duration(s.config.OperationRetentionSeconds)*time.Second, time.Duration(s.config.ExecutionRetentionSeconds)*time.Second)
}
func (s *Store) Prune(ctx context.Context, now time.Time, operations, outputs time.Duration) error {
	outputCutoff := now.Add(-outputs).UTC().Format(time.RFC3339Nano)
	operationCutoff := now.Add(-operations).UTC().Format(time.RFC3339Nano)
	return s.Write(ctx, func(tx *storeTx) error {
		if err := tx.exec(`UPDATE runtime_records SET record=json_set(record,'$.state','expired') WHERE kind='executions'
            AND json_extract(record,'$.state') NOT IN ('running','unknown') AND json_extract(record,'$.operationId') IN
            (SELECT id FROM runtime_records WHERE kind='operations' AND json_extract(record,'$.operation.status') IN ('succeeded','failed')
             AND julianday(json_extract(record,'$.operation.finishedAt'))<=julianday(?))`, outputCutoff); err != nil {
			return err
		}
		if err := tx.exec(`DELETE FROM runtime_records WHERE kind='execution_results' AND id IN
            (SELECT id FROM runtime_records WHERE kind='executions' AND json_extract(record,'$.state')='expired')`); err != nil {
			return err
		}
		if err := tx.exec(`DELETE FROM runtime_records WHERE kind='operations'
            AND json_extract(record,'$.operation.status') IN ('succeeded','failed')
            AND julianday(json_extract(record,'$.operation.finishedAt'))<=julianday(?)
            AND id NOT IN (SELECT json_extract(record,'$.operationId') FROM runtime_records WHERE kind='executions' AND json_extract(record,'$.state') IN ('running','unknown'))
            AND id NOT IN (SELECT json_extract(record,'$.box.operationId') FROM runtime_records WHERE kind='boxes' AND json_extract(record,'$.box.operationId') IS NOT NULL)`, operationCutoff); err != nil {
			return err
		}
		if err := tx.exec(`DELETE FROM runtime_records WHERE kind='execution_results' AND id IN
            (SELECT id FROM runtime_records WHERE kind='executions' AND json_extract(record,'$.operationId') NOT IN (SELECT id FROM runtime_records WHERE kind='operations'))`); err != nil {
			return err
		}
		if err := tx.exec(`DELETE FROM runtime_records WHERE kind='executions' AND json_extract(record,'$.operationId') NOT IN (SELECT id FROM runtime_records WHERE kind='operations')`); err != nil {
			return err
		}
		return tx.exec(`DELETE FROM runtime_records WHERE kind='boxes' AND json_extract(record,'$.box.phase')='deleted'
            AND coalesce(json_extract(record,'$.box.operationId'),'')='' AND id NOT IN (SELECT box_id FROM runtime_records WHERE kind='operations')`)
	})
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
