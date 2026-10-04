package service

import (
	"context"
	"time"
)

func (s *Store) Changes() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changes == nil {
		s.changes = make(chan struct{})
	}
	return s.changes
}
func (s *Store) notifyLocked() {
	if s.changes != nil {
		close(s.changes)
		s.changes = nil
	}
}

func (s *Service) waitOperation(ctx context.Context, client, id string, wait time.Duration) (Operation, error) {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		// Subscribe before reading to avoid missing a commit between the two.
		changed := s.store.Changes()
		op, err := s.operation(client, id)
		if err != nil || wait == 0 || op.Status == "succeeded" || op.Status == "failed" {
			return op, err
		}
		select {
		case <-ctx.Done():
			return Operation{}, ctx.Err()
		case <-s.ctx.Done():
			return Operation{}, s.ctx.Err()
		case <-timer.C:
			return s.operation(client, id)
		case <-changed:
		}
	}
}
