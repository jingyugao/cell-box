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

func (s *Service) waitOperation(ctx context.Context, client, id string, wait time.Duration) (result Operation, retErr error) {
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

// The HTTP caller waits for a durable terminal result. Accepted work retains its
// service-owned context: a lost connection does not cancel or duplicate restore.
func (s *Service) waitResume(ctx context.Context, client, id string) (Operation, error) {
	// The worker has a 15 minute deadline plus a bounded completion commit.
	op, err := s.waitOperation(ctx, client, id, 16*time.Minute)
	if err != nil {
		return Operation{}, err
	}
	switch op.Status {
	case "succeeded":
		return op, nil
	case "failed":
		if op.Error != nil {
			return Operation{}, op.Error
		}
		return Operation{}, apiError("RUNTIME_ERROR", "Resume failed")
	default:
		return Operation{}, apiError("UNKNOWN_OUTCOME", "Lifecycle operation is still pending; retry with the same idempotency key")
	}
}
