package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"context"
	"time"
)

// A resume submits one runtime transition. There is no repair or request replay.
func (s *Service) launchNodeResume(ctx context.Context, op Operation, b boxRecord, provider boxprovider.ResumeRequestProvider) {
	var result boxprovider.Observation
	s.launchWithCommitContext(ctx, op, func(ctx context.Context) (map[string]string, error) {
		s.ensureRuntimeFeed(b)
		if err := provider.RequestResume(ctx, b.Handle, b.TransitionID, op.ID); err != nil {
			return nil, err
		}
		var err error
		result, err = s.waitNodeResume(ctx, b, op.ID)
		if err != nil {
			return nil, err
		}
		return map[string]string{"boxId": op.TargetID}, nil
	}, func(tx *storeTx) error {
		current, err := tx.Box(b.ClientID, op.TargetID)
		if err != nil {
			return err
		}
		if current.Box.OperationID != op.ID || current.Handle != b.Handle {
			return boxprovider.ErrStaleTransition
		}
		latest, observed, _ := s.feedSnapshot(s.ensureRuntimeFeed(current))
		if !observed || latest.Phase != "running" || !latest.GuestReady || latest.TransitionID != op.ID || latest.ObservedTransitionID != op.ID ||
			latest.ExecutionID != result.ExecutionID || latest.Generation != result.Generation || result.Generation < current.Box.Generation {
			return boxprovider.ErrStaleTransition
		}
		current.ExecutionID = result.ExecutionID
		current.Box.ExecutionID = result.ExecutionID
		current.Box.Generation = result.Generation
		current.TransitionID = result.TransitionID
		current.ObservedTransitionID = result.ObservedTransitionID
		current.Box.Phase = "running"
		current.Box.Error = nil
		current.Box.Version++
		now := time.Now().UTC()
		current.Box.ObservedAt = &now
		return tx.PutBox(current)
	})
}
