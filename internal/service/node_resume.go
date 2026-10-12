package service

import (
	"context"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/telemetry"
)

func (s *Service) launchNodeResume(request context.Context, op Operation, b boxRecord, repairOnly bool, provider boxprovider.ResumeRequestProvider, completed func(error)) {
	transitionID := op.ID
	if b.ResumeIntentID != "" {
		transitionID = b.ResumeIntentID
	}
	if repairOnly {
		if b.ResumeTransitionID != "" {
			transitionID = b.ResumeTransitionID
		}
	}
	var result boxprovider.Observation
	work := func(ctx context.Context) (map[string]string, error) {
		ctx, span := telemetry.Start(ctx, "cellbox.runtime.transition", telemetry.String("action", "resume"))
		var err error
		defer func() { telemetry.End(span, err) }()
		_, err = s.cachedRuntimeObservation(ctx, b)
		if err != nil {
			return nil, err
		}
		feed := s.ensureRuntimeFeed(b)
		observation, _, _ := s.feedSnapshot(feed)
		if observation.TransitionID != transitionID || observation.ObservedTransitionID != transitionID || !observation.GuestReady || observation.Phase != "running" {
			err = provider.RequestResume(ctx, b.Handle, b.TransitionID, transitionID)
			if err != nil {
				return nil, err
			}
			span.AddEvent("cellbox.runtime.requested")
		}
		result, err = s.waitNodeResume(ctx, b, transitionID)
		if err != nil {
			return nil, err
		}
		return map[string]string{"boxId": op.TargetID}, nil
	}
	commit := func(st *State) error {
		current, ok := st.Boxes[op.TargetID]
		if !ok || current.Box.OperationID != op.ID || !current.ResumePending || current.Handle != b.Handle {
			return boxprovider.ErrStaleTransition
		}
		feed := s.ensureRuntimeFeed(current)
		latest, observed, _ := s.feedSnapshot(feed)
		if !observed || latest.Phase != "running" || !latest.GuestReady || latest.TransitionID != transitionID || latest.ObservedTransitionID != transitionID || latest.ExecutionID != result.ExecutionID || latest.Generation != result.Generation {
			return boxprovider.ErrStaleTransition
		}
		if result.Generation < current.Box.Generation {
			return boxprovider.ErrStaleExecution
		}
		current.ExecutionID = result.ExecutionID
		current.Box.ExecutionID = result.ExecutionID
		current.Box.Generation = result.Generation
		current.TransitionID = result.TransitionID
		current.ObservedTransitionID = result.ObservedTransitionID
		current.ResumePending, current.ResumeNeedsGuest = false, false
		current.ResumeIntentID, current.ResumeExecutionID = "", ""
		current.ResumeTransitionID, current.ResumeObservedTransitionID = "", ""
		current.Box.Phase, current.Box.Error = "running", nil
		current.Box.Version++
		now := time.Now().UTC()
		current.Box.ObservedAt = &now
		st.Boxes[op.TargetID] = current
		return nil
	}
	s.launchWithCompletion(request, op, work, commit, completed)
}
