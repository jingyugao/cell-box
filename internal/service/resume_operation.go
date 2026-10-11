package service

import (
	"context"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/telemetry"
)

func (s *Service) launchResumeOperation(request context.Context, op Operation, b boxRecord, repairOnly bool, completed func(error)) {
	work := func(ctx context.Context) (map[string]string, error) {
		p := s.providers[b.Profile.Provider]
		transitionID := ""
		if transitionProvider, ok := p.(boxprovider.TransitionProvider); ok {
			if repairOnly {
				transitionID = b.ResumeTransitionID
			} else {
				transitionID = op.ID
				runtimeCtx, runtimeSpan := telemetry.Start(ctx, "cellbox.runtime.transition", telemetry.String("action", "resume"))
				err := transitionProvider.ActionTransition(runtimeCtx, b.Handle, "resume", b.TransitionID, op.ID)
				if err == nil {
					runtimeSpan.AddEvent("cellbox.runtime.requested")
				}
				if err == nil {
					_, err = s.waitStateTransition(runtimeCtx, op.TargetID, "running", false, transitionID)
				}
				telemetry.End(runtimeSpan, err)
				if err != nil {
					return nil, err
				}
			}
		} else if !repairOnly {
			runtimeCtx, runtimeSpan := telemetry.Start(ctx, "cellbox.runtime.transition", telemetry.String("action", "resume"))
			err := p.Action(runtimeCtx, b.Handle, "resume")
			if err == nil {
				runtimeSpan.AddEvent("cellbox.runtime.requested")
				_, err = s.waitStateTransition(runtimeCtx, op.TargetID, "running", false, "")
			}
			telemetry.End(runtimeSpan, err)
			if err != nil {
				return nil, err
			}
		}
		updated, err := s.rawBox(op.TargetID)
		if err != nil {
			return nil, err
		}
		if updated.Box.OperationID != op.ID {
			return nil, boxprovider.ErrStaleTransition
		}
		if transitionID != "" && (updated.TransitionID != transitionID || updated.ObservedTransitionID != transitionID) {
			return nil, boxprovider.ErrStaleTransition
		}
		if repairOnly {
			if !updated.ResumePending || !updated.ResumeNeedsGuest || updated.ResumeExecutionID != updated.ExecutionID ||
				updated.ResumeTransitionID != updated.TransitionID || updated.ResumeObservedTransitionID != updated.ObservedTransitionID {
				return nil, boxprovider.ErrStaleTransition
			}
			observation, err := p.Inspect(ctx, updated.Handle)
			if err != nil {
				return nil, err
			}
			if observation.Phase != "running" || observation.ExecutionID != updated.ResumeExecutionID ||
				observation.TransitionID != updated.ResumeTransitionID || observation.ObservedTransitionID != updated.ResumeObservedTransitionID {
				return nil, boxprovider.ErrStaleTransition
			}
		} else {
			// Persist that runtime resume is confirmed before touching the Guest.
			// If shutdown interrupts unquiesce, restart can repair only this step.
			err = s.store.UpdateBoxContext(ctx, op.TargetID, func(st *State) error {
				current, err := owned(st, b.ClientID, op.TargetID)
				if err != nil {
					return err
				}
				if current.Box.OperationID != op.ID || !current.ResumePending {
					return boxprovider.ErrStaleTransition
				}
				if transitionID != "" && (current.TransitionID != transitionID || current.ObservedTransitionID != transitionID) {
					return boxprovider.ErrStaleTransition
				}
				current.ResumeNeedsGuest = true
				current.ResumeExecutionID = current.ExecutionID
				current.ResumeTransitionID = current.TransitionID
				current.ResumeObservedTransitionID = current.ObservedTransitionID
				current.Box.Phase = "resuming"
				current.Box.Version++
				st.Boxes[op.TargetID] = current
				updated = current
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
		started := time.Now()
		guestCtx, guestSpan := telemetry.Start(ctx, "cellbox.unquiesce")
		err = s.resumeGuest(guestCtx, updated)
		telemetry.End(guestSpan, err)
		runtimeTiming(op.TargetID, "cellbox.unquiesce", started, err)
		if err != nil {
			return nil, err
		}
		return map[string]string{"boxId": op.TargetID}, nil
	}
	commit := func(st *State) error {
		current, ok := st.Boxes[op.TargetID]
		if !ok || current.Box.OperationID != op.ID || !current.ResumePending || !current.ResumeNeedsGuest ||
			current.ResumeExecutionID != current.ExecutionID || current.ResumeTransitionID != current.TransitionID || current.ResumeObservedTransitionID != current.ObservedTransitionID {
			return boxprovider.ErrStaleTransition
		}
		current.ResumePending = false
		current.ResumeNeedsGuest = false
		current.ResumeIntentID = ""
		current.ResumeExecutionID = ""
		current.ResumeTransitionID = ""
		current.ResumeObservedTransitionID = ""
		current.Box.Phase = "running"
		current.Box.Error = nil
		current.Box.Version++
		st.Boxes[op.TargetID] = current
		return nil
	}
	done := func(err error) {
		if err == nil {
			if final, readErr := s.rawBox(op.TargetID); readErr == nil && final.Box.Phase == "running" && !final.ResumePending {
				s.checkHealthAsync(final)
			}
		}
		if completed != nil {
			completed(err)
		}
	}
	s.launchWithCompletion(request, op, work, commit, done)
}

func resumeOperationKind(kind string) bool { return kind == "resume" || kind == "ensure-running" }

func lifecycleOperationKind(kind string) bool {
	switch kind {
	case "create", "restore", "freeze", "unfreeze", "suspend", "resume", "ensure-running", "destroy", "activate", "reconcile", "rebuild", "upgrade":
		return true
	default:
		return false
	}
}

func (s *Service) recoverResumeOperations() {
	var pending []struct {
		op  Operation
		box boxRecord
	}
	_ = s.store.View(func(st State) error {
		for _, record := range st.Operations {
			op := record.Operation
			if !resumeOperationKind(op.Kind) || (op.Status != "running" && op.Status != "queued") {
				continue
			}
			b, ok := st.Boxes[op.TargetID]
			if !ok || b.Box.OperationID != op.ID || b.ClientID != record.ClientID {
				continue
			}
			pending = append(pending, struct {
				op  Operation
				box boxRecord
			}{op: op, box: b})
		}
		return nil
	})
	for _, item := range pending {
		s.launchResumeOperation(s.ctx, item.op, item.box, item.box.ResumeNeedsGuest, nil)
	}
}
