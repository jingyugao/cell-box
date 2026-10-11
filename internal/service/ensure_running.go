package service

import (
	"context"
	"fmt"
	"time"
)

func (s *Service) ensureRunning(request context.Context, client, key, id string, expectedGeneration *uint64) (Operation, error) {
	if key == "" || len(key) > 200 {
		return Operation{}, apiError("INVALID_REQUEST", "Idempotency-Key header (1..200 bytes) is required")
	}
	input := struct {
		ExpectedGeneration *uint64 `json:"expectedGeneration,omitempty"`
	}{ExpectedGeneration: expectedGeneration}
	digest := inputHash(input)
	key = client + ":ensure-running:" + id + ":" + key
	unlock := s.admissions.lock(id)
	defer unlock()

	snapshot, err := s.store.BoxAdmission(client, id, key)
	if err != nil {
		return Operation{}, err
	}
	current := snapshot.Box
	if snapshot.KeyExists {
		if snapshot.Key.Expired {
			return Operation{}, apiError("OPERATION_EXPIRED", "Operation history has expired; this idempotency key cannot be reused")
		}
		if snapshot.Key.Hash != digest {
			return Operation{}, apiError("CONFLICT", "Idempotency key was already used with different input")
		}
		if !snapshot.KeyOperationExists {
			return Operation{}, apiError("NOT_FOUND", "Operation not found")
		}
		return snapshot.KeyOperation, nil
	}
	if expectedGeneration != nil && *expectedGeneration != current.Box.Generation {
		return Operation{}, apiError("STALE_GENERATION", "Lifecycle request targets another Box generation")
	}

	// Joining an existing resume is a metadata-only admission. It must not issue
	// a second provider transition or wait for another K8s observation.
	if active := snapshot.ActiveOperation; snapshot.ActiveOperationExists {
		if !resumeOperationKind(active.Kind) || active.Status != "running" && active.Status != "queued" {
			return Operation{}, apiError("BUSY", "Box has an active operation")
		}
		return s.bindEnsureKey(client, key, digest, id, current.Box.Generation, active)
	}

	observed, err := s.observe(request, current)
	if err != nil {
		return Operation{}, err
	}
	current, err = s.rawBox(id)
	if err != nil {
		return Operation{}, err
	}
	if expectedGeneration != nil && *expectedGeneration != current.Box.Generation {
		return Operation{}, apiError("STALE_GENERATION", "Lifecycle request targets another Box generation")
	}
	if current.Box.Version != observed.Version {
		return Operation{}, apiError("CONFLICT", "Box changed while ensuring it is running")
	}
	if current.Upgrade != nil || current.Box.OperationID != "" {
		latest, readErr := s.store.BoxAdmission(client, id, "")
		if readErr != nil {
			return Operation{}, readErr
		}
		if latest.ActiveOperationExists && resumeOperationKind(latest.ActiveOperation.Kind) {
			return s.bindEnsureKey(client, key, digest, id, current.Box.Generation, latest.ActiveOperation)
		}
		return Operation{}, apiError("BUSY", "Box has an active operation")
	}

	var operation Operation
	var launch bool
	var repairOnly bool
	var admitted boxRecord
	err = s.store.UpdateBoxContext(request, id, func(st *State) error {
		if previous, ok := st.Keys[key]; ok {
			if previous.Expired {
				return apiError("OPERATION_EXPIRED", "Operation history has expired; this idempotency key cannot be reused")
			}
			if previous.Hash != digest {
				return apiError("CONFLICT", "Idempotency key was already used with different input")
			}
			operation = st.Operations[previous.OperationID].Operation
			return nil
		}
		b, err := owned(st, client, id)
		if err != nil {
			return err
		}
		if expectedGeneration != nil && *expectedGeneration != b.Box.Generation {
			return apiError("STALE_GENERATION", "Lifecycle request targets another Box generation")
		}
		if b.Box.Version != current.Box.Version || b.Box.Phase != current.Box.Phase || b.TransitionID != current.TransitionID || b.ObservedTransitionID != current.ObservedTransitionID ||
			b.ResumePending != current.ResumePending || b.ResumeNeedsGuest != current.ResumeNeedsGuest || b.ResumeIntentID != current.ResumeIntentID {
			return apiError("CONFLICT", "Box changed while ensuring it is running")
		}
		if b.Box.OperationID != "" || b.Upgrade != nil {
			return apiError("BUSY", "Box has an active operation")
		}
		operation = Operation{ID: randomID("op-"), Kind: "ensure-running", TargetID: id, Status: "running", Version: 1, CreatedAt: time.Now().UTC()}
		admitTransition := func(repair bool) error {
			if err := busy(st, b, true); err != nil {
				return err
			}
			if activeExec(st, id) {
				return apiError("BUSY", "Box has an active execution")
			}
			s.mu.Lock()
			streamErr := s.streamBusy(id)
			s.mu.Unlock()
			if streamErr != nil {
				return streamErr
			}
			if repair {
				if !b.ResumePending || !b.ResumeNeedsGuest || b.ResumeExecutionID != b.ExecutionID ||
					b.ResumeTransitionID != b.TransitionID || b.ResumeObservedTransitionID != b.ObservedTransitionID {
					return apiError("STALE_TRANSITION", "Guest resume intent no longer matches the observed runtime")
				}
				repairOnly = true
			} else {
				b.ResumePending = true
				b.ResumeNeedsGuest = false
				b.ResumeIntentID = operation.ID
				b.ResumeExecutionID = ""
				b.ResumeTransitionID = ""
				b.ResumeObservedTransitionID = ""
			}
			b.Box.OperationID = operation.ID
			b.Box.Phase = "resuming"
			b.Box.Error = nil
			b.Box.Version++
			st.Boxes[id] = b
			admitted = b
			launch = true
			return nil
		}
		switch b.Box.Phase {
		case "running":
			if b.ResumePending {
				if !b.ResumeNeedsGuest {
					return apiError("STALE_TRANSITION", "Runtime is running without a confirmed guest resume transition")
				}
				if err := admitTransition(true); err != nil {
					return err
				}
				break
			}
			now := time.Now().UTC()
			box := b.Box
			box.OperationID = ""
			operation.Status = "succeeded"
			operation.Version++
			operation.FinishedAt = &now
			operation.Result = map[string]string{"boxId": id}
			operation.Box = &box
		case "resuming":
			if !b.ResumePending || !b.ResumeNeedsGuest {
				return apiError("CONFLICT", "Box has an unconfirmed resume transition")
			}
			if err := admitTransition(true); err != nil {
				return err
			}
			break
		case "suspended":
			if err := admitTransition(false); err != nil {
				return err
			}
			break
		default:
			return apiError("CONFLICT", fmt.Sprintf("Box cannot ensure running from phase %s", b.Box.Phase))
		}
		st.Operations[operation.ID] = operationRecord{Operation: operation, ClientID: client}
		st.Keys[key] = keyRecord{Hash: digest, OperationID: operation.ID}
		return nil
	})
	if err != nil {
		return Operation{}, err
	}
	if launch {
		s.launchResumeOperation(request, operation, admitted, repairOnly, nil)
	}
	return operation, nil
}

func (s *Service) bindEnsureKey(client, key, digest, id string, generation uint64, active Operation) (Operation, error) {
	var operation Operation
	err := s.store.UpdateBoxContext(context.Background(), id, func(st *State) error {
		if previous, ok := st.Keys[key]; ok {
			if previous.Expired {
				return apiError("OPERATION_EXPIRED", "Operation history has expired; this idempotency key cannot be reused")
			}
			if previous.Hash != digest {
				return apiError("CONFLICT", "Idempotency key was already used with different input")
			}
			operation = st.Operations[previous.OperationID].Operation
			return nil
		}
		b, err := owned(st, client, id)
		if err != nil {
			return err
		}
		if b.Box.Generation != generation || b.Box.OperationID != active.ID {
			return apiError("CONFLICT", "Box lifecycle operation changed while joining")
		}
		record, ok := st.Operations[active.ID]
		if !ok || record.ClientID != client || !resumeOperationKind(record.Operation.Kind) || record.Operation.Status != "running" && record.Operation.Status != "queued" {
			return apiError("BUSY", "Box has an active operation")
		}
		operation = record.Operation
		st.Keys[key] = keyRecord{Hash: digest, OperationID: operation.ID}
		return nil
	})
	return operation, err
}
