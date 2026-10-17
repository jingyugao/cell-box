package service

import (
	"context"
	"net/http"
	"reflect"

	"cellbox.local/cellbox/internal/boxprovider"
)

// upgradeBox never exports or restores an archive. The provider fences the old
// execution before rearming the same disk and starts the replacement staged.
func (s *Service) upgradeBox(w http.ResponseWriter, r *http.Request, id string) {
	var input struct {
		TemplateID string `json:"templateId"`
	}
	if err := decode(w, r, &input); err != nil {
		fail(w, err)
		return
	}
	if input.TemplateID == "" {
		fail(w, apiError("INVALID_REQUEST", "templateId is required"))
		return
	}
	client := clientID(r)
	var original, next boxRecord
	var target templateRecord
	op, fresh, err := s.prepareOperation(client, r.Header.Get("Idempotency-Key"), "upgrade", id, input, func(st *State, op *Operation) error {
		var err error
		original, err = owned(st, client, id)
		if err != nil {
			return err
		}
		if original.Upgrade != nil {
			return apiError("BUSY", "A disk upgrade is still being committed")
		}
		if err := busy(st, original, true); err != nil {
			return err
		}
		if activeExec(st, id) {
			return apiError("BUSY", "Box has an active execution")
		}
		if original.Profile.Provider != "resumable-k8s-pod" || !original.Profile.PersistentHome {
			return boxprovider.ErrUnsupported
		}
		switch original.Box.Phase {
		case "running", "suspended", "failed", "staged":
		default:
			return apiError("CONFLICT", "Box is not ready for disk upgrade")
		}
		target, err = ownedTemplate(*st, client, input.TemplateID)
		if err != nil {
			return err
		}
		profile := target.Profile
		if !profile.PersistentHome || profile.NodeName != original.Profile.NodeName || profile.Namespace != original.Profile.Namespace ||
			profile.Guest.Workspace != original.Profile.Guest.Workspace || profile.Guest.Agent != original.Profile.Guest.Agent ||
			!reflect.DeepEqual(profile.Guest.Debug, original.Profile.Guest.Debug) ||
			profile.SharedReadOnlyHostPath != original.Profile.SharedReadOnlyHostPath ||
			profile.DebugReadOnlyHostPath != original.Profile.DebugReadOnlyHostPath || profile.DebugReadWriteHostPath != original.Profile.DebugReadWriteHostPath {
			return apiError("CONFLICT", "Upgrade must retain the disk, node, identities and mounts")
		}
		_, ok := s.providers[profile.Provider].(boxprovider.HomeUpgradeProvider)
		if !ok {
			return boxprovider.ErrUnsupported
		}
		// Hold the existing Box operation lock while resolving and caching the target.
		next = original
		next.UpgradeImageID = target.Template.ImportedImageID
		next.UpgradeTemplateID = target.Template.ID
		next.Box.OperationID, next.Box.Phase = op.ID, "upgrading"
		next.Box.Error = nil
		next.Box.Version++
		st.Boxes[id] = next
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	if fresh {
		s.launchWithCommitContext(r.Context(), op, func(ctx context.Context) (map[string]string, error) {
			profile := target.Profile
			if err := s.cacheImportedImage(ctx, profile.Image); err != nil {
				return nil, err
			}
			if original.Box.Phase == "running" {
				if err := s.quiesceGuest(ctx, original); err != nil {
					return nil, err
				}
			}
			// The template owns resources as well as the image; never override its captured configuration.
			next.Profile = profile
			next.Box.Image, next.Box.ImageID, next.Box.ImportedImageID = profile.Image, profile.Image, target.Template.ImportedImageID
			next.Box.ProfileID = profile.ID
			next.Box.TemplateID, next.Box.TemplateName, next.Box.TemplateVersion = target.Template.ID, target.Template.Name, target.Template.Version
			next.Box.Capabilities = profileCapabilities(profile)
			next.Staged, next.RestoreComplete = true, true
			next.RestoreArchiveID = ""
			// Persist intent before changing the CR. Observation replays the same
			// nonce after a lost response or API restart, never activates the old Pod.
			next.Upgrade = &upgradeIntent{Nonce: op.ID, PreviousTemplateID: original.Box.TemplateID, PreviousImportedImageID: original.Box.ImportedImageID}
			if err := s.store.Update(func(st *State) error { st.Boxes[id] = next; return nil }); err != nil {
				return nil, err
			}
			if _, err := s.commitDiskUpgrade(ctx, next); err != nil {
				return nil, err
			}

			if _, err := s.waitState(ctx, id, "staged", true); err != nil {
				return nil, err
			}
			return map[string]string{"boxId": id, "image": profile.Image, "templateId": target.Template.ID}, nil
		}, nil)
	}
	writeJSON(w, http.StatusAccepted, op)
}

func (s *Service) commitDiskUpgrade(ctx context.Context, b boxRecord) (boxRecord, error) {
	// Serialize intent replay with new lifecycle admission. A delayed observer
	// must never replay a superseded image transition from its stale snapshot.
	unlock := s.admissions.lock(b.Box.ID)
	defer unlock()
	current, err := s.rawBox(b.Box.ID)
	if err != nil {
		return b, err
	}
	b = current
	if b.Upgrade == nil {
		return b, nil
	}
	if b.Upgrade.RuntimeCommitted {
		return b, nil
	}
	if b.Box.OperationID == "" || b.Box.OperationID != b.Upgrade.Nonce {
		return b, boxprovider.ErrStaleTransition
	}
	nonce := b.Upgrade.Nonce
	provider, ok := s.providers[b.Profile.Provider].(boxprovider.HomeUpgradeProvider)
	if !ok {
		return b, boxprovider.ErrUnsupported
	}
	handle, err := provider.Upgrade(ctx, b.Handle, runtimeSpec(b), nonce)
	if err != nil {
		return b, err
	}
	err = s.store.Update(func(st *State) error {
		current, ok := st.Boxes[b.Box.ID]
		if !ok {
			return apiError("NOT_FOUND", "Box not found")
		}
		if current.Box.OperationID != nonce || current.Upgrade == nil || current.Upgrade.Nonce != nonce {
			return boxprovider.ErrStaleTransition
		}
		current.Handle = handle
		current.Upgrade.RuntimeCommitted = true
		current.Box.Version++
		st.Boxes[b.Box.ID] = current
		b = current
		return nil
	})
	return b, err
}

// recoverUpgradeOperations replays only a persisted, Box-owned disk upgrade
// intent. Earlier interrupted preparation phases are never guessed or rerun.
func (s *Service) recoverUpgradeOperations() {
	var pending []Operation
	_ = s.store.View(func(st State) error {
		for _, record := range st.Operations {
			op := record.Operation
			if op.Kind != "upgrade" || (op.Status != "running" && op.Status != "queued") {
				continue
			}
			b, ok := st.Boxes[op.TargetID]
			if ok && b.ClientID == record.ClientID && b.Box.OperationID == op.ID && b.Upgrade != nil && b.Upgrade.Nonce == op.ID {
				pending = append(pending, op)
			}
		}
		return nil
	})
	for _, op := range pending {
		s.launchWithCompletion(s.ctx, op, func(ctx context.Context) (map[string]string, error) {
			b, err := s.rawBox(op.TargetID)
			if err != nil {
				return nil, err
			}
			if _, err = s.commitDiskUpgrade(ctx, b); err != nil {
				return nil, err
			}
			if _, err = s.waitState(ctx, op.TargetID, "staged", true); err != nil {
				return nil, err
			}
			b, err = s.rawBox(op.TargetID)
			if err != nil {
				return nil, err
			}
			return map[string]string{"boxId": op.TargetID, "image": b.Box.Image, "importedImageId": b.Box.ImportedImageID}, nil
		}, nil, nil)
	}
}
