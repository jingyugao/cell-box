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
		ImportedImageID string `json:"importedImageId"`
	}
	if err := decode(w, r, &input); err != nil {
		fail(w, err)
		return
	}
	if input.ImportedImageID == "" {
		fail(w, apiError("INVALID_REQUEST", "importedImageId is required"))
		return
	}
	client := clientID(r)
	var original, next boxRecord
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
		image, ok := st.ImportedImages[input.ImportedImageID]
		if !ok || image.ClientID != client {
			return apiError("NOT_FOUND", "Imported image not found")
		}
		if image.ImportedImage.Deleting {
			return apiError("CONFLICT", "Imported image is being deleted")
		}
		// Hold the existing Box operation lock while resolving and caching the target.
		next = original
		next.UpgradeImageID = input.ImportedImageID
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
			profile, err := s.profile(client, original.Box.ProfileID)
			if err != nil {
				return nil, err
			}
			profile, err = s.importedProfile(client, input.ImportedImageID, profile)
			if err != nil {
				return nil, err
			}
			if !profile.PersistentHome || profile.NodeName != original.Profile.NodeName || profile.Namespace != original.Profile.Namespace ||
				profile.Guest.Workspace != original.Profile.Guest.Workspace || profile.Guest.Agent != original.Profile.Guest.Agent ||
				!reflect.DeepEqual(profile.Guest.Debug, original.Profile.Guest.Debug) ||
				profile.SharedReadOnlyHostPath != original.Profile.SharedReadOnlyHostPath ||
				profile.DebugReadOnlyHostPath != original.Profile.DebugReadOnlyHostPath || profile.DebugReadWriteHostPath != original.Profile.DebugReadWriteHostPath {
				return nil, apiError("CONFLICT", "Upgrade must retain the disk, node, identities and mounts")
			}
			_, ok := s.providers[profile.Provider].(boxprovider.HomeUpgradeProvider)
			if !ok {
				return nil, boxprovider.ErrUnsupported
			}
			if err := s.cacheImportedImage(ctx, profile.Image); err != nil {
				return nil, err
			}
			if original.Box.Phase == "running" {
				if err := s.quiesceGuest(ctx, original); err != nil {
					return nil, err
				}
			}
			profile.CPU, profile.MemoryMiB = original.Profile.CPU, original.Profile.MemoryMiB
			next.Profile = profile
			next.Box.Image, next.Box.ImageID, next.Box.ImportedImageID = profile.Image, profile.Image, input.ImportedImageID
			next.Box.Capabilities = profileCapabilities(profile)
			next.Staged, next.RestoreComplete = true, true
			next.RestoreArchiveID = ""
			// Persist intent before changing the CR. Observation replays the same
			// nonce after a lost response or API restart, never activates the old Pod.
			next.Upgrade = &upgradeIntent{Nonce: op.ID, PreviousImportedImageID: original.Box.ImportedImageID}
			if err := s.store.Update(func(st *State) error { st.Boxes[id] = next; return nil }); err != nil {
				return nil, err
			}
			if _, err := s.commitDiskUpgrade(ctx, next); err != nil {
				return nil, err
			}

			if _, err := s.waitState(ctx, id, "staged", true); err != nil {
				return nil, err
			}
			return map[string]string{"boxId": id, "image": profile.Image, "importedImageId": input.ImportedImageID}, nil
		}, nil)
	}
	writeJSON(w, http.StatusAccepted, op)
}

func (s *Service) commitDiskUpgrade(ctx context.Context, b boxRecord) (boxRecord, error) {
	if b.Upgrade == nil {
		return b, nil
	}
	provider, ok := s.providers[b.Profile.Provider].(boxprovider.HomeUpgradeProvider)
	if !ok {
		return b, boxprovider.ErrUnsupported
	}
	handle, err := provider.Upgrade(ctx, b.Handle, runtimeSpec(b), b.Upgrade.Nonce)
	if err != nil {
		return b, err
	}
	err = s.store.Update(func(st *State) error {
		current, ok := st.Boxes[b.Box.ID]
		if !ok {
			return apiError("NOT_FOUND", "Box not found")
		}
		if current.Upgrade != nil && current.Upgrade.Nonce == b.Upgrade.Nonce {
			current.Handle = handle
			current.Upgrade = nil
			current.Box.Version++
			st.Boxes[b.Box.ID] = current
		}
		b = current
		return nil
	})
	return b, err
}
