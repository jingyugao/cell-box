package service

import (
	"context"
	"net/http"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
)

// CoCell owns its persisted project default; this publishes the same target to
// the node's warm pool without changing existing Boxes or the base profile.
func (s *Service) setWarmDefaultImage(w http.ResponseWriter, r *http.Request) {
	s.warmDefaultMu.Lock()
	defer s.warmDefaultMu.Unlock()
	var input struct {
		ImportedImageID string `json:"importedImageId,omitempty"`
	}
	if err := decode(w, r, &input); err != nil {
		fail(w, err)
		return
	}
	profile, err := s.profile(clientID(r), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	provider, ok := s.providers[profile.Provider].(boxprovider.WarmDefaultImageProvider)
	if !ok {
		fail(w, apiError("UNSUPPORTED_CAPABILITY", "Provider does not support a default warm image"))
		return
	}
	if input.ImportedImageID != "" {
		profile, err = s.importedProfile(clientID(r), input.ImportedImageID, profile)
		if err != nil {
			fail(w, err)
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	err = provider.SetWarmDefaultImage(ctx, profile.Namespace, boxprovider.WarmDefaultImage{
		ProfileID: profile.ID, NodeName: profile.NodeName, Image: profile.Image,
	})
	if err != nil {
		fail(w, cloneError(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"image": profile.Image})
}

func (s *Service) warmDefaultImages(ctx context.Context) ([]string, error) {
	seen := map[string]bool{}
	var images []string
	for _, profile := range s.config.Profiles {
		provider, ok := s.providers[profile.Provider].(boxprovider.WarmDefaultImageProvider)
		if !ok || profile.Namespace == "" || seen[profile.Namespace] {
			continue
		}
		seen[profile.Namespace] = true
		targets, err := provider.GetWarmDefaultImages(ctx, profile.Namespace)
		if err != nil {
			return nil, err
		}
		for _, target := range targets {
			images = append(images, target.Image)
		}
	}
	return images, nil
}
