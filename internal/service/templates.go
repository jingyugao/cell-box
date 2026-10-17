package service

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
)

// Template identifies one immutable, resolved execution configuration. WarmSlots
// is mutable scheduling policy, not part of the version's execution identity.
type Template struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	Version         string    `json:"version"`
	ProfileID       string    `json:"profileId"`
	ImportedImageID string    `json:"importedImageId,omitempty"`
	Image           string    `json:"image"`
	CPU             float64   `json:"cpu"`
	MemoryMiB       int64     `json:"memoryMiB"`
	WarmSlots       int       `json:"warmSlots"`
	Deleting        bool      `json:"deleting,omitempty"`
	CreatedAt       time.Time `json:"createdAt"`
}

type templateRecord struct {
	Template    Template `json:"template"`
	ClientID    string   `json:"clientId"`
	Profile     Profile  `json:"profile"`
	RequestHash string   `json:"requestHash"`
	Deleting    bool     `json:"deleting,omitempty"`
}

type createTemplateRequest struct {
	Name            string `json:"name"`
	Version         string `json:"version"`
	ProfileID       string `json:"profileId"`
	ImportedImageID string `json:"importedImageId,omitempty"`
	WarmSlots       int    `json:"warmSlots,omitempty"`
}

var templateVersion = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,79}$`)

func (s *Service) validateTemplateLocations() error {
	return s.store.View(func(st State) error {
		for id, rec := range st.Templates {
			if s.providers[rec.Profile.Provider] == nil {
				return fmt.Errorf("template %s requires provider %s", id, rec.Profile.Provider)
			}
			if rec.Profile.Provider != "resumable-k8s-pod" {
				continue
			}
			found := false
			for _, profile := range s.config.Profiles {
				if profile.Provider == rec.Profile.Provider && profile.NodeName == rec.Profile.NodeName && profile.Namespace == rec.Profile.Namespace {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("template %s belongs to a different node or namespace; local templates cannot be relocated by changing profiles", id)
			}
		}
		return nil
	})
}

func ownedTemplate(st State, client, id string) (templateRecord, error) {
	record, ok := st.Templates[id]
	if !ok || record.ClientID != client {
		return record, apiError("NOT_FOUND", "Template not found")
	}
	if record.Deleting {
		return record, apiError("CONFLICT", "Template is being deleted")
	}
	return record, nil
}

func (s *Service) template(client, id string) (templateRecord, error) {
	var record templateRecord
	err := s.store.View(func(st State) error { var err error; record, err = ownedTemplate(st, client, id); return err })
	return record, err
}

func (s *Service) validateWarmSlots(profile Profile, slots int) error {
	if slots < 0 || slots > 1024 {
		return apiError("INVALID_REQUEST", "warmSlots must be 0..1024")
	}
	if slots > 0 {
		if _, ok := s.providers[profile.Provider].(boxprovider.WarmTemplateProvider); !ok {
			return apiError("UNSUPPORTED_CAPABILITY", "Provider does not support template prewarming")
		}
	}
	return nil
}

func (s *Service) createTemplate(w http.ResponseWriter, r *http.Request) {
	var input createTemplateRequest
	if err := decode(w, r, &input); err != nil {
		fail(w, err)
		return
	}
	if !validName.MatchString(input.Name) || !templateVersion.MatchString(input.Version) || input.ProfileID == "" {
		fail(w, apiError("INVALID_REQUEST", "name, version and profileId are required and must be valid identifiers"))
		return
	}
	s.templateMu.Lock()
	defer s.templateMu.Unlock()
	client := clientID(r)
	hash := inputHash(input)
	var result Template
	// A retry resolves the original immutable version, even if operator config changed.
	err := s.store.View(func(st State) error {
		for _, rec := range st.Templates {
			if rec.ClientID == client && rec.Template.Name == input.Name && rec.Template.Version == input.Version {
				if rec.Deleting || rec.RequestHash != hash {
					return apiError("CONFLICT", "Template version already exists with different configuration or is being deleted")
				}
				result = rec.Template
			}
		}
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	status := http.StatusOK
	if result.ID == "" {
		profile, err := s.profile(client, input.ProfileID)
		if err != nil {
			fail(w, err)
			return
		}
		if input.ImportedImageID != "" {
			profile, err = s.importedProfile(client, input.ImportedImageID, profile)
			if err != nil {
				fail(w, err)
				return
			}
		}
		if err = s.validateWarmSlots(profile, input.WarmSlots); err != nil {
			fail(w, err)
			return
		}
		result = Template{ID: randomID("tpl-"), Name: input.Name, Version: input.Version, ProfileID: input.ProfileID, ImportedImageID: input.ImportedImageID, Image: profile.Image, CPU: profile.CPU, MemoryMiB: profile.MemoryMiB, WarmSlots: input.WarmSlots, CreatedAt: time.Now().UTC()}
		err = s.store.Update(func(st *State) error {
			// Image deletion and template registration serialize at the same commit.
			if input.ImportedImageID != "" {
				image, ok := st.ImportedImages[input.ImportedImageID]
				if !ok || image.ClientID != client {
					return apiError("NOT_FOUND", "Imported image not found")
				}
				if image.ImportedImage.Deleting {
					return apiError("CONFLICT", "Imported image is being deleted")
				}
			}
			st.Templates[result.ID] = templateRecord{Template: result, ClientID: client, Profile: profile, RequestHash: hash}
			return nil
		})
		if err != nil {
			fail(w, err)
			return
		}
		status = http.StatusCreated
	}
	if err = s.syncTemplates(r.Context()); err != nil {
		fail(w, apiError("UNKNOWN_OUTCOME", "Template saved; prewarm publication failed; retry the same request"))
		return
	}
	writeJSON(w, status, result)
}

func (s *Service) listTemplates(w http.ResponseWriter, r *http.Request) {
	out := []Template{}
	err := s.store.View(func(st State) error {
		for _, rec := range st.Templates {
			if rec.ClientID == clientID(r) {
				t := rec.Template
				t.Deleting = rec.Deleting
				out = append(out, t)
			}
		}
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Version < out[j].Version
	})
	writeJSON(w, http.StatusOK, out)
}

func (s *Service) getTemplate(w http.ResponseWriter, r *http.Request) {
	var out Template
	err := s.store.View(func(st State) error {
		rec, ok := st.Templates[r.PathValue("id")]
		if !ok || rec.ClientID != clientID(r) {
			return apiError("NOT_FOUND", "Template not found")
		}
		out = rec.Template
		out.Deleting = rec.Deleting
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Service) setTemplateWarmSlots(w http.ResponseWriter, r *http.Request) {
	var input struct {
		WarmSlots *int `json:"warmSlots"`
	}
	if err := decode(w, r, &input); err != nil {
		fail(w, err)
		return
	}
	if input.WarmSlots == nil {
		fail(w, apiError("INVALID_REQUEST", "warmSlots is required"))
		return
	}
	s.templateMu.Lock()
	defer s.templateMu.Unlock()
	var out Template
	err := s.store.Update(func(st *State) error {
		rec, err := ownedTemplate(*st, clientID(r), r.PathValue("id"))
		if err != nil {
			return err
		}
		if err = s.validateWarmSlots(rec.Profile, *input.WarmSlots); err != nil {
			return err
		}
		rec.Template.WarmSlots = *input.WarmSlots
		st.Templates[rec.Template.ID] = rec
		out = rec.Template
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	if err = s.syncTemplates(r.Context()); err != nil {
		fail(w, apiError("UNKNOWN_OUTCOME", "Prewarm policy saved; publication failed; retry the same request"))
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Service) deleteTemplate(w http.ResponseWriter, r *http.Request) {
	s.templateMu.Lock()
	defer s.templateMu.Unlock()
	id := r.PathValue("id")
	err := s.store.Update(func(st *State) error {
		rec, ok := st.Templates[id]
		if !ok || rec.ClientID != clientID(r) {
			return apiError("NOT_FOUND", "Template not found")
		}
		for _, box := range st.Boxes {
			if box.Box.Phase != "deleted" && (box.Box.TemplateID == id || box.UpgradeTemplateID == id || (box.Upgrade != nil && box.Upgrade.PreviousTemplateID == id)) {
				return apiError("CONFLICT", "Template is referenced by a Box")
			}
		}
		for _, archive := range st.Archives {
			if archive.Archive.TemplateID == id {
				return apiError("CONFLICT", "Template is referenced by an archive")
			}
		}
		rec.Deleting = true
		st.Templates[id] = rec
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	// Keep image references until the desired warm target has been withdrawn.
	if err = s.syncTemplates(r.Context()); err != nil {
		fail(w, apiError("UNKNOWN_OUTCOME", "Template deletion is pending prewarm withdrawal; retry or inspect the template"))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// stagedWarmSlots divides one template's requested capacity between its two
// launch forms. A staged launch retains its argv after activation, so its
// checkpoint must restore into the exact staged Spec. With no instances yet,
// prepare the ordinary form; existing instances steer the same shared budget.
func stagedWarmSlots(st State, rec templateRecord) (ordinary, staged int) {
	var hasOrdinary, hasStaged, latestStaged bool
	var latest time.Time
	var latestID string
	for _, box := range st.Boxes {
		if box.Box.Phase == "deleted" || box.Box.Phase == "deleting" {
			continue
		}
		mode := box.Staged
		if box.UpgradeTemplateID == rec.Template.ID {
			mode = true
		} else if box.Box.TemplateID != rec.Template.ID {
			continue
		}
		if mode {
			hasStaged = true
		} else {
			hasOrdinary = true
		}
		if latestID == "" || box.Box.CreatedAt.After(latest) || (box.Box.CreatedAt.Equal(latest) && box.Box.ID > latestID) {
			latest, latestStaged = box.Box.CreatedAt, mode
			latestID = box.Box.ID
		}
	}
	count := rec.Template.WarmSlots
	if !hasStaged {
		return count, 0
	}
	if !hasOrdinary {
		return 0, count
	}
	ordinary, staged = count/2, count/2
	if count%2 != 0 {
		if latestStaged {
			staged++
		} else {
			ordinary++
		}
	}
	return ordinary, staged
}

// syncTemplates is called with templateMu held. Publication is a full desired
// state replacement; retrying it after process or Kubernetes failures is safe.
func (s *Service) syncTemplates(ctx context.Context) error {
	type location struct{ provider, namespace string }
	targets := map[location][]boxprovider.WarmTemplate{}
	for _, profile := range s.config.Profiles {
		if _, ok := s.providers[profile.Provider].(boxprovider.WarmTemplateProvider); ok {
			targets[location{profile.Provider, profile.Namespace}] = nil
		}
	}
	deleting := []string{}
	err := s.store.View(func(st State) error {
		for id, rec := range st.Templates {
			where := location{rec.Profile.Provider, rec.Profile.Namespace}
			if rec.Deleting {
				deleting = append(deleting, id)
			}
			if _, ok := s.providers[rec.Profile.Provider].(boxprovider.WarmTemplateProvider); !ok {
				if rec.Template.WarmSlots > 0 {
					return fmt.Errorf("template %s provider is unavailable", id)
				}
				continue
			}
			if _, ok := targets[where]; !ok {
				targets[where] = nil
			}
			if rec.Deleting || rec.Template.WarmSlots == 0 {
				continue
			}
			ordinary, staged := stagedWarmSlots(st, rec)
			for _, variant := range []struct {
				suffix string
				staged bool
				slots  int
			}{
				{"", false, ordinary}, {"-staged", true, staged},
			} {
				if variant.slots == 0 {
					continue
				}
				spec := runtimeSpec(boxRecord{Box: Box{ID: id}, Profile: rec.Profile, Staged: variant.staged})
				spec.Inventory = nil
				targets[where] = append(targets[where], boxprovider.WarmTemplate{ID: id + variant.suffix, Slots: variant.slots, Spec: spec})
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for where, list := range targets {
		sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
		call, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := s.providers[where.provider].(boxprovider.WarmTemplateProvider).SetWarmTemplates(call, where.namespace, list)
		cancel()
		if err != nil {
			return err
		}
	}
	if len(deleting) > 0 {
		return s.store.Update(func(st *State) error {
			for _, id := range deleting {
				delete(st.Templates, id)
			}
			return nil
		})
	}
	return nil
}

func (s *Service) notifyTemplateSync() {
	select {
	case s.templateWake <- struct{}{}:
	default:
	}
}

func (s *Service) startTemplateSync() {
	capable := false
	for _, profile := range s.config.Profiles {
		if _, ok := s.providers[profile.Provider].(boxprovider.WarmTemplateProvider); ok {
			capable = true
		}
	}
	if !capable {
		return
	}
	// One service owns local metadata and publishes this node's template catalog.
	// Restart/retry repairs the window between a durable policy and publication.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			s.templateMu.Lock()
			err := s.syncTemplates(s.ctx)
			s.templateMu.Unlock()
			if err != nil && s.ctx.Err() == nil {
				slog.Error("template prewarm sync failed", "error", err)
			}
			select {
			case <-s.ctx.Done():
				return
			case <-s.templateWake:
			case <-ticker.C:
			}
		}
	}()
}
