package service

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jingyugao/cell-box/internal/catalog"
)

// Template is a runtime projection of an immutable catalog revision. It is not
// persisted as a second template authority in the service metadata.
type Template struct {
	ID        string  `json:"id"` // template family ID
	Name      string  `json:"name"`
	Version   string  `json:"version"`
	ProfileID string  `json:"profileId"`
	Image     string  `json:"image"`
	CPU       float64 `json:"cpu"`
	MemoryMiB int64   `json:"memoryMiB"`
	WarmSlots int     `json:"warmSlots"`
}

type templateRecord struct {
	Template   Template `json:"template"`
	RevisionID string   `json:"revisionId"`
	ClientID   string   `json:"clientId"`
	Profile    Profile  `json:"profile"`
}

func catalogAPIError(err error) error {
	if err == nil {
		return nil
	}
	var e *catalog.Error
	if errors.As(err, &e) {
		return apiError(e.Code, e.Message)
	}
	var api *APIError
	if errors.As(err, &api) {
		return api
	}
	return err
}

// resolveTemplate resolves either a family stable pointer or a specific
// revision, then materializes its immutable runtime snapshot for existing
// provider code. Restore may explicitly target a retired revision recorded by
// an archive; ordinary creation require an active revision.
func (s *Service) resolveTemplate(client, templateID, revisionID string, allowRetired bool) (templateRecord, error) {
	var out templateRecord
	var revision catalog.Revision
	var snapshot json.RawMessage
	var err error
	ctx := context.Background()
	if revisionID != "" && allowRetired {
		revision, snapshot, err = s.catalog.RevisionSnapshot(ctx, client, revisionID)
	} else {
		revision, snapshot, err = s.catalog.Resolve(ctx, client, templateID, revisionID)
	}
	if err != nil {
		return out, catalogAPIError(err)
	}
	var profile Profile
	if err = json.Unmarshal(snapshot, &profile); err != nil {
		return out, err
	}
	if profile.ID == "" || profile.ID != revision.ProfileID || s.providers[profile.Provider] == nil {
		return out, apiError("CONFLICT", "Template revision runtime profile is unavailable")
	}
	if profile.Provider == "resumable-k8s-pod" {
		locationConfigured := false
		for _, configured := range s.config.Profiles {
			if configured.Provider == profile.Provider && configured.NodeName == profile.NodeName && configured.Namespace == profile.Namespace {
				locationConfigured = true
				break
			}
		}
		if !locationConfigured {
			return out, apiError("CONFLICT", "Template revision targets a resumable node or namespace that is not configured")
		}
	}
	out = templateRecord{
		Template: Template{ID: revision.TemplateID, Name: revision.TemplateName, Version: revision.Version,
			ProfileID: revision.ProfileID, Image: profile.Image, CPU: profile.CPU, MemoryMiB: profile.MemoryMiB,
			WarmSlots: revision.WarmSlots},
		RevisionID: revision.ID, ClientID: client, Profile: profile,
	}
	return out, nil
}
