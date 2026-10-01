package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/inventory"
	"cellbox.local/cellbox/internal/objectstorage"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"
)

// ListImages reads objects for every request. The mutation cache remains private
// to transaction handling and is never the source of a remote image listing.
func (s *Store) ListImages(ctx context.Context) ([]importedImageRecord, error) {
	if s.objects == nil {
		out := []importedImageRecord{}
		err := s.View(func(st State) error {
			for _, r := range st.ImportedImages {
				out = append(out, r)
			}
			return nil
		})
		return out, err
	}
	keys, err := s.objects.List(ctx, "images")
	if err != nil {
		return nil, err
	}
	out := []importedImageRecord{}
	for _, key := range keys {
		parts := strings.Split(key, "/")
		if len(parts) != 3 || parts[2] != "metadata.json" {
			continue
		}
		expected, err := imageMetadataKey(parts[1])
		if err != nil || key != expected {
			return nil, errors.New("invalid image metadata key")
		}
		data, _, err := readObject(ctx, s.objects, key)
		// A concurrent deletion between LIST and GET is expected.
		if errors.Is(err, objectstorage.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var stored storedImage
		if json.Unmarshal(data, &stored) != nil || stored.Revision == "" {
			return nil, errors.New("invalid image metadata")
		}
		if stored.Deleted {
			continue
		}
		if stored.Record == nil || stored.Record.ImportedImage.ID != parts[1] {
			return nil, errors.New("image metadata identity mismatch")
		}
		out = append(out, *stored.Record)
	}
	return out, nil
}
func sortInventory(boxes []Box) {
	sort.Slice(boxes, func(i, j int) bool {
		if boxes[i].CreatedAt.Equal(boxes[j].CreatedAt) {
			return boxes[i].ID < boxes[j].ID
		}
		return boxes[i].CreatedAt.Before(boxes[j].CreatedAt)
	})
}
func (s *Service) listBoxes(w http.ResponseWriter, r *http.Request) {
	observation := r.URL.Query().Get("observation")
	if observation != "" && observation != "resource" {
		fail(w, apiError("INVALID_REQUEST", "observation must be resource when supplied"))
		return
	}
	resourcesOnly := observation == "resource"
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	namespaces := map[string]boxprovider.InventoryProvider{}
	for _, profile := range s.config.Profiles {
		allowed := false
		for _, client := range profile.Clients {
			if client == clientID(r) {
				allowed = true
			}
		}
		if !allowed {
			continue
		}
		provider, ok := s.providers[profile.Provider].(boxprovider.InventoryProvider)
		if !ok {
			if resourcesOnly {
				fail(w, apiError("UNSUPPORTED_CAPABILITY", "Provider does not support resource inventory"))
				return
			}
			if s.objects != nil {
				fail(w, apiError("UNSUPPORTED_CAPABILITY", "Remote inventory requires a Kubernetes provider"))
				return
			}
			s.listLegacyBoxes(w, r)
			return
		}
		if resourcesOnly {
			if _, ok := provider.(boxprovider.ResourceInventoryProvider); !ok {
				fail(w, apiError("UNSUPPORTED_CAPABILITY", "Provider does not support resource inventory"))
				return
			}
		}
		namespaces[profile.Provider+"/"+profile.Namespace] = provider
	}
	wanted := map[string]bool{}
	for _, id := range r.URL.Query()["id"] {
		wanted[id] = true
	}
	filtered := len(wanted) > 0
	boxes := []Box{}
	for key, provider := range namespaces {
		namespace := strings.SplitN(key, "/", 2)[1]
		var rows []inventory.Record
		var err error
		if resourcesOnly {
			rows, err = provider.(boxprovider.ResourceInventoryProvider).ListResources(ctx, namespace, clientID(r))
		} else {
			rows, err = provider.List(ctx, namespace, clientID(r))
		}
		if err != nil {
			fail(w, err)
			return
		}
		for _, row := range rows {
			var box Box
			if json.Unmarshal(row.Box, &box) != nil || box.ID == "" {
				fail(w, errors.New("invalid runtime inventory box"))
				return
			}
			if !filtered || wanted[box.ID] {
				boxes = append(boxes, box)
				delete(wanted, box.ID)
			}
		}
	}
	// Preserve explicit ID lookup authorization semantics, without consulting state.
	if len(wanted) > 0 {
		fail(w, apiError("NOT_FOUND", "Box not found"))
		return
	}
	sortInventory(boxes)
	writeJSON(w, 200, boxes)
}
func (s *Service) listCheckpoints(w http.ResponseWriter, r *http.Request) {
	if s.objects == nil {
		s.listLegacyBoxes(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	keys, err := s.objects.List(ctx, "checkpoints")
	if err != nil {
		fail(w, err)
		return
	}
	boxes := []Box{}
	for _, key := range keys {
		parts := strings.Split(key, "/")
		if len(parts) != 3 || parts[2] != "metadata.json" {
			continue
		}
		data, _, err := readObject(ctx, s.objects, key)
		if errors.Is(err, objectstorage.ErrNotFound) {
			continue
		}
		if err != nil {
			fail(w, err)
			return
		}
		var record inventory.Record
		if json.Unmarshal(data, &record) != nil || record.RuntimeID != parts[1] || record.Snapshot == "" || record.ClientID == "" {
			fail(w, errors.New("invalid checkpoint inventory"))
			return
		}
		if record.ClientID != clientID(r) {
			continue
		}
		var box Box
		if json.Unmarshal(record.Box, &box) != nil || box.ID == "" || box.Phase == "" {
			fail(w, errors.New("invalid checkpoint box metadata"))
			return
		}
		boxes = append(boxes, box)
	}
	sortInventory(boxes)
	writeJSON(w, 200, boxes)
}

func (s *Store) ReadImage(ctx context.Context, id string) (importedImageRecord, error) {
	var record importedImageRecord
	if s.objects == nil {
		err := s.View(func(st State) error {
			var ok bool
			record, ok = st.ImportedImages[id]
			if !ok {
				return apiError("NOT_FOUND", "Imported image not found")
			}
			return nil
		})
		return record, err
	}
	key, err := imageMetadataKey(id)
	if err != nil {
		return record, apiError("NOT_FOUND", "Imported image not found")
	}
	data, _, err := readObject(ctx, s.objects, key)
	if errors.Is(err, objectstorage.ErrNotFound) {
		return record, apiError("NOT_FOUND", "Imported image not found")
	}
	if err != nil {
		return record, err
	}
	var stored storedImage
	if json.Unmarshal(data, &stored) != nil || stored.Revision == "" {
		return record, errors.New("invalid imported image metadata")
	}
	if stored.Deleted {
		return record, apiError("NOT_FOUND", "Imported image not found")
	}
	if stored.Record == nil || stored.Record.ImportedImage.ID != id {
		return record, errors.New("image metadata identity mismatch")
	}
	return *stored.Record, nil
}
