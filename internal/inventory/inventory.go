package inventory

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

const Annotation = "cellbox.local/inventory"
const ClientLabel = "cellbox.local/client"

type Record struct {
	ClientID  string          `json:"clientId"`
	Box       json.RawMessage `json:"box"`
	RuntimeID string          `json:"runtimeId,omitempty"`
	Snapshot  string          `json:"snapshot,omitempty"`
	NodeName  string          `json:"nodeName,omitempty"`
	Staged    bool            `json:"staged,omitempty"`
}

func ClientValue(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])[:40]
}
func FromResource(w *api.ResumablePod, phase string) (Record, error) {
	var r Record
	if err := json.Unmarshal([]byte(w.Annotations[Annotation]), &r); err != nil || r.ClientID == "" {
		return r, errors.New("invalid sandbox inventory annotation")
	}
	var box map[string]json.RawMessage
	if err := json.Unmarshal(r.Box, &box); err != nil || box == nil {
		return r, errors.New("invalid sandbox inventory box")
	}
	var id string
	if json.Unmarshal(box["id"], &id) != nil || id == "" || id != w.Labels["cellbox.local/box-id"] || w.Labels[ClientLabel] != ClientValue(r.ClientID) {
		return r, errors.New("sandbox inventory ownership mismatch")
	}
	box["phase"], _ = json.Marshal(phase)
	box["generation"], _ = json.Marshal(w.Status.Cycle)
	box["imageId"], _ = json.Marshal(w.Spec.Container.Image)
	// An operation ID in the creation annotation is no longer authoritative.
	delete(box, "operationId")
	delete(box, "error")
	if phase == "failed" {
		box["error"], _ = json.Marshal(map[string]string{"code": "RUNTIME_ERROR", "message": w.Status.Message})
	}
	r.Box, _ = json.Marshal(box)
	r.RuntimeID = string(w.UID)
	r.Snapshot = w.Status.Snapshot
	r.NodeName = w.Spec.NodeName
	return r, nil
}
