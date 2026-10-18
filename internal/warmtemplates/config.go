// SPDX-License-Identifier: Apache-2.0

// Package warmtemplates defines the immutable desired specs and observation
// snapshot used by the node-local shared warm pool.
package warmtemplates

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"

	api "cellbox.local/cellbox/api/v1alpha1"
	"k8s.io/apimachinery/pkg/util/validation"
)

type Template struct {
	ID    string   `json:"id"`
	Slots int      `json:"slots"`
	Spec  api.Spec `json:"spec"`
}

func Validate(namespace, key string, t Template) error {
	if len(validation.IsDNS1123Label(namespace)) > 0 || len(validation.IsDNS1123Label(key)) > 0 ||
		t.ID != key || t.Slots < 0 || t.Spec.NodeName == "" || t.Spec.DesiredState != "Running" ||
		t.Spec.Container.Name == "" || t.Spec.Container.Image == "" {
		return errors.New("invalid Cellbox warm template record")
	}
	return nil
}

// HashTemplates identifies the exact desired pool configuration. IDs are
// sorted so a SQL query's row order does not affect the hash.
func HashTemplates(templates []Template) (string, error) {
	ordered := append([]Template(nil), templates...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	encoded, err := json.Marshal(ordered)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:]), nil
}
