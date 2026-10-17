// SPDX-License-Identifier: Apache-2.0

// Package warmtemplates stores the complete runtime specs that feed the
// node-local shared warm pool.
package warmtemplates

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	api "cellbox.local/cellbox/api/v1alpha1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const ConfigMapName = "cellbox-warm-templates"

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

// Read returns no targets when the ConfigMap is absent. Invalid records fail
// closed so an incomplete template can never cause an unrelated Pod to warm.
func Read(ctx context.Context, reader client.Reader, namespace string) ([]Template, error) {
	if ctx == nil || reader == nil || len(validation.IsDNS1123Label(namespace)) > 0 {
		return nil, errors.New("Kubernetes reader and namespace are required")
	}
	cm := &core.ConfigMap{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ConfigMapName}, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if cm.Namespace != namespace || cm.Name != ConfigMapName {
		return nil, errors.New("warm template ConfigMap identity mismatch")
	}
	out := make([]Template, 0, len(cm.Data))
	for key, raw := range cm.Data {
		var t Template
		decoder := json.NewDecoder(bytes.NewBufferString(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&t); err != nil {
			return nil, fmt.Errorf("decode warm template %q: %w", key, err)
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("decode warm template %q: expected one JSON object", key)
		}
		if err := Validate(namespace, key, t); err != nil {
			return nil, fmt.Errorf("validate warm template %q: %w", key, err)
		}
		out = append(out, t)
	}
	return out, nil
}
