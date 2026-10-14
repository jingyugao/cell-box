// SPDX-License-Identifier: Apache-2.0

// Package warmdefaults stores the immutable image selected for each warm-pool
// profile in the Cellbox namespace.
package warmdefaults

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"

	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const ConfigMapName = "cellbox-warm-default"

var validProfileID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$`)
var validImage = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/-]*@sha256:[a-f0-9]{64}$`)
var validNodeName = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$`)

// Image identifies the profile's selected default image and scheduling node.
type Image struct {
	ProfileID string `json:"profileId"`
	NodeName  string `json:"nodeName"`
	Image     string `json:"image"`
}

func Validate(namespace, key string, image Image) error {
	if len(validation.IsDNS1123Label(namespace)) > 0 || !validProfileID.MatchString(key) || image.ProfileID != key ||
		!validNodeName.MatchString(image.NodeName) || !validImage.MatchString(image.Image) {
		return errors.New("invalid Cellbox warm-default image record")
	}
	return nil
}

// Read returns no targets when the ConfigMap has not yet been initialized.
// Invalid records fail closed so arbitrary recent images never warm by default.
func Read(ctx context.Context, reader client.Reader, namespace string) ([]Image, error) {
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
		return nil, errors.New("warm-default ConfigMap identity mismatch")
	}
	images := make([]Image, 0, len(cm.Data))
	for key, raw := range cm.Data {
		var image Image
		decoder := json.NewDecoder(bytes.NewBufferString(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&image); err != nil {
			return nil, fmt.Errorf("decode warm-default profile %q: %w", key, err)
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("decode warm-default profile %q: expected one JSON object", key)
		}
		if err := Validate(namespace, key, image); err != nil {
			return nil, fmt.Errorf("validate warm-default profile %q: %w", key, err)
		}
		images = append(images, image)
	}
	return images, nil
}
