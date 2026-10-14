// SPDX-License-Identifier: Apache-2.0

package warmdefaults

import (
	"context"
	"strings"
	"testing"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReadMissingAndValidWarmDefaultConfig(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := core.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).Build()
	images, err := Read(context.Background(), reader, "cellbox")
	if err != nil || len(images) != 0 {
		t.Fatalf("missing target should disable the pool: images=%v err=%v", images, err)
	}
	digest := strings.Repeat("a", 64)
	cm := &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Namespace: "cellbox", Name: ConfigMapName}, Data: map[string]string{
		"default": `{"profileId":"default","nodeName":"node-1","image":"registry.example/cellbox@sha256:` + digest + `"}`,
	}}
	if err = reader.Create(context.Background(), cm); err != nil {
		t.Fatal(err)
	}
	images, err = Read(context.Background(), reader, "cellbox")
	if err != nil || len(images) != 1 || images[0] != (Image{ProfileID: "default", NodeName: "node-1", Image: "registry.example/cellbox@sha256:" + digest}) {
		t.Fatalf("valid target = %v, %v", images, err)
	}
}

func TestReadRejectsMismatchedOrInvalidTargetRecords(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := core.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, record := range []string{
		`{"profileId":"other","nodeName":"node-1","image":"registry.example/cellbox@sha256:` + strings.Repeat("a", 64) + `"}`,
		`{"profileId":"default","nodeName":"node-1","image":"registry.example/cellbox:latest"}`,
		`{"profileId":"default","nodeName":"node-1","image":"registry.example/cellbox@sha256:` + strings.Repeat("a", 64) + `","fallback":"unsafe"}`,
	} {
		reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&core.ConfigMap{
			ObjectMeta: meta.ObjectMeta{Namespace: "cellbox", Name: ConfigMapName}, Data: map[string]string{"default": record},
		}).Build()
		if _, err := Read(context.Background(), reader, "cellbox"); err == nil {
			t.Fatalf("accepted invalid warm-default record %s", record)
		}
	}
}
