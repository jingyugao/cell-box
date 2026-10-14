// SPDX-License-Identifier: Apache-2.0

package resumable

import (
	"context"
	"strings"
	"testing"

	"cellbox.local/cellbox/internal/boxprovider"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestWarmDefaultImageConfigMapUpsertAndRead(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := core.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	p := New(fake.NewClientBuilder().WithScheme(scheme).Build())
	ctx := context.Background()
	missing, err := p.GetWarmDefaultImages(ctx, "cellbox")
	if err != nil || len(missing) != 0 {
		t.Fatalf("missing ConfigMap should return no targets: %v, %v", missing, err)
	}
	one := boxprovider.WarmDefaultImage{ProfileID: "default", NodeName: "node-a", Image: "registry.example/cellbox@sha256:" + strings.Repeat("a", 64)}
	two := boxprovider.WarmDefaultImage{ProfileID: "test", NodeName: "node-b", Image: "registry.example/cellbox@sha256:" + strings.Repeat("b", 64)}
	if err := p.SetWarmDefaultImage(ctx, "cellbox", one); err != nil {
		t.Fatal(err)
	}
	if err := p.SetWarmDefaultImage(ctx, "cellbox", two); err != nil {
		t.Fatal(err)
	}
	got, err := p.GetWarmDefaultImages(ctx, "cellbox")
	if err != nil || len(got) != 2 {
		t.Fatalf("read targets = %v, %v", got, err)
	}
	found := map[string]boxprovider.WarmDefaultImage{}
	for _, item := range got {
		found[item.ProfileID] = item
	}
	if found[one.ProfileID] != one || found[two.ProfileID] != two {
		t.Fatalf("upsert did not preserve all profile records: %v", got)
	}
	replacement := boxprovider.WarmDefaultImage{ProfileID: "default", NodeName: "node-c", Image: "registry.example/cellbox@sha256:" + strings.Repeat("c", 64)}
	if err = p.SetWarmDefaultImage(ctx, "cellbox", replacement); err != nil {
		t.Fatal(err)
	}
	got, err = p.GetWarmDefaultImages(ctx, "cellbox")
	if err != nil || len(got) != 2 {
		t.Fatalf("replacement targets = %v, %v", got, err)
	}
	found = map[string]boxprovider.WarmDefaultImage{}
	for _, item := range got {
		found[item.ProfileID] = item
	}
	if found[replacement.ProfileID] != replacement || found[two.ProfileID] != two {
		t.Fatalf("replacement overwrote another profile: %v", got)
	}
}

func TestWarmDefaultImageRejectsMutableImageAndInvalidIdentity(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := core.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	p := New(fake.NewClientBuilder().WithScheme(scheme).Build())
	for _, input := range []boxprovider.WarmDefaultImage{
		{ProfileID: "", NodeName: "node-a", Image: "registry.example/cellbox@sha256:" + strings.Repeat("a", 64)},
		{ProfileID: "default", NodeName: "node-a", Image: "registry.example/cellbox:latest"},
		{ProfileID: "default", NodeName: "node/a", Image: "registry.example/cellbox@sha256:" + strings.Repeat("a", 64)},
	} {
		if err := p.SetWarmDefaultImage(context.Background(), "cellbox", input); err == nil {
			t.Fatalf("accepted invalid target %+v", input)
		}
	}
}
