// SPDX-License-Identifier: Apache-2.0

package resumable

import (
	"context"
	"errors"
	"testing"
)

func TestCacheImageRequiresEmbeddedRuntime(t *testing.T) {
	_, spec, _ := fixture(t)
	provider := &Provider{}
	if err := provider.CacheImage(context.Background(), spec.Image, spec.NodeName, spec.Namespace); err == nil {
		t.Fatal("image cache request without embedded runtime succeeded")
	}
	want := errors.New("pull failed")
	called := false
	provider.CacheImageLocal = func(context.Context, string, string, string) error { called = true; return want }
	if err := provider.CacheImage(context.Background(), spec.Image, spec.NodeName, spec.Namespace); !errors.Is(err, want) || !called {
		t.Fatalf("embedded image cache error = %v, called=%v", err, called)
	}
}
