// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestCellboxGVKUsesInternalResumablePodType(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{Kind, Kind + "List"} {
		obj, err := scheme.New(GroupVersion.WithKind(kind))
		if err != nil {
			t.Fatal(err)
		}
		switch kind {
		case Kind:
			if _, ok := obj.(*ResumablePod); !ok {
				t.Fatalf("%s registered as %T", kind, obj)
			}
		default:
			if _, ok := obj.(*ResumablePodList); !ok {
				t.Fatalf("%s registered as %T", kind, obj)
			}
		}
	}
}
