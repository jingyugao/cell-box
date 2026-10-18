// SPDX-License-Identifier: Apache-2.0

package warmtemplates

import (
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	core "k8s.io/api/core/v1"
)

func TestTemplateValidationAndStableHash(t *testing.T) {
	valid := Template{ID: "revision-one", Slots: 2, Spec: api.Spec{
		NodeName: "node-a", DesiredState: "Running", PersistentHome: true,
		Container: core.Container{Name: "cellbox", Image: "image"},
	}}
	if err := Validate("cellbox", valid.ID, valid); err != nil {
		t.Fatal(err)
	}
	if err := Validate("cellbox", "other", valid); err == nil {
		t.Fatal("mismatched template key was accepted")
	}
	if err := Validate("cellbox", valid.ID, Template{ID: valid.ID, Slots: -1, Spec: valid.Spec}); err == nil {
		t.Fatal("negative slot count was accepted")
	}
	first, err := HashTemplates([]Template{valid, {ID: "revision-two", Slots: 1, Spec: valid.Spec}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := HashTemplates([]Template{{ID: "revision-two", Slots: 1, Spec: valid.Spec}, valid})
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("row order changed the warm config hash: %s != %s", first, second)
	}
	changed, err := HashTemplates([]Template{{ID: valid.ID, Slots: 3, Spec: valid.Spec}})
	if err != nil {
		t.Fatal(err)
	}
	if changed == first {
		t.Fatal("slot change did not change the warm config hash")
	}
}
