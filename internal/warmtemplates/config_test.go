// SPDX-License-Identifier: Apache-2.0

package warmtemplates

import (
	"context"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReadWarmTemplatesMissingAndRejectsInvalidRecords(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := core.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	reader := fake.NewClientBuilder().WithScheme(scheme).Build()
	got, err := Read(context.Background(), reader, "cellbox")
	if err != nil || len(got) != 0 {
		t.Fatalf("missing template config should mean no warm targets: %#v err=%v", got, err)
	}
	for _, record := range []string{
		`{"id":"other","slots":1,"spec":{"nodeName":"node","desiredState":"Running","container":{"name":"c","image":"i"}}}`,
		`{"id":"tpl-one","slots":-1,"spec":{"nodeName":"node","desiredState":"Running","container":{"name":"c","image":"i"}}}`,
		`{"id":"tpl-one","slots":1,"spec":{"nodeName":"","desiredState":"Running","container":{"name":"c","image":"i"}}}`,
		`{"id":"tpl-one","slots":1,"spec":{"nodeName":"node","desiredState":"Running","container":{"name":"c","image":"i"}},"unexpected":true}`,
	} {
		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&core.ConfigMap{
			ObjectMeta: meta.ObjectMeta{Namespace: "cellbox", Name: ConfigMapName},
			Data:       map[string]string{"tpl-one": record},
		}).Build()
		if _, err := Read(context.Background(), client, "cellbox"); err == nil {
			t.Fatalf("invalid template record accepted: %s", record)
		}
	}
}

func TestValidateTemplateAllowsPersistentHomeCompatibilityWithoutBindingIt(t *testing.T) {
	template := Template{ID: "tpl-one", Slots: 2, Spec: api.Spec{
		NodeName: "node-a", DesiredState: "Running", PersistentHome: true,
		Container: core.Container{Name: "cellbox", Image: "image"},
	}}
	if err := Validate("cellbox", template.ID, template); err != nil {
		t.Fatalf("persistentHome is part of resume identity while warm Pod generation strips the HOME mount: %v", err)
	}
}
