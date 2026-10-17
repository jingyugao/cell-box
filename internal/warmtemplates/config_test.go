// SPDX-License-Identifier: Apache-2.0

package warmtemplates

import (
	"context"
	"sync"
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
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

func TestLocalStoreIsolatesNamespacesCopiesAndInvalidatesSnapshots(t *testing.T) {
	store := NewLocalStore()
	ch := store.Changes("cellbox")
	if _, _, _, initialized := store.Read("cellbox"); initialized {
		t.Fatal("new store must distinguish uninitialized from empty configuration")
	}
	template := Template{ID: "rev-one", Slots: 1, Spec: api.Spec{NodeName: "node-a", DesiredState: "Running", Container: core.Container{Name: "cellbox", Image: "image", Env: []core.EnvVar{{Name: "K", Value: "before"}}}}}
	if err := store.Set("cellbox", []Template{template}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	default:
		t.Fatal("configuration update did not wake controller")
	}
	templates, hash, generation, initialized := store.Read("cellbox")
	if !initialized || hash == "" || generation != 1 || len(templates) != 1 {
		t.Fatalf("initial config = %#v %q %d %v", templates, hash, generation, initialized)
	}
	templates[0].Spec.Container.Env[0].Value = "mutated read"
	template.Spec.Container.Env[0].Value = "mutated input"
	stored, storedHash, storedGeneration, _ := store.Read("cellbox")
	if stored[0].Spec.Container.Env[0].Value != "before" || storedHash != hash || storedGeneration != generation {
		t.Fatal("store leaked mutable template data")
	}
	if err := store.Set("other", nil); err != nil {
		t.Fatal(err)
	}
	if got, _, _, ok := store.Read("other"); !ok || len(got) != 0 {
		t.Fatalf("other namespace empty config = %#v initialized=%v", got, ok)
	}
	if _, _, _, ok := store.Read("cellbox"); !ok {
		t.Fatal("namespace configuration leaked across namespaces")
	}
	snapshot := Snapshot{NodeName: "node-a", DesiredHash: hash, ObservedGeneration: generation, ObservedAt: time.Now(), Pools: []boxprovider.WarmPoolObservation{{PoolID: "pool", TemplateIDs: []string{"rev-one"}, ReadySlots: 1}}}
	store.Publish("cellbox", snapshot)
	if got, ok := store.Snapshot("cellbox", "node-a"); !ok || len(got.Pools) != 1 {
		t.Fatalf("published snapshot unavailable: %#v %v", got, ok)
	}
	if err := store.Set("cellbox", []Template{{ID: "rev-one", Slots: 2, Spec: template.Spec}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Snapshot("cellbox", "node-a"); ok {
		t.Fatal("configuration change retained prior snapshot")
	}
	if _, newHash, newGeneration, _ := store.Read("cellbox"); newHash == hash || newGeneration != generation+1 {
		t.Fatalf("changed config hash/generation = %s/%d", newHash, newGeneration)
	}
}

func TestLocalStoreConcurrentSetReadAndPublish(t *testing.T) {
	store := NewLocalStore()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				template := Template{ID: "rev-one", Slots: (i + j) % 3, Spec: api.Spec{NodeName: "node-a", DesiredState: "Running", Container: core.Container{Name: "cellbox", Image: "image"}}}
				if err := store.Set("cellbox", []Template{template}); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
				_, hash, generation, initialized := store.Read("cellbox")
				if !initialized || hash == "" || generation < 1 {
					t.Errorf("invalid concurrent read: %q %d %v", hash, generation, initialized)
					return
				}
				store.Publish("cellbox", Snapshot{NodeName: "node-a", DesiredHash: hash, ObservedGeneration: generation, ObservedAt: time.Now()})
			}
		}(i)
	}
	wg.Wait()
}

func TestZeroValueLocalStorePreservesChangesSubscription(t *testing.T) {
	var store LocalStore
	changes := store.Changes("cellbox")
	template := Template{ID: "rev-one", Slots: 1, Spec: api.Spec{NodeName: "node-a", DesiredState: "Running", Container: core.Container{Name: "cellbox", Image: "image"}}}
	if err := store.Set("cellbox", []Template{template}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changes:
	case <-time.After(time.Second):
		t.Fatal("Set discarded the Changes subscription created on the zero-value store")
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
