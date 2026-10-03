package resumable

import (
	"errors"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestGuestConnectionFencesExecutionAndContainerRestarts(t *testing.T) {
	p, spec, ctx := fixture(t)
	h, err := p.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{}
	if err := p.Client.Get(ctx, client.ObjectKeyFromObject(&api.ResumablePod{ObjectMeta: meta.ObjectMeta{Name: h.Name, Namespace: h.Namespace}}), w); err != nil {
		t.Fatal(err)
	}
	w.Status.Phase, w.Status.PodName, w.Status.PodUID = "Running", "pod-a", "pod-a-uid"
	if err := p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	pod := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "pod-a", Namespace: h.Namespace, UID: "pod-a-uid", OwnerReferences: []meta.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: api.Kind, Name: w.Name, UID: w.UID, Controller: boolPtr(true)}}},
		Status: core.PodStatus{Phase: core.PodRunning, Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue}}, ContainerStatuses: []core.ContainerStatus{{Name: w.Spec.Container.Name, ContainerID: "containerd://one", State: core.ContainerState{Running: &core.ContainerStateRunning{StartedAt: meta.Now()}}}}}}
	if err := p.Client.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	svc := &core.Service{ObjectMeta: meta.ObjectMeta{Name: w.Name, Namespace: w.Namespace, OwnerReferences: pod.OwnerReferences}, Spec: core.ServiceSpec{Selector: map[string]string{api.OwnerLabel: string(w.UID), api.ServingLabel: "true"}, Ports: w.Spec.ServicePorts}}
	if err := p.Client.Create(ctx, svc); err != nil {
		t.Fatal(err)
	}
	if _, err := p.GuestForExecution(ctx, h, "old-pod"); !errors.Is(err, boxprovider.ErrStaleExecution) {
		t.Fatal("stale execution accepted")
	}
	for i := 0; i < 2; i++ {
		if _, err := p.GuestForExecution(ctx, h, "pod-a-uid"); err != nil {
			t.Fatal(err)
		}
	}
	pod.Status.ContainerStatuses[0].ContainerID = "containerd://two"
	pod.Status.ContainerStatuses[0].RestartCount++
	if err := p.Client.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if _, err := p.GuestForExecution(ctx, h, "pod-a-uid"); err != nil {
		t.Fatal(err)
	}
	pod.Status.ContainerStatuses[0].ContainerID = "containerd://three"
	if err := p.Client.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	originalClient := p.Client
	p.Client = podGetHookClient{Client: originalClient, afterPodGet: func() {
		p.Client = originalClient
		pod.Status.ContainerStatuses[0].ContainerID = "containerd://four"
		if err := p.Client.Status().Update(ctx, pod); err != nil {
			t.Fatal(err)
		}
	}}
	if _, err := p.GuestForExecution(ctx, h, "pod-a-uid"); !errors.Is(err, boxprovider.ErrStaleExecution) {
		t.Fatalf("container changed during connection resolution: %v", err)
	}
}
