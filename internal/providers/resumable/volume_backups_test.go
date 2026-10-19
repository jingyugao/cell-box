package resumable

import (
	"context"
	"errors"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestWithStoppedVolumeRequiresEveryAttachmentStopped(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	newBox := func(name, phase, desired string, podName, podUID string, mounts []api.VolumeMount) *api.ResumablePod {
		return &api.ResumablePod{ObjectMeta: meta.ObjectMeta{Namespace: "tenant", Name: name}, Spec: api.Spec{NodeName: "node-a", DesiredState: desired, Mounts: mounts}, Status: api.Status{Phase: phase, PodName: podName, PodUID: podUID}}
	}
	tests := []struct {
		name  string
		boxes []*api.ResumablePod
		want  error
	}{
		{name: "detached volume without Box"},
		{name: "suspended Box", boxes: []*api.ResumablePod{newBox("suspended", "Suspended", "Suspended", "", "", []api.VolumeMount{{UID: "vol-a"}})}},
		{name: "running phase", boxes: []*api.ResumablePod{newBox("running", "Running", "Running", "pod", "pod-uid", []api.VolumeMount{{UID: "vol-a"}})}, want: boxprovider.ErrNotReady},
		{name: "failed phase with pod", boxes: []*api.ResumablePod{newBox("failed", "Failed", "Suspended", "pod", "pod-uid", []api.VolumeMount{{UID: "vol-a"}})}, want: boxprovider.ErrNotReady},
		{name: "failed phase without pod", boxes: []*api.ResumablePod{newBox("failed", "Failed", "Suspended", "", "", []api.VolumeMount{{UID: "vol-a"}})}},
		{name: "another shared Box running", boxes: []*api.ResumablePod{
			newBox("suspended", "Suspended", "Suspended", "", "", []api.VolumeMount{{UID: "vol-a"}}),
			newBox("other", "Running", "Running", "pod", "pod-uid", []api.VolumeMount{{UID: "vol-a"}}),
		}, want: boxprovider.ErrNotReady},
		{name: "deleting Box", boxes: []*api.ResumablePod{func() *api.ResumablePod {
			b := newBox("deleting", "Suspended", "Suspended", "", "", []api.VolumeMount{{UID: "vol-a"}})
			now := meta.Now()
			b.DeletionTimestamp = &now
			b.Finalizers = []string{"test.finalizer"}
			return b
		}()}, want: boxprovider.ErrNotReady},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := make([]runtime.Object, 0, len(tt.boxes))
			for _, b := range tt.boxes {
				objs = append(objs, b)
			}
			p := New(fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build())
			called := false
			p.WithStoppedVolumeLocal = func(_ context.Context, _ boxprovider.VolumeSpec, work func(string) error) error {
				called = true
				return work("/volume")
			}
			err := p.WithStoppedVolume(context.Background(), boxprovider.VolumeSpec{UID: "vol-a", NodeName: "node-a", Namespace: "tenant"}, func(path string) error {
				if path != "/volume" {
					t.Fatalf("callback path = %q", path)
				}
				return nil
			})
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if tt.want == nil && err != nil {
				t.Fatal(err)
			}
			if called != (tt.want == nil) {
				t.Fatalf("local callback called = %t, want %t", called, tt.want == nil)
			}
		})
	}
}
