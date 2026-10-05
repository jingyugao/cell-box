package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cellbox.local/cellbox/internal/imagecache"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type pullFunc func(context.Context, string) error

func (f pullFunc) CacheImage(ctx context.Context, image string) error { return f(ctx, image) }

func TestImageCacheIsNodeScopedAndNeverCreatesPods(t *testing.T) {
	for _, outcome := range []string{"succeeded", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = core.AddToScheme(scheme)
			request := &core.ConfigMap{ObjectMeta: meta.ObjectMeta{Name: "cache", Namespace: "boxes", CreationTimestamp: meta.Now(), Labels: map[string]string{imagecache.Label: "true"}}, Data: map[string]string{"node": "node-a", "image": "registry/prepared@sha256:" + strings.Repeat("a", 64)}}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(request).Build()
			calls := 0
			r := &ImageCacheReconciler{Client: c, NodeName: "node-b", Runtime: pullFunc(func(_ context.Context, image string) error {
				calls++
				if image != request.Data["image"] {
					t.Fatal("wrong image pulled")
				}
				if outcome == "failed" {
					return errors.New("registry unavailable")
				}
				return nil
			})}
			key := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(request)}
			if _, err := r.Reconcile(context.Background(), key); err != nil || calls != 0 {
				t.Fatalf("wrong node pulled image: %v", err)
			}
			r.NodeName = "node-a"
			if _, err := r.Reconcile(context.Background(), key); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(context.Background(), key.NamespacedName, request); err != nil {
				t.Fatal(err)
			}
			if request.Annotations[imagecache.Status] != outcome {
				t.Fatalf("wrong cache outcome: %+v", request.Annotations)
			}
			if _, err := r.Reconcile(context.Background(), key); err != nil || calls != 1 {
				t.Fatalf("completed pull repeated: %v", err)
			}
			pods := &core.PodList{}
			if err := c.List(context.Background(), pods); err != nil || len(pods.Items) != 0 {
				t.Fatalf("image cache created workloads: %v", err)
			}
		})
	}
}
