package resumable

import (
	"context"
	"errors"
	"strings"
	"testing"

	"cellbox.local/cellbox/internal/controller"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type imageCacheClient struct {
	client.Client
	onCreate func(*core.ConfigMap) error
}

func (c imageCacheClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	request := obj.(*core.ConfigMap)
	request.CreationTimestamp = meta.Now()
	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	return c.onCreate(request)
}

type cachePull func(context.Context, string) error

func (f cachePull) CacheImage(ctx context.Context, image string) error { return f(ctx, image) }

func TestCacheImageWaitsForNodeResultAndCleansRequest(t *testing.T) {
	for _, outcome := range []string{"success", "failure", "cancel"} {
		t.Run(outcome, func(t *testing.T) {
			p, spec, _ := fixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			base := p.Client
			p.Client = imageCacheClient{Client: base, onCreate: func(request *core.ConfigMap) error {
				if request.Immutable == nil || !*request.Immutable {
					t.Fatal("cache request can change during pull")
				}
				if outcome == "cancel" {
					cancel()
					return nil
				}
				r := &controller.ImageCacheReconciler{Client: base, NodeName: spec.NodeName, Runtime: cachePull(func(_ context.Context, image string) error {
					if image != spec.Image {
						t.Fatal("wrong image cached")
					}
					if outcome == "failure" {
						return errors.New("pull failed")
					}
					return nil
				})}
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(request)})
				return err
			}}
			err := p.CacheImage(ctx, spec.Image, spec.NodeName, spec.Namespace)
			if outcome == "success" && err != nil {
				t.Fatal(err)
			}
			if outcome == "failure" && (err == nil || !strings.Contains(err.Error(), "node image cache failed")) {
				t.Fatalf("cache failure not propagated: %v", err)
			}
			if outcome == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
			requests := &core.ConfigMapList{}
			if err := base.List(context.Background(), requests); err != nil || len(requests.Items) != 0 {
				t.Fatalf("cache request leaked: %v", err)
			}
		})
	}
}
