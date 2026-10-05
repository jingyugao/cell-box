package controller

import (
	"context"
	"fmt"
	"time"

	"cellbox.local/cellbox/internal/imagecache"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ImagePuller interface {
	CacheImage(context.Context, string) error
}

// ImageCacheReconciler uses CRI directly; it never runs the imported image.
type ImageCacheReconciler struct {
	client.Client
	Runtime  ImagePuller
	NodeName string
}

func (r *ImageCacheReconciler) Reconcile(ctx context.Context, key ctrl.Request) (ctrl.Result, error) {
	request := &core.ConfigMap{}
	if err := r.Get(ctx, key.NamespacedName, request); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if request.Labels[imagecache.Label] != "true" || request.Data["node"] != r.NodeName {
		return ctrl.Result{}, nil
	}
	// Remove abandoned requests after an interrupted API import, including ones
	// whose pull completed before the API process could delete them.
	if time.Since(request.CreationTimestamp.Time) > 30*time.Minute {
		return ctrl.Result{}, client.IgnoreNotFound(r.Delete(ctx, request))
	}
	if request.Annotations[imagecache.Status] != "" {
		return ctrl.Result{RequeueAfter: 30 * time.Minute}, nil
	}
	pull, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	err := fmt.Errorf("immutable image reference is required")
	if pullableImage.MatchString(request.Data["image"]) {
		err = r.Runtime.CacheImage(pull, request.Data["image"])
	}
	// Re-read before publishing the outcome: imports can be cancelled while a
	// pull is in progress. Never recreate a deleted request or update a new UID.
	current := &core.ConfigMap{}
	if readErr := r.Get(ctx, key.NamespacedName, current); readErr != nil {
		if apierrors.IsNotFound(readErr) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, readErr
	}
	if current.UID != request.UID || current.Data["image"] != request.Data["image"] || current.Data["node"] != request.Data["node"] {
		return ctrl.Result{}, nil
	}
	old := current.DeepCopy()
	if current.Annotations == nil {
		current.Annotations = map[string]string{}
	}
	current.Annotations[imagecache.Status] = "succeeded"
	if err != nil {
		current.Annotations[imagecache.Status] = "failed"
		current.Annotations[imagecache.Message] = "Cannot cache prepared image on runtime node; check CRI and registry access"
		ctrl.LoggerFrom(ctx).Error(err, "image cache failed", "node", r.NodeName)
	}
	return ctrl.Result{RequeueAfter: 30 * time.Minute}, r.Patch(ctx, current, client.MergeFrom(old))
}
