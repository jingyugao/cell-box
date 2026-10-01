package resumable

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/inventory"
	"context"
	"errors"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// List uses namespace-scoped Kubernetes LIST requests, including pagination.
// No business ledger or per-box GET request is involved.
func (p *Provider) List(ctx context.Context, namespace, clientID string) ([]inventory.Record, error) {
	if namespace == "" || clientID == "" {
		return nil, errors.New("inventory namespace and client are required")
	}
	resources := []api.ResumablePod{}
	continuation := ""
	for {
		page := &api.ResumablePodList{}
		if err := p.Client.List(ctx, page, client.InNamespace(namespace), client.MatchingLabels{managedLabel: "true", inventory.ClientLabel: inventory.ClientValue(clientID)}, client.Limit(500), client.Continue(continuation)); err != nil {
			return nil, err
		}
		resources = append(resources, page.Items...)
		continuation = page.Continue
		if continuation == "" {
			break
		}
	}
	pods := map[string]core.Pod{}
	needPods := false
	for _, w := range resources {
		if w.Status.Phase == "Running" {
			needPods = true
		}
	}
	if needPods {
		continuation = ""
		for {
			page := &core.PodList{}
			if err := p.Client.List(ctx, page, client.InNamespace(namespace), client.HasLabels{api.OwnerLabel}, client.Limit(500), client.Continue(continuation)); err != nil {
				return nil, err
			}
			for _, pod := range page.Items {
				pods[pod.Name] = pod
			}
			continuation = page.Continue
			if continuation == "" {
				break
			}
		}
	}
	out := []inventory.Record{}
	for _, w := range resources {
		phase := "failed"
		switch w.Status.Phase {
		case "", "Creating":
			phase = "creating"
		case "Restoring":
			phase = "resuming"
		case "Suspended":
			phase = "suspended"
			if w.Spec.DesiredState == "Running" {
				phase = "resuming"
			}
		case "Checkpointing":
			phase = "checkpointing"
		case "Suspending":
			phase = "suspending"
		case "Deleting":
			phase = "deleting"
		case "Running":
			pod, ok := pods[w.Status.PodName]
			owner := meta.GetControllerOf(&pod)
			if ok && w.Status.PodUID != "" && string(pod.UID) == w.Status.PodUID && owner != nil && owner.UID == w.UID && owner.Kind == api.Kind && owner.APIVersion == api.GroupVersion.String() && pod.DeletionTimestamp == nil && pod.Status.Phase != core.PodFailed && pod.Status.Phase != core.PodSucceeded {
				phase = "running"
			}
		}
		if w.DeletionTimestamp != nil {
			phase = "deleting"
		}
		record, err := inventory.FromResource(&w, phase)
		if err != nil {
			return nil, err
		}
		if record.ClientID != clientID {
			return nil, errors.New("inventory client label mismatch")
		}
		if record.Staged && phase == "running" && w.Annotations["cellbox.local/restore-stage"] != "running" {
			stage := "restoring"
			if w.Annotations["cellbox.local/restore-stage"] == "staged" {
				stage = "staged"
			}
			record, err = inventory.FromResource(&w, stage)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, record)
	}
	return out, nil
}

func (p *Provider) SetInventoryStage(ctx context.Context, h boxprovider.Handle, stage string) error {
	w, err := p.get(ctx, h)
	if err != nil {
		return err
	}
	old := w.DeepCopy()
	if w.Annotations == nil {
		w.Annotations = map[string]string{}
	}
	w.Annotations["cellbox.local/restore-stage"] = stage
	return p.Client.Patch(ctx, w, client.MergeFrom(old))
}
