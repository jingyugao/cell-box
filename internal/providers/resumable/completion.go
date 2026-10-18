package resumable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/trace"
	"os"
	"strings"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/telemetry"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const observationBuffer = 8

// WatchObservations subscribes to committed snapshots from the embedded
// controller. Separate-process watch polling is unsupported.
func (p *Provider) WatchObservations(ctx context.Context, h boxprovider.Handle) (<-chan boxprovider.Observation, error) {
	if p == nil || p.Local == nil {
		return nil, errors.New("embedded runtime control is required")
	}
	return p.watchLocalObservations(ctx, h)
}

// The embedded runtime publishes complete, committed snapshots directly. Only
// the initial subscription reads Kubernetes; completions need no watch or GET.
func (p *Provider) watchLocalObservations(ctx context.Context, h boxprovider.Handle) (<-chan boxprovider.Observation, error) {
	if err := validateHandle(h); err != nil {
		return nil, err
	}
	snapshots, stop, err := p.Local.Subscribe(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name})
	if apierrors.IsNotFound(err) {
		return nil, boxprovider.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	out := make(chan boxprovider.Observation, observationBuffer)
	go func() {
		defer stop()
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case snapshot := <-snapshots:
				deleted := snapshot.Deleted || !validObservationIdentity(snapshot.Resource, h)
				observation := boxprovider.Observation{Phase: "deleted"}
				if !deleted {
					observation = observationFromResource(snapshot.Resource, false)
				}
				select {
				case <-ctx.Done():
					return
				case out <- observation:
				}
				if deleted {
					return
				}
			}
		}
	}()
	return out, nil
}

func validObservationIdentity(w *api.ResumablePod, h boxprovider.Handle) bool {
	return w != nil && w.Namespace == h.Namespace && w.Name == h.Name && string(w.UID) == h.ID &&
		w.Labels[managedLabel] == "true" && w.Labels[boxLabel] == boxIDFromHandle(h) && w.Spec.NodeName == h.NodeName
}

func boxIDFromHandle(h boxprovider.Handle) string { return strings.TrimPrefix(h.Name, "cellbox-") }

func observationFromResource(w *api.ResumablePod, deleted bool) boxprovider.Observation {
	observation := boxprovider.Observation{
		Phase: "failed", ExecutionID: w.Status.PodUID, Message: w.Status.Message,
		TransitionID: w.Spec.TransitionID, ObservedTransitionID: w.Status.ObservedTransitionID,
		GuestReady: w.Spec.TransitionID != "" && w.Status.ResumeReadyTransitionID == w.Spec.TransitionID &&
			w.Status.ObservedTransitionID == w.Spec.TransitionID && w.Status.ObservedGeneration == w.Generation,
	}
	if w.Status.Cycle > 0 {
		observation.Generation = uint64(w.Status.Cycle)
	}
	if deleted {
		observation.Phase = "deleted"
		return observation
	}
	if w.DeletionTimestamp != nil {
		observation.Phase = "deleting"
		return observation
	}
	switch w.Status.Phase {
	case "", "Creating":
		observation.Phase, observation.ExecutionID = "creating", ""
	case "Restoring":
		observation.Phase, observation.ExecutionID = "resuming", ""
	case "Running":
		observation.Phase = "running"
	case "Checkpointing", "Suspending":
		observation.Phase = "suspending"
	case "Suspended":
		observation.Phase, observation.ExecutionID = "suspended", ""
	case "Failing", "Failed":
		observation.Phase = "failed"
	case "Deleting":
		observation.Phase = "deleting"
	}
	return observation
}

type jsonPatchOperation struct {
	Operation string `json:"op"`
	Path      string `json:"path"`
	Value     any    `json:"value,omitempty"`
}

// RequestResume uses a single conditional JSON Patch in the normal path. Empty
// previous tokens are a legacy case: read once to verify the token is absent,
// then use resourceVersion as the patch CAS because JSON Patch cannot test an
// absent optional field. A failed patch reads once only to confirm same-token
// replay after an unknown response.
func (p *Provider) RequestResume(ctx context.Context, h boxprovider.Handle, expectedPrevious, transitionID string) error {
	if transitionID == "" {
		return errors.New("resume transition ID must be non-empty")
	}
	if err := validateHandle(h); err != nil {
		return err
	}
	if p.Local != nil && p.Local.HasJournal() {
		return p.Local.RequestResume(ctx, h, expectedPrevious, transitionID)
	}
	if transitionID == expectedPrevious {
		current, err := p.get(ctx, h)
		if err != nil {
			return err
		}
		if current.Spec.TransitionID == transitionID && current.Spec.DesiredState == "Running" {
			return nil
		}
		return boxprovider.ErrStaleTransition
	}
	var legacy *api.ResumablePod
	if expectedPrevious == "" {
		w, err := p.get(ctx, h)
		if err != nil {
			return err
		}
		if w.Spec.TransitionID != "" || w.Status.ObservedTransitionID != "" || w.Spec.DesiredState != "Suspended" || w.Status.Phase != "Suspended" {
			return boxprovider.ErrStaleTransition
		}
		legacy = w
	}

	operations := []jsonPatchOperation{
		{Operation: "test", Path: "/metadata/name", Value: h.Name},
		{Operation: "test", Path: "/metadata/namespace", Value: h.Namespace},
		{Operation: "test", Path: "/metadata/uid", Value: h.ID},
		{Operation: "test", Path: "/metadata/labels/" + jsonPointerEscape(managedLabel), Value: "true"},
		{Operation: "test", Path: "/metadata/labels/" + jsonPointerEscape(boxLabel), Value: boxIDFromHandle(h)},
		{Operation: "test", Path: "/spec/nodeName", Value: h.NodeName},
		{Operation: "test", Path: "/spec/desiredState", Value: "Suspended"},
		{Operation: "test", Path: "/status/phase", Value: "Suspended"},
	}
	if expectedPrevious != "" {
		operations = append(operations,
			jsonPatchOperation{Operation: "test", Path: "/spec/transitionID", Value: expectedPrevious},
			jsonPatchOperation{Operation: "test", Path: "/status/observedTransitionID", Value: expectedPrevious})
	} else {
		operations = append(operations, jsonPatchOperation{Operation: "test", Path: "/metadata/resourceVersion", Value: legacy.ResourceVersion})
	}
	operations = append(operations,
		jsonPatchOperation{Operation: "replace", Path: "/spec/desiredState", Value: "Running"},
		jsonPatchOperation{Operation: "add", Path: "/spec/transitionID", Value: transitionID})
	traceValue := telemetry.ResumeOperationAnnotation(ctx, transitionID)
	if traceValue != "" {
		// Change only this trace key, preserving unrelated metadata.
		// Older resources without an annotations map are repaired below.
		operations = append(operations, jsonPatchOperation{Operation: "add", Path: "/metadata/annotations/cellbox.local~1operation-trace", Value: traceValue})
	}
	encoded, err := json.Marshal(operations)
	if err != nil {
		return fmt.Errorf("encode resume patch: %w", err)
	}
	obj := &api.ResumablePod{ObjectMeta: meta.ObjectMeta{Name: h.Name, Namespace: h.Namespace}}
	if os.Getenv("CELLBOX_KUBE_LATENCY_DIAGNOSTICS") == "1" {
		trace.SpanFromContext(ctx).AddEvent("kubernetes.patch.begin")
	}
	patchErr := p.Client.Patch(ctx, obj, client.RawPatch(types.JSONPatchType, encoded))
	if patchErr == nil {
		return nil
	}
	// Only this error path reads the CR to distinguish an unknown response
	// after a committed patch from a stale operation that lost the CAS.
	current, getErr := p.get(ctx, h)
	if getErr == nil && current.Spec.TransitionID == transitionID && current.Spec.DesiredState == "Running" {
		return nil
	}
	if errors.Is(getErr, boxprovider.ErrNotFound) {
		return getErr
	}
	if getErr == nil && (current.Spec.TransitionID != expectedPrevious || current.Spec.DesiredState != "Suspended" || current.Status.Phase != "Suspended" ||
		(expectedPrevious != "" && current.Status.ObservedTransitionID != expectedPrevious) ||
		(expectedPrevious == "" && current.Status.ObservedTransitionID != "")) {
		return boxprovider.ErrStaleTransition
	}
	if getErr == nil && traceValue != "" && current.Annotations == nil {
		// JSON Patch cannot add a nested key when its parent is absent.
		// Repair that legacy shape under an additional resourceVersion fence.
		operations[len(operations)-1] = jsonPatchOperation{Operation: "add", Path: "/metadata/annotations", Value: map[string]string{"cellbox.local/operation-trace": traceValue}}
		operations = append(operations, jsonPatchOperation{Operation: "test", Path: "/metadata/resourceVersion", Value: current.ResourceVersion})
		encoded, err = json.Marshal(operations)
		if err != nil {
			return err
		}
		if err = p.Client.Patch(ctx, obj, client.RawPatch(types.JSONPatchType, encoded)); err == nil {
			return nil
		}
		return err
	}
	if apierrors.IsConflict(patchErr) || apierrors.IsInvalid(patchErr) {
		return boxprovider.ErrStaleTransition
	}
	return patchErr
}

func validateHandle(h boxprovider.Handle) error {
	if h.Provider != name || h.ID == "" || h.Name == "" || h.Namespace == "" || h.NodeName == "" || !strings.HasPrefix(h.Name, "cellbox-") || boxIDFromHandle(h) == "" {
		return errors.New("invalid resumable provider handle")
	}
	return nil
}

func jsonPointerEscape(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}
