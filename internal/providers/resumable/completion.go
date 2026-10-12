package resumable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/telemetry"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const observationBuffer = 8

// WatchObservations establishes a watch at the resourceVersion of a validated
// initial read. Each event is mapped directly from its CR payload; event
// processing never reads the CR or Pod again. Reconnects take one fresh CR
// snapshot and resume watching from that version.
func (p *Provider) WatchObservations(ctx context.Context, h boxprovider.Handle) (<-chan boxprovider.Observation, error) {
	c, ok := p.Client.(client.WithWatch)
	if !ok {
		return nil, errors.New("Kubernetes client does not support watches")
	}
	initial, stream, err := p.openObservationWatch(ctx, c, h)
	if err != nil {
		return nil, err
	}
	out := make(chan boxprovider.Observation, observationBuffer)
	go p.runObservationWatch(ctx, c, h, out, initial, stream)
	return out, nil
}

func (p *Provider) openObservationWatch(ctx context.Context, c client.WithWatch, h boxprovider.Handle) (boxprovider.Observation, watch.Interface, error) {
	w, err := p.get(ctx, h)
	if err != nil {
		return boxprovider.Observation{}, nil, err
	}
	stream, err := c.Watch(ctx, &api.ResumablePodList{}, &client.ListOptions{Namespace: h.Namespace,
		Raw: &meta.ListOptions{ResourceVersion: w.ResourceVersion, FieldSelector: fields.OneTermEqualSelector("metadata.name", h.Name).String()}})
	if err != nil {
		return boxprovider.Observation{}, nil, err
	}
	return observationFromResource(w, false), stream, nil
}

func (p *Provider) runObservationWatch(ctx context.Context, c client.WithWatch, h boxprovider.Handle, out chan<- boxprovider.Observation,
	initial boxprovider.Observation, stream watch.Interface) {
	defer close(out)
	defer func() {
		if stream != nil {
			stream.Stop()
		}
	}()
	emit := func(observation boxprovider.Observation) bool {
		select {
		case out <- observation:
			return true
		case <-ctx.Done():
			return false
		}
	}
	if !emit(initial) {
		return
	}
	reconnectDelay := 100 * time.Millisecond
	for ctx.Err() == nil {
		if stream == nil {
			if !waitWatchRetry(ctx, reconnectDelay) {
				return
			}
			reconnectDelay = min(reconnectDelay*2, 5*time.Second)
			var err error
			initial, stream, err = p.openObservationWatch(ctx, c, h)
			if err != nil {
				if errors.Is(err, boxprovider.ErrNotFound) {
					emit(boxprovider.Observation{Phase: "deleted"})
					return
				}
				continue
			}
			reconnectDelay = 100 * time.Millisecond
			if !emit(initial) {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case event, ok := <-stream.ResultChan():
			if !ok || event.Type == watch.Error {
				stream.Stop()
				stream = nil

				continue
			}
			if event.Type == watch.Bookmark {
				continue
			}
			resource, ok := event.Object.(*api.ResumablePod)
			if !ok || !validObservationIdentity(resource, h) {
				// A same-name CR with a different UID is a new sandbox. Stop this
				// handle's stream instead of publishing another execution's state.
				emit(boxprovider.Observation{Phase: "deleted"})
				return
			}
			if event.Type == watch.Deleted {
				observation := observationFromResource(resource, true)
				observation.Phase = "deleted"
				if !emit(observation) {
					return
				}
				return
			}
			if event.Type != watch.Added && event.Type != watch.Modified {
				continue
			}
			if !emit(observationFromResource(resource, false)) {
				return
			}
		}
	}
}

func waitWatchRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
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
	if w.Spec.Upgrade != nil && w.Spec.Upgrade.Nonce != w.Status.UpgradeNonce {
		observation.Phase, observation.ExecutionID = "creating", ""
		return observation
	}
	if w.Status.Phase == "Failed" && w.Spec.PersistentHome && w.Spec.DesiredState == "Running" &&
		w.Spec.RebuildNonce != "" && w.Spec.RebuildNonce != w.Status.RebuildNonce {
		observation.Phase, observation.ExecutionID = "creating", ""
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
