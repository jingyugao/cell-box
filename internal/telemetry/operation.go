package telemetry

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"context"
	"encoding/json"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"time"
)

const operationAnnotation = "cellbox.local/operation-trace"

type operationContext struct {
	Parent       string    `json:"parent"`
	Generation   int64     `json:"generation"`
	TransitionID string    `json:"transitionId,omitempty"`
	Expires      time.Time `json:"expires"`
}

// InjectOperation accompanies the same optimistic spec patch that requests work.
// Bind to the next generation so later manual edits cannot inherit stale traces.
// Baggage and tracestate are deliberately excluded from durable metadata.
func InjectOperation(ctx context.Context, w *api.ResumablePod) {
	if w.Annotations == nil {
		w.Annotations = map[string]string{}
	}
	delete(w.Annotations, operationAnnotation)
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	data, _ := json.Marshal(operationContext{Parent: carrier.Get("traceparent"), Generation: w.Generation + 1, Expires: time.Now().Add(30 * time.Minute)})
	w.Annotations[operationAnnotation] = string(data)
}

func ExtractOperation(ctx context.Context, w *api.ResumablePod) context.Context {
	var op operationContext
	raw := w.Annotations[operationAnnotation]
	if len(raw) > 512 || json.Unmarshal([]byte(raw), &op) != nil || (op.TransitionID == "" && op.Generation != w.Generation || op.TransitionID != "" && op.TransitionID != w.Spec.TransitionID) || time.Now().After(op.Expires) {
		return ctx
	}
	// Steady-state readiness refreshes must not keep appending to a finished resume.
	if w.Status.ObservedGeneration == w.Generation && w.Status.Phase == w.Spec.DesiredState {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": op.Parent})
}

func StartAt(ctx context.Context, name string, started time.Time, attributes ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer("cellbox.runtime").Start(ctx, name, trace.WithTimestamp(started), trace.WithAttributes(attributes...))
}

// ResumeOperationAnnotation binds a one-patch command to its fenced token,
// avoiding an extra resource read solely to obtain the next generation.
func ResumeOperationAnnotation(ctx context.Context, transitionID string) string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ""
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	data, _ := json.Marshal(operationContext{Parent: carrier.Get("traceparent"), TransitionID: transitionID, Expires: time.Now().Add(30 * time.Minute)})
	return string(data)
}
