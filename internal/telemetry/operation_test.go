package telemetry

import (
	"context"
	"encoding/json"
	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"go.opentelemetry.io/otel/trace"
	"testing"
	"time"
)

func TestOperationPropagationRejectsStaleAndSettledWork(t *testing.T) {
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	w := &api.ResumablePod{}
	w.Generation = 3
	w.Spec.DesiredState = "Running"
	w.Status.Phase = "Suspended"
	InjectOperation(ctx, w)
	w.Generation++
	got := trace.SpanContextFromContext(ExtractOperation(context.Background(), w))
	if got.TraceID() != sc.TraceID() || got.SpanID() != sc.SpanID() || !got.IsRemote() {
		t.Fatal("lost asynchronous operation parent")
	}
	tests := map[string]func(*api.ResumablePod){
		"new generation": func(w *api.ResumablePod) { w.Generation++ },
		"settled":        func(w *api.ResumablePod) { w.Status.ObservedGeneration = w.Generation; w.Status.Phase = "Running" },
		"expired": func(w *api.ResumablePod) {
			var op operationContext
			_ = json.Unmarshal([]byte(w.Annotations[operationAnnotation]), &op)
			op.Expires = time.Now().Add(-time.Second)
			b, _ := json.Marshal(op)
			w.Annotations[operationAnnotation] = string(b)
		},
		"malformed":            func(w *api.ResumablePod) { w.Annotations[operationAnnotation] = "bad" },
		"untraced next action": func(w *api.ResumablePod) { InjectOperation(context.Background(), w) },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			copy := w.DeepCopy()
			change(copy)
			if trace.SpanContextFromContext(ExtractOperation(context.Background(), copy)).IsValid() {
				t.Fatal("attached stale trace")
			}
		})
	}
}

func TestResumeTraceIsBoundToTransition(t *testing.T) {
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	w := &api.ResumablePod{}
	w.Spec.TransitionID = "resume-one"
	w.Spec.DesiredState = "Running"
	w.Status.Phase = "Restoring"
	w.Annotations = map[string]string{operationAnnotation: ResumeOperationAnnotation(ctx, "resume-one")}
	if trace.SpanContextFromContext(ExtractOperation(context.Background(), w)).TraceID() != sc.TraceID() {
		t.Fatal("lost resume parent")
	}
	w.Spec.TransitionID = "resume-two"
	if trace.SpanContextFromContext(ExtractOperation(context.Background(), w)).IsValid() {
		t.Fatal("attached trace to different transition")
	}
}
