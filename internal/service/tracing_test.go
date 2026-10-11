package service

import (
	"context"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"testing"
)

// Admission commits before the async worker starts. It must still use the HTTP
// trace, otherwise the dominant object-store write appears as an empty HTTP span.
func TestResumeAdmissionStorageRetainsRequestTrace(t *testing.T) {
	objects := &ledgerObjects{}
	store := openRecords(t, objects)
	updateRecords(t, store, func(st *State) { st.Boxes["a"] = coreBox("a") })
	service := &Service{ctx: context.Background(), store: store}
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })
	ctx, parent := otel.Tracer("test").Start(context.Background(), "request")
	_, fresh, err := service.prepareOperationContext(ctx, "owner", "resume-key", "resume", "a", nil, func(*State, *Operation) error { return nil })
	parent.End()
	if err != nil || !fresh {
		t.Fatalf("admit: fresh=%v err=%v", fresh, err)
	}
	seen := map[string]bool{}
	for _, s := range recorder.Ended() {
		if s.SpanContext().TraceID() != parent.SpanContext().TraceID() {
			t.Fatalf("detached span: %s", s.Name())
		}
		seen[s.Name()] = true
	}
	for _, name := range []string{"cellbox.operation.admission", "cellbox.operation.admission_lock", "cellbox.persist.wait_writer", "cellbox.storage.put"} {
		if !seen[name] {
			t.Fatalf("missing %s", name)
		}
	}
}
