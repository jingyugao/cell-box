package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestDetachedRetainsTraceAndServiceCancellation(t *testing.T) {
	request, cancelRequest := context.WithCancel(context.Background())
	service, cancelService := context.WithCancel(context.Background())
	defer cancelService()
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled})
	request = trace.ContextWithSpanContext(request, sc)
	background := Detached(request, service)
	cancelRequest()
	if background.Err() != nil {
		t.Fatal("request disconnect cancelled accepted work")
	}
	if got := trace.SpanContextFromContext(background); !got.Equal(sc) {
		t.Fatal("background work lost the originating trace")
	}
	cancelService()
	if background.Err() != context.Canceled {
		t.Fatal("service cancellation no longer stops background work")
	}
}
