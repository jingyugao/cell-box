package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestHTTPHandlerTracesLifecycleAndOnlyPropagatesFastRequests(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})

	type observed struct {
		spanContext trace.SpanContext
		recording   bool
	}
	var lifecycle, fast observed
	handler := HTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value := observed{spanContext: trace.SpanContextFromContext(r.Context()), recording: trace.SpanFromContext(r.Context()).IsRecording()}
		if r.URL.Path == "/v1/boxes/box-1:resume" {
			lifecycle = value
		} else {
			fast = value
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	const traceID = "0123456789abcdef0123456789abcdef"
	const parentID = "0123456789abcdef"
	request := func(method, path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, nil)
		r.Header.Set("traceparent", "00-"+traceID+"-"+parentID+"-01")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, r)
		return response
	}
	if got := request(http.MethodPost, "/v1/boxes/box-1:resume").Code; got != http.StatusNoContent {
		t.Fatalf("lifecycle status = %d", got)
	}
	if got := request(http.MethodGet, "/healthz").Code; got != http.StatusNoContent {
		t.Fatalf("health status = %d", got)
	}
	if lifecycle.spanContext.TraceID().String() != traceID || lifecycle.spanContext.SpanID().String() == parentID || !lifecycle.recording {
		t.Fatalf("lifecycle request did not create a child server span: %#v", lifecycle)
	}
	if fast.spanContext.TraceID().String() != traceID || fast.spanContext.SpanID().String() != parentID || !fast.spanContext.IsRemote() || fast.recording {
		t.Fatalf("fast request did not only propagate its incoming context: %#v", fast)
	}

	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want only the lifecycle request", len(spans))
	}
	if spans[0].Name != "cellbox.http.server" || spans[0].Parent.SpanID().String() != parentID || spans[0].SpanKind != trace.SpanKindServer {
		t.Fatalf("unexpected lifecycle span: %#v", spans[0])
	}
}
