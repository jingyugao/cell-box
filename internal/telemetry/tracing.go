package telemetry

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Initialize is opt-in. Export runs in the background and never gates a Box action.
func Initialize(ctx context.Context) (func(context.Context) error, error) {
	if os.Getenv("OTEL_SDK_DISABLED") == "true" ||
		(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "") {
		return func(context.Context) error { return nil }, nil
	}
	protocol := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
	if protocol == "" {
		protocol = os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
	}
	if protocol != "" && protocol != "http/protobuf" {
		return nil, fmt.Errorf("tracing requires OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf")
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("initialize trace exporter: %w", err)
	}
	name := os.Getenv("OTEL_SERVICE_NAME")
	if name == "" {
		name = "cellbox-api"
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", name))),
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second), sdktrace.WithExportTimeout(5*time.Second), sdktrace.WithMaxQueueSize(4096)),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return provider.Shutdown, nil
}

func Start(ctx context.Context, name string, attributes ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer("cellbox.runtime").Start(ctx, name, trace.WithAttributes(attributes...))
}

func String(key, value string) attribute.KeyValue { return attribute.String(key, value) }

// Report failure without exporting error messages that can contain credentials.
func End(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, "")
	}
	span.End()
}

// Retain causality, but keep the service's cancellation and deadline semantics.
func Detached(request, service context.Context) context.Context {
	return trace.ContextWithSpanContext(service, trace.SpanContextFromContext(request))
}
