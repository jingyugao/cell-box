package resumable

import (
	"context"
	"testing"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/telemetry"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestResumePropagatesRuntimeTransitionAsControllerParent(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(provider)
	defer func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	}()

	p, spec, ctx := fixture(t)
	h, err := p.Create(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	w := &api.ResumablePod{}
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	w.Generation = 7
	w.Spec.DesiredState = "Suspended"
	w.Status.Phase = "Suspended"
	if err := p.Client.Update(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := p.Client.Status().Update(ctx, w); err != nil {
		t.Fatal(err)
	}

	ctx, transition := telemetry.Start(ctx, "cellbox.runtime.transition")
	wantParent := trace.SpanContextFromContext(ctx)
	if err := p.Action(ctx, h, "resume"); err != nil {
		t.Fatal(err)
	}
	transition.End()

	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: h.Namespace, Name: h.Name}, w); err != nil {
		t.Fatal(err)
	}
	// The fake Kubernetes client does not advance metadata.generation when the
	// desired-state spec changes; the controller will observe the next generation.
	w.Generation++
	controllerCtx := telemetry.ExtractOperation(context.Background(), w)
	gotParent := trace.SpanContextFromContext(controllerCtx)
	if !gotParent.IsValid() || gotParent.TraceID() != wantParent.TraceID() || gotParent.SpanID() != wantParent.SpanID() || !gotParent.IsRemote() {
		t.Fatalf("controller context parent = %#v, want runtime.transition %s/%s", gotParent, wantParent.TraceID(), wantParent.SpanID())
	}

	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Name != "cellbox.runtime.transition" {
		t.Fatalf("resume dispatch created an extra or orphan span: %#v", spans)
	}
	if w.Spec.DesiredState != "Running" || w.Annotations["cellbox.jingyugao.github.io/operation-trace"] == "" {
		t.Fatal("resume did not patch desired state with its operation trace")
	}
}
