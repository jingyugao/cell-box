package service

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func installSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	return recorder
}

func TestOperationAdmissionStorageRetainsRequestTrace(t *testing.T) {
	store := newTestStore(t)
	service := &Service{ctx: context.Background(), store: store}
	recorder := installSpanRecorder(t)
	ctx, parent := otel.Tracer("test").Start(context.Background(), "request")
	parentContext := parent.SpanContext()
	op, err := service.prepareOperationContext(ctx, "owner", "resume", "box-a", func(*storeTx, *Operation) error { return nil })
	parent.End()
	if err != nil || op.ID == "" || op.Kind != "resume" {
		t.Fatalf("admit: op=%+v err=%v", op, err)
	}
	for _, span := range recorder.Ended() {
		if span.SpanContext().TraceID() != parentContext.TraceID() {
			t.Fatalf("admission storage span detached from request: %s", span.Name())
		}
	}
	if len(recorder.Ended()) == 0 {
		t.Fatal("missing storage transaction span")
	}
}

func TestAsyncOperationRetainsRequestTraceAfterCancellation(t *testing.T) {
	store := newTestStore(t)
	service := &Service{ctx: context.Background(), store: store}
	b := coreBox("box-a")
	b.ClientID = "owner"
	if err := store.Write(context.Background(), func(tx *storeTx) error { return tx.PutBox(b) }); err != nil {
		t.Fatal(err)
	}
	recorder := installSpanRecorder(t)
	request, cancel := context.WithCancel(context.Background())
	request, parent := otel.Tracer("test").Start(request, "http-request")
	traceID := parent.SpanContext().TraceID()
	op, err := service.prepareOperationContext(request, "owner", "resume", "box-a", func(tx *storeTx, op *Operation) error {
		current, err := tx.Box("owner", "box-a")
		if err != nil {
			return err
		}
		current.Box.Phase = "resuming"
		current.Box.OperationID = op.ID
		return tx.PutBox(current)
	})
	if err != nil {
		t.Fatal(err)
	}
	service.launchWithCommitContext(request, op, func(ctx context.Context) (map[string]string, error) {
		return nil, nil
	}, nil)
	parent.End()
	cancel()
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	finished, err := service.waitOperation(ctx, "owner", op.ID, time.Second)
	if err != nil || finished.Status != "succeeded" {
		t.Fatalf("completion: %+v, %v", finished, err)
	}
	var asyncSpan bool
	for _, span := range recorder.Ended() {
		if span.Name() == "cellbox.operation.resume" {
			asyncSpan = true
			if span.SpanContext().TraceID() != traceID {
				t.Fatalf("async work lost request trace: %s", span.SpanContext().TraceID())
			}
		}
	}
	if !asyncSpan {
		t.Fatal("missing asynchronous operation span")
	}
}
