package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"context"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
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
	for _, name := range []string{"cellbox.operation.admission", "cellbox.storage.write_object"} {
		if !seen[name] {
			t.Fatalf("missing %s", name)
		}
	}
}

func TestResumeTotalSurvivesRequestAndContainsAdmissionAndCompletion(t *testing.T) {
	f := newCoreFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/unquiesce" {
			close(entered)
			<-release
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer guest.Close()
	defer once.Do(func() { close(release) })
	f.provider.mu.Lock()
	f.provider.created = true
	f.provider.guestURL = guest.URL
	f.provider.mu.Unlock()
	f.service.providers["resumable-k8s-pod"] = f.provider
	profile := f.config.Profiles[0]
	profile.Provider = "resumable-k8s-pod"
	if err := f.service.store.Update(func(st *State) error {
		st.Boxes["box-total"] = boxRecord{Box: Box{ID: "box-total", Phase: "suspended", Generation: 1}, ClientID: "client-a", Profile: profile, Handle: boxprovider.Handle{Provider: profile.Provider, ID: "runtime-total"}, ExecutionID: "old"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) }()
	request, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, parent := otel.Tracer("test").Start(request, "http")
	op, err := f.service.actionContext(request, "client-a", "total-key", "box-total", "resume")
	if err != nil {
		t.Fatal(err)
	}
	parent.End()
	cancel()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request cancellation stopped accepted resume")
	}
	for _, span := range recorder.Ended() {
		if span.Name() == "cellbox.resume.total" {
			t.Fatal("total ended at HTTP acceptance")
		}
	}
	// Reusing the key must not create a second full restore or end the first span.
	if _, err = f.service.actionContext(context.Background(), "client-a", "total-key", "box-total", "resume"); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	f.waitOperation(t, op.ID, "succeeded")
	if err = f.service.Close(); err != nil {
		t.Fatal(err)
	}
	spans := map[string]sdktrace.ReadOnlySpan{}
	totals := 0
	for _, span := range recorder.Ended() {
		spans[span.Name()] = span
		if span.Name() == "cellbox.resume.total" {
			totals++
		}
	}
	total := spans["cellbox.resume.total"]
	if total == nil || totals != 1 {
		t.Fatalf("total count %d", totals)
	}
	if spans["cellbox.resume.replay"] == nil {
		t.Fatal("missing idempotent replay span")
	}
	for _, name := range []string{"cellbox.operation.admission", "cellbox.runtime.transition", "cellbox.unquiesce", "cellbox.persist_completion"} {
		var child sdktrace.ReadOnlySpan
		for _, s := range recorder.Ended() {
			if s.Name() == name && s.SpanContext().TraceID() == total.SpanContext().TraceID() {
				child = s
				break
			}
		}
		if child == nil || child.StartTime().Before(total.StartTime()) || child.EndTime().After(total.EndTime()) {
			t.Fatalf("total does not contain %s", name)
		}
		if child.Parent().SpanID() != total.SpanContext().SpanID() {
			t.Fatalf("wrong parent for %s", name)
		}
	}
}
