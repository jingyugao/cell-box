package resumable

import (
	"context"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/metrics"
)

// Enable only during an investigation. Events annotate existing operation spans;
// background watches and requests without a traced operation remain untouched.
func configureLatencyDiagnostics(cfg *rest.Config) {
	if os.Getenv("CELLBOX_KUBE_LATENCY_DIAGNOSTICS") != "1" {
		return
	}
	metrics.Register(metrics.RegisterOpts{RateLimiterLatency: throttleLatency{}})
	cfg.Wrap(func(next http.RoundTripper) http.RoundTripper { return latencyTransport{next} })
}

type latencyTransport struct{ next http.RoundTripper }

func (t latencyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	span := trace.SpanFromContext(r.Context())
	if r.Method != http.MethodPatch || !span.IsRecording() {
		return t.next.RoundTrip(r)
	}
	start := time.Now()
	span.AddEvent("kubernetes.transport.begin")
	callbacks := &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			span.AddEvent("kubernetes.transport.connection", trace.WithAttributes(attribute.Bool("reused", info.Reused)))
		},
		WroteRequest:         func(httptrace.WroteRequestInfo) { span.AddEvent("kubernetes.transport.written") },
		GotFirstResponseByte: func() { span.AddEvent("kubernetes.transport.first_byte") },
	}
	response, err := t.next.RoundTrip(r.WithContext(httptrace.WithClientTrace(r.Context(), callbacks)))
	span.AddEvent("kubernetes.transport.headers", trace.WithAttributes(attribute.Float64("elapsed_ms", float64(time.Since(start).Microseconds())/1000)))
	return response, err
}

type throttleLatency struct{}

func (throttleLatency) Observe(ctx context.Context, verb string, _ url.URL, latency time.Duration) {
	span := trace.SpanFromContext(ctx)
	if verb == http.MethodPatch && span.IsRecording() {
		span.AddEvent("kubernetes.client.throttle", trace.WithAttributes(attribute.Float64("elapsed_ms", float64(latency.Microseconds())/1000)))
	}
}
