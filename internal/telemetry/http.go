package telemetry

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// HTTPHandler traces lifecycle commands. Frequent status, lease and health
// requests only propagate context; they must not bury a restore in read spans.
func HTTPHandler(next http.Handler) http.Handler {
	lifecycle := otelhttp.NewHandler(next, "cellbox.http.server")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if lifecycleRequest(r) {
			lifecycle.ServeHTTP(w, r)
			return
		}
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func lifecycleRequest(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}
	path := r.URL.Path
	if path == "/v1/boxes" || path == "/v1/boxes:restore" {
		return true
	}
	if !strings.HasPrefix(path, "/v1/boxes/") {
		return false
	}
	box, action, ok := strings.Cut(strings.TrimPrefix(path, "/v1/boxes/"), ":")
	if !ok || strings.Contains(box, "/") {
		return false
	}
	switch action {
	case "resume", "suspend", "rebuild", "destroy", "freeze", "unfreeze", "activate", "reconcile":
		return true
	}
	return false
}
