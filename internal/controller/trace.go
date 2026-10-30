package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jingyugao/cell-box/internal/telemetry"
)

// traced runs one step of a resume inside its own span, so the time between
// the request and the Running status can be attributed in the operation's trace.
func traced(ctx context.Context, name string, fn func(context.Context) error) error {
	started := time.Now()
	ctx, span := telemetry.Start(ctx, name)
	err := fn(ctx)
	telemetry.End(span, err)
	stageLog(name, started, err)
	return err
}

// stageLog prints one line per finished resume step with nanosecond timestamps
// that can be lined up with the wrapper's timings.log and the client's clock.
// It follows the sandbox.runtime_stage lines the service already prints.
func stageLog(name string, started time.Time, err error) {
	finished := time.Now()
	status := "ok"
	if err != nil {
		status = "failed"
	}
	data, _ := json.Marshal(map[string]any{"event": "controller.resume_stage", "stage": name, "status": status,
		"startNs": started.UnixNano(), "endNs": finished.UnixNano(), "durationMs": float64(finished.Sub(started).Microseconds()) / 1000})
	fmt.Printf("%s\n", data)
}
