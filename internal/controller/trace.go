package controller

import (
	"context"

	"github.com/jingyugao/cell-box/internal/telemetry"
)

// traced runs one step of a resume inside its own span, so the time between
// the request and the Running status can be attributed in the operation's trace.
func traced(ctx context.Context, name string, fn func(context.Context) error) error {
	ctx, span := telemetry.Start(ctx, name)
	err := fn(ctx)
	telemetry.End(span, err)
	return err
}
