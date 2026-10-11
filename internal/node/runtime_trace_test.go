// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// This is a sanitized excerpt of the INFO-level timing table emitted by
// runsc's gVisor timing.Timer.Log. It retains the real table layout while
// excluding unrelated runtime log lines.
const actualRestoreTimingExcerpt = `I1011 03:21:43.692118       1 timing.go:866] ---- Restore timing information ----
[03:20:59.164343]	                      	╭─ Restore
[03:21:43.497310]	+44.332s ▉▉▉▉▉▉▉▉▉▉▉▉	├─ cm.Restore RPC: 44.332s
[03:21:43.501494]	 +0.000s             	├─ restore cont 1/2: 0.000s
[03:21:43.631710]	 +0.068s             	├─ restore cont 2/2: 0.130s
[03:21:43.638609]	 +0.006s             	├─ loading kernel: 0.006s
[03:21:43.678823]	 +0.000s             	├─ kernel loaded: 0.040s
[03:21:43.678923]	 +0.000s             	├─ Starting sandbox: 0.000s
[03:21:43.691999]	 +0.000s             	├─ kernel notified: 0.000s
[03:21:43.692002]	 +0.000s             	╰─ END 0.000s, total 44.527s
---- End of Restore timing information ----
`

func TestParseRestoreTimingUsesRestoreRPCNotWarmWait(t *testing.T) {
	date := time.Date(2026, time.October, 11, 0, 0, 0, 0, time.Local)
	timing, ok := parseRestoreTiming([]byte(actualRestoreTimingExcerpt), date)
	if !ok {
		t.Fatal("parseRestoreTiming() did not find the INFO timing table")
	}
	if got, want := timing.points["cm.Restore RPC"], time.Date(2026, time.October, 11, 3, 21, 43, 497310000, time.Local); !got.Equal(want) {
		t.Fatalf("restore RPC timestamp = %v, want %v", got, want)
	}
	if got, want := timing.duration("cm.Restore RPC", "kernel notified"); !want || got != 194689*time.Microsecond {
		t.Fatalf("restore RPC duration = %v, want %v (ok=%t)", got, 194689*time.Microsecond, want)
	}
	if got, want := timing.duration("restore cont 1/2", "restore cont 2/2"); !want || got != 130216*time.Microsecond {
		t.Fatalf("container registration gap = %v, want %v (ok=%t)", got, 130216*time.Microsecond, want)
	}
	if got, want := timing.duration("loading kernel", "kernel loaded"); !want || got != 40214*time.Microsecond {
		t.Fatalf("kernel load duration = %v, want %v (ok=%t)", got, 40214*time.Microsecond, want)
	}
}

func TestParseRestoreTimingRejectsIncompleteTable(t *testing.T) {
	date := time.Date(2026, time.October, 11, 0, 0, 0, 0, time.Local)
	if _, ok := parseRestoreTiming([]byte("---- Restore timing information ----\n[03:21:43.497310] ├─ cm.Restore RPC: 44.332s\n"), date); ok {
		t.Fatal("parseRestoreTiming() accepted a table without its footer")
	}
}

func TestParseRestoreTimingHandlesMidnight(t *testing.T) {
	contents := `---- Restore timing information ----
[23:59:59.900000]	├─ cm.Restore RPC: 0.000s
[00:00:00.100000]	├─ kernel notified: 0.200s
---- End of Restore timing information ----
`
	date := time.Date(2026, time.October, 11, 0, 0, 0, 0, time.Local)
	timing, ok := parseRestoreTiming([]byte(contents), date)
	if !ok {
		t.Fatal("parseRestoreTiming() did not find the wrapped timing table")
	}
	if got, want := timing.duration("cm.Restore RPC", "kernel notified"); !want || got != 200*time.Millisecond {
		t.Fatalf("midnight-spanning restore duration = %v, want %v (ok=%t)", got, 200*time.Millisecond, want)
	}
}

func TestRecordRuntimeTraceImportsRestoreIntervals(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })

	const sid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	logDir := filepath.Join(t.TempDir(), "logs", sid)
	if err := os.MkdirAll(logDir, 0700); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(logDir, "runsc.20261011-032143.692118.boot.log")
	if err := os.WriteFile(logPath, []byte(actualRestoreTimingExcerpt), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "runsc.20261011-032143.692118.create.log"), []byte(actualRestoreTimingExcerpt), 0600); err != nil {
		t.Fatal(err)
	}

	tracer := otel.Tracer("test")
	ctx, parent := tracer.Start(context.Background(), "warm.restore")
	b := &Backend{Base: filepath.Dir(filepath.Dir(logDir))}
	importAt := time.Date(2026, time.October, 11, 3, 21, 44, 0, time.Local)
	b.recordRuntimeTraceAt(ctx, sid, importAt)
	b.recordRuntimeTrace(ctx, "../invalid")
	parent.End()

	spans := recorder.Ended()
	byName := make(map[string]sdktrace.ReadOnlySpan, len(spans))
	for _, span := range spans {
		byName[span.Name()] = span
	}
	restore, ok := byName["runsc.restore"]
	if !ok {
		t.Fatal("missing runsc.restore span")
	}
	if got := restore.EndTime().Sub(restore.StartTime()); got != 194689*time.Microsecond {
		t.Fatalf("runsc.restore duration = %v, want %v", got, 194689*time.Microsecond)
	}
	wait, ok := byName["runsc.restore.container_2_registration_gap"]
	if !ok {
		t.Fatal("missing container registration gap span")
	}
	if got := wait.EndTime().Sub(wait.StartTime()); got != 130216*time.Microsecond {
		t.Fatalf("container registration gap = %v, want %v", got, 130216*time.Microsecond)
	}
	if got, want := wait.Parent().SpanID(), restore.SpanContext().SpanID(); got != want {
		t.Fatalf("wait span parent ID = %v, want restore span %v", got, want)
	}
	if _, ok := byName["runsc.restore.kernel_load"]; ok {
		t.Fatal("unexpected short kernel load span")
	}
	if _, ok := byName["node.runtime_trace.import"]; ok {
		t.Fatal("unexpected importer bookkeeping span")
	}
	if got := attributeValue(restore.Attributes(), "runtime.trace.kernel_load_us"); got != int64(40214) {
		t.Fatalf("kernel load duration attribute = %v, want %d", got, 40214)
	}
	if got := attributeValue(restore.Attributes(), "runtime.trace.registration_gap_us"); got != int64(130216) {
		t.Fatalf("registration gap duration attribute = %v, want %d", got, 130216)
	}
	if got := attributeValue(byName["warm.restore"].Attributes(), "runtime.trace.imported"); got != true {
		t.Fatalf("runtime trace imported attribute = %v, want true", got)
	}
}

func attributeValue(attrs []attribute.KeyValue, key string) any {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.AsInterface()
		}
	}
	return nil
}
