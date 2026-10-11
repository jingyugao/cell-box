// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	runtimeTraceLogLimit = 1 << 20
	// Warm restore logs are imported promptly after CRI reports Running. The
	// bound rejects stale logs and wall-clock parses from a mismatched zone.
	runtimeTraceMaxAge = time.Minute
)

var (
	restoreLogTimeRE  = regexp.MustCompile(`^\[(\d{2}:\d{2}:\d{2}\.\d{6})\]`)
	bootLogNameRE     = regexp.MustCompile(`^runsc\.(\d{8}-\d{6}\.\d{6})\.boot\.log$`)
	restoreMilestones = []string{
		"cm.Restore RPC",
		"restore cont 1/2",
		"restore cont 2/2",
		"loading kernel",
		"kernel loaded",
		"kernel notified",
	}
)

type restoreTiming struct {
	points map[string]time.Time
}

// parseRestoreTiming parses the INFO-level timing table emitted by gVisor's
// timing.Timer.Log. The filename timestamp provides the node-local date for
// table rows, which themselves contain only local wall-clock time.
func parseRestoreTiming(contents []byte, logDate time.Time) (restoreTiming, bool) {
	const (
		header = "---- Restore timing information ----"
		footer = "---- End of Restore timing information ----"
	)
	var blocks []restoreTiming
	var points map[string]time.Time
	var previousClock time.Duration
	dayOffset := 0
	havePreviousClock := false
	inside := false
	for _, line := range strings.Split(string(contents), "\n") {
		if strings.Contains(line, header) {
			inside = true
			points = make(map[string]time.Time)
			previousClock = 0
			dayOffset = 0
			havePreviousClock = false
			continue
		}
		if !inside {
			continue
		}
		if strings.Contains(line, footer) {
			blocks = append(blocks, restoreTiming{points: points})
			inside = false
			continue
		}
		match := restoreLogTimeRE.FindStringSubmatch(line)
		if len(match) != 2 {
			continue
		}
		clock, err := time.ParseInLocation("15:04:05.000000", match[1], logDate.Location())
		if err != nil {
			continue
		}
		clockOffset := time.Duration(clock.Hour())*time.Hour + time.Duration(clock.Minute())*time.Minute + time.Duration(clock.Second())*time.Second + time.Duration(clock.Nanosecond())
		if havePreviousClock && clockOffset < previousClock && previousClock-clockOffset > 12*time.Hour {
			dayOffset++
		}
		previousClock = clockOffset
		havePreviousClock = true
		at := time.Date(logDate.Year(), logDate.Month(), logDate.Day()+dayOffset, clock.Hour(), clock.Minute(), clock.Second(), clock.Nanosecond(), logDate.Location())
		for _, milestone := range restoreMilestones {
			if strings.Contains(line, "├─ "+milestone+":") {
				if _, exists := points[milestone]; !exists {
					points[milestone] = at
				}
			}
		}
	}
	// Do not import a truncated table. It may have been read from a bounded
	// tail that starts or ends in the middle of a log write.
	for i := len(blocks) - 1; i >= 0; i-- {
		if _, ok := blocks[i].points["cm.Restore RPC"]; ok {
			return blocks[i], true
		}
	}
	return restoreTiming{}, false
}

func (t restoreTiming) duration(from, to string) (time.Duration, bool) {
	a, aok := t.points[from]
	b, bok := t.points[to]
	if !aok || !bok || b.Before(a) {
		return 0, false
	}
	return b.Sub(a), true
}

// recordRuntimeTrace imports completed gVisor restore timings after CRI reports
// the restored container running. Missing, oversized, or malformed logs are
// ignored so diagnostics cannot affect restore success.
func (b *Backend) recordRuntimeTrace(ctx context.Context, sid string) {
	b.recordRuntimeTraceAt(ctx, sid, time.Now())
}

func (b *Backend) recordRuntimeTraceAt(ctx context.Context, sid string, now time.Time) {
	parent := trace.SpanFromContext(ctx)
	if !parent.IsRecording() || !ValidID(sid) || len(sid) != 64 {
		return
	}
	imported := false
	defer func() { parent.SetAttributes(attribute.Bool("runtime.trace.imported", imported)) }()
	dir := filepath.Join(b.Base, "logs", sid)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type logFile struct {
		name    string
		logDate time.Time
		modTime time.Time
	}
	var files []logFile
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		match := bootLogNameRE.FindStringSubmatch(entry.Name())
		if len(match) != 2 {
			continue
		}
		// runsc formats table clocks in time.Local; deployed node and controller
		// processes both use UTC, so the filename's local date is an absolute date.
		logDate, err := time.ParseInLocation("20060102-150405.000000", match[1], time.Local)
		if err != nil {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		files = append(files, logFile{name: entry.Name(), logDate: logDate, modTime: info.ModTime()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].modTime.After(files[j].modTime) })
	remaining := runtimeTraceLogLimit
	var timing restoreTiming
	found := false
	for i, file := range files {
		if i >= 16 {
			break
		}
		if remaining <= 0 {
			break
		}
		path := filepath.Join(dir, file.name)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
			continue
		}
		size := info.Size()
		if size > int64(remaining) {
			size = int64(remaining)
		}
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		if _, err := f.Seek(info.Size()-size, io.SeekStart); err != nil {
			f.Close()
			continue
		}
		contents := make([]byte, size)
		_, readErr := io.ReadFull(f, contents)
		f.Close()
		remaining -= len(contents)
		if readErr != nil {
			continue
		}
		if parsed, ok := parseRestoreTiming(contents, file.logDate); ok {
			timing, found = parsed, true
			break
		}
	}
	if !found {
		return
	}
	start, startOK := timing.points["cm.Restore RPC"]
	end, endOK := timing.points["kernel notified"]
	if !startOK || !endOK || !end.After(start) || end.After(now) || now.Sub(start) > runtimeTraceMaxAge {
		return
	}
	imported = true
	var restoreAttributes []attribute.KeyValue
	var registrationGap time.Duration
	var hasRegistrationGap bool
	if wait, ok := timing.duration("restore cont 1/2", "restore cont 2/2"); ok && wait > 0 {
		registrationGap, hasRegistrationGap = wait, true
		restoreAttributes = append(restoreAttributes, attribute.Int64("runtime.trace.registration_gap_us", wait.Microseconds()))
	}
	for _, phase := range []struct{ from, to, key string }{
		{"restore cont 2/2", "loading kernel", "runtime.trace.kernel_load_start_gap_us"},
		{"kernel loaded", "kernel notified", "runtime.trace.kernel_notify_us"},
	} {
		if elapsed, ok := timing.duration(phase.from, phase.to); ok {
			restoreAttributes = append(restoreAttributes, attribute.Int64(phase.key, elapsed.Microseconds()))
		}
	}
	if kernelLoad, ok := timing.duration("loading kernel", "kernel loaded"); ok && kernelLoad > 0 {
		restoreAttributes = append(restoreAttributes,
			attribute.Int64("runtime.trace.kernel_load_us", kernelLoad.Microseconds()),
			attribute.Int64("runtime.trace.kernel_load_start_offset_us", timing.points["loading kernel"].Sub(start).Microseconds()),
		)
	}
	restoreCtx := addRuntimeTraceSpan(ctx, "runsc.restore", start, end, "gvisor.info_timer", "Restore", restoreAttributes...)
	if hasRegistrationGap && registrationGap >= 50*time.Millisecond {
		addRuntimeTraceSpan(restoreCtx, "runsc.restore.container_2_registration_gap", timing.points["restore cont 1/2"], timing.points["restore cont 2/2"], "gvisor.info_timer", "Restore")
	}
}

func addRuntimeTraceSpan(ctx context.Context, name string, start, end time.Time, source, timer string, extra ...attribute.KeyValue) context.Context {
	attrs := []attribute.KeyValue{
		attribute.String("runtime.trace.source", source),
		attribute.String("runtime.trace.timer", timer),
		attribute.Int64("runtime.trace.duration_us", end.Sub(start).Microseconds()),
	}
	attrs = append(attrs, extra...)
	_, span := otel.Tracer("cellbox.runtime").Start(ctx, name,
		trace.WithTimestamp(start),
		trace.WithAttributes(attrs...),
	)
	span.End(trace.WithTimestamp(end))
	return trace.ContextWithSpan(ctx, span)
}
