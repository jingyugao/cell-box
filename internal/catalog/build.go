package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"cellbox.local/cellbox/internal/image"
)

type imageBuilder struct {
	options  image.BuildKitOptions
	insecure string
}

var localDockerImage = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func validBuildImage(value string, snapshot json.RawMessage) bool {
	if image.ValidRegistryImage(value) {
		return true
	}
	var profile struct {
		Provider string `json:"provider"`
	}
	return json.Unmarshal(snapshot, &profile) == nil && profile.Provider == "docker" && localDockerImage.MatchString(value)
}

func profileUsesLocalRunsc(snapshot json.RawMessage) bool {
	var profile struct {
		Provider string `json:"provider"`
	}
	return json.Unmarshal(snapshot, &profile) == nil && profile.Provider == "local-runsc"
}

func (b *imageBuilder) Build(ctx context.Context, in BuildInput, log io.Writer, stage func(string) error) (out BuildResult, err error) {
	if err = stage("resolving"); err != nil {
		return out, err
	}
	run, err := image.ParseRunCommand(in.RunCommand)
	if err != nil {
		return out, err
	}
	if run.Image != "" {
		base, e := image.ParseRegistryReference(in.BaseImage, b.insecure)
		if e != nil {
			return out, e
		}
		runRef, e := image.ParseRegistryReference(run.Image, b.insecure)
		if e != nil {
			return out, e
		}
		if runRef.Name() != base.Name() {
			return out, errors.New("runCommand image must match baseImage")
		}
	}
	resolved, err := image.ResolveRegistryImage(ctx, in.BaseImage, in.Platform, b.insecure, nil, run)
	if err != nil {
		return out, err
	}
	out = BuildResult{ResolvedBaseImage: resolved.Source, Command: resolved.Command, Env: resolved.Env, WorkingDir: resolved.WorkingDir}
	fmt.Fprintf(log, "Resolved base image: %s\n", resolved.Source)
	if profileUsesLocalRunsc(in.Profile.Snapshot) {
		// Standalone runsc can use a prebuilt immutable image without any
		// BuildKit service. When a buildCommand is supplied, use the same
		// BuildKit publication pipeline as the Kubernetes provider.
		if in.BuildCommand == "" {
			out.Image = resolved.Source
			fmt.Fprintf(log, "Using immutable source image for local-runsc: %s\n", out.Image)
			return out, nil
		}
	}
	if profileUsesLocalRunsc(in.Profile.Snapshot) && b.options.Address == "" {
		return out, errors.New("local-runsc buildCommand requires BuildKit to be configured")
	}
	if err = stage("building"); err != nil {
		return out, err
	}
	opts := b.options
	opts.Base = resolved.Source
	opts.Platform = in.Platform
	opts.BuildCommand = in.BuildCommand
	opts.LogWriter = log
	opts.NoCache = true
	opts.BuildID = in.OperationID
	prepared, err := image.PrepareBuildKit(ctx, opts)
	if err != nil {
		return out, err
	}
	out.Image = prepared.Image
	fmt.Fprintf(log, "Published image: %s\n", out.Image)
	return out, nil
}

func (m *Manager) logPath(id string) string { return filepath.Join(m.config.Dir, "logs", id+".log") }

// Keep arbitrary build output off SQLite and bound disk use per operation.
type buildLog struct {
	f         *os.File
	written   int64
	truncated bool
}

func (w *buildLog) Write(p []byte) (int, error) {
	const budget = 16 << 20
	n := len(p)
	remaining := int64(budget) - w.written
	if remaining > 0 {
		count, err := w.f.Write(p[:min(int64(len(p)), remaining)])
		w.written += int64(count)
		if err != nil {
			return count, err
		}
	}
	if int64(n) > remaining && !w.truncated {
		w.truncated = true
		if _, err := w.f.WriteString("\n[build log truncated at 16 MiB]\n"); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func (m *Manager) worker() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.wake:
		}
		for {
			client, op, in, ctx, stop, err := m.claim()
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					m.stopOnStorageError(err)
				}
				return
			}
			if op.ID == "" {
				break
			}
			m.executeBuild(ctx, client, op, in)
			stop()
			m.mu.Lock()
			delete(m.active, op.ID)
			m.mu.Unlock()
		}
	}
}

func (m *Manager) claim() (string, Operation, BuildInput, context.Context, context.CancelFunc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var op Operation
	var in BuildInput
	if m.closed {
		return "", op, in, nil, nil, context.Canceled
	}
	var client, raw, input string
	err := m.db.QueryRow("SELECT client_id,record,input FROM operations WHERE status='queued' ORDER BY created_at,id LIMIT 1").Scan(&client, &raw, &input)
	if errors.Is(err, sql.ErrNoRows) {
		return "", op, in, nil, nil, nil
	}
	if err != nil {
		return "", op, in, nil, nil, err
	}
	if err = json.Unmarshal([]byte(raw), &op); err != nil {
		return "", op, in, nil, nil, err
	}
	if err = json.Unmarshal([]byte(input), &in); err != nil {
		return "", op, in, nil, nil, err
	}
	now := time.Now().UTC()
	op.Status = "running"
	op.Stage = "starting"
	op.Version++
	op.StartedAt = &now
	op.UpdatedAt = now
	if _, err = m.db.Exec("UPDATE operations SET status=?,record=? WHERE id=?", op.Status, encode(op), op.ID); err != nil {
		return "", op, in, nil, nil, err
	}
	ctx, cancel := context.WithCancel(m.ctx)
	m.active[op.ID] = cancel
	return client, op, in, ctx, cancel, nil
}

func (m *Manager) executeBuild(ctx context.Context, client string, op Operation, in BuildInput) {
	f, err := os.OpenFile(m.logPath(op.ID), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		m.complete(client, op.ID, in, BuildResult{}, nil, err)
		return
	}
	log := &buildLog{f: f}
	fmt.Fprintf(log, "Build %s for template %s, user version %s\n", op.ID, op.TargetID, in.UserVersion)
	result, buildErr := m.config.Builder.Build(ctx, in, log, func(stage string) error { return m.stage(client, op.ID, stage) })
	var snapshot json.RawMessage
	if buildErr == nil {
		if profileUsesLocalRunsc(in.Profile.Snapshot) && in.BuildCommand == "" && result.Image != result.ResolvedBaseImage {
			buildErr = errors.New("local-runsc revisions without buildCommand must use the resolved source image")
		}
		if buildErr == nil && !validBuildImage(result.Image, in.Profile.Snapshot) {
			buildErr = errors.New("builder did not return an immutable registry image")
		}
		if buildErr == nil && m.config.FinalizeProfile != nil {
			snapshot, buildErr = m.config.FinalizeProfile(in.Profile, result)
		}
		if buildErr == nil && m.config.FinalizeProfile == nil {
			snapshot = in.Profile.Snapshot
		}
		if buildErr == nil && !json.Valid(snapshot) {
			buildErr = errors.New("invalid final profile snapshot")
		}
	}
	if buildErr != nil {
		fmt.Fprintf(log, "Build failed: %v\n", buildErr)
	}
	// Closing the log precedes terminal publication: terminal eof is stable.
	buildErr = errors.Join(buildErr, f.Sync(), f.Close())
	m.complete(client, op.ID, in, result, snapshot, buildErr)
}

func (m *Manager) stage(client, id, stage string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	op, _, err := getOperation(context.Background(), m.db, client, id)
	if err != nil {
		return err
	}
	if op.Status != "running" || m.closed {
		return context.Canceled
	}
	op.Stage = stage
	op.Version++
	op.UpdatedAt = time.Now().UTC()
	_, err = m.db.Exec("UPDATE operations SET record=? WHERE id=?", encode(op), id)
	return err
}

func (m *Manager) finish(client, id string, in BuildInput, result BuildResult, snapshot json.RawMessage, buildErr error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	op, _, err := getOperation(context.Background(), tx, client, id)
	if err != nil {
		return err
	}
	if terminal(op.Status) {
		return nil
	}
	now := time.Now().UTC()
	op.Version++
	op.UpdatedAt = now
	op.FinishedAt = &now
	op.Result = map[string]string{}
	if result.ResolvedBaseImage != "" {
		op.Result["resolvedBaseImage"] = result.ResolvedBaseImage
	}
	if result.Image != "" {
		op.Result["image"] = result.Image
	}
	if op.Status == "cancelling" {
		op.Status = "cancelled"
		op.Stage = "cancelled"
	} else if m.closed {
		op.Status = "failed"
		op.Stage = "interrupted"
		op.Error = &Error{Code: "UNKNOWN_OUTCOME", Message: "Service stopped during build; the result is unknown and the build was not resumed"}
	} else if buildErr != nil {
		op.Status = "failed"
		op.Stage = "failed"
		message := buildErr.Error()
		if len(message) > 4096 {
			message = message[:4096]
		}
		op.Error = &Error{Code: "IMAGE_BUILD_FAILED", Message: message}
	} else {
		t, err := getTemplate(context.Background(), tx, client, in.TemplateID)
		if err != nil {
			return err
		}
		var next int
		if err = tx.QueryRow("SELECT COALESCE(MAX(revision),0)+1 FROM template_revisions WHERE template_id=? AND user_version=?", in.TemplateID, in.UserVersion).Scan(&next); err != nil {
			return err
		}
		r := Revision{ID: newID("rev-"), TemplateID: t.ID, TemplateName: t.Name, UserVersion: in.UserVersion, Revision: next, Version: fmt.Sprintf("%s-r%d", in.UserVersion, next), ProfileID: in.Profile.ID, BaseImage: in.BaseImage, ResolvedBaseImage: result.ResolvedBaseImage, Image: result.Image, BuildCommand: in.BuildCommand, RunCommand: in.RunCommand, Platform: in.Platform, CPU: in.Profile.CPU, MemoryMiB: in.Profile.MemoryMiB, WarmSlots: *in.WarmSlots, Status: "active", Generation: 1, BuildOperationID: id, CreatedAt: now, UpdatedAt: now}
		if _, err = tx.Exec("INSERT INTO template_revisions(id,client_id,template_id,user_version,revision,record,profile) VALUES(?,?,?,?,?,?,?)", r.ID, client, r.TemplateID, r.UserVersion, r.Revision, encode(r), string(snapshot)); err != nil {
			return err
		}
		op.Status = "succeeded"
		op.Stage = "completed"
		op.Result["revisionId"] = r.ID
	}
	if _, err = tx.Exec("UPDATE operations SET status=?,record=? WHERE id=?", op.Status, encode(op), id); err != nil {
		return err
	}
	return tx.Commit()
}

// A failed revision commit must not leave a completed worker looking active.
// The second transaction records failure and retains any published image. If
// even that cannot be saved, stop accepting work until the service is restarted.
func (m *Manager) complete(client, id string, in BuildInput, result BuildResult, snapshot json.RawMessage, buildErr error) {
	if err := m.finish(client, id, in, result, snapshot, buildErr); err != nil {
		slog.Error("cannot commit template build result", "operation", id, "error", err)
		if fallbackErr := m.finish(client, id, in, result, nil, fmt.Errorf("Cannot commit build result: %w", err)); fallbackErr != nil {
			m.stopOnStorageError(fallbackErr)
		}
	}
}

func (m *Manager) stopOnStorageError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fault = err
	m.cancel()
	slog.Error("local catalog stopped after storage failure", "error", err)
}
