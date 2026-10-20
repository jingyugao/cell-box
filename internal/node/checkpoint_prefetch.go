// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	prefetchCommandTimeout = 45 * time.Second
	prefetchOutputLimit    = 64 * 1024
)

// PrefetchStatus is the durable result of one external prefetch attempt.
// ToolOutput is truncated to prefetchOutputLimit bytes.
type PrefetchStatus struct {
	Owner         string    `json:"owner"`
	Snapshot      string    `json:"snapshot"`
	CheckpointDir string    `json:"checkpointDir"`
	Manifest      string    `json:"manifest"`
	State         string    `json:"state"`
	StartedAt     time.Time `json:"startedAt"`
	EndedAt       time.Time `json:"endedAt,omitempty"`
	ExitCode      int       `json:"exitCode"`
	Canceled      bool      `json:"canceled,omitempty"`
	Error         string    `json:"error,omitempty"`
	ToolOutput    string    `json:"toolOutput,omitempty"`
}

type prefetchKey struct{ owner, snapshot string }

type prefetchJob struct {
	cancel    context.CancelFunc
	done      chan struct{}
	status    PrefetchStatus
	canceled  atomic.Bool
	statusErr error
}

// PrefetchManager runs checkpointprefetch outside the checkpoint directory and
// owns its cancellation and wait lifecycle. Its zero value and nil receiver are
// disabled and safe to use.
type PrefetchManager struct {
	Base        string
	HostMountNS bool

	mu         sync.Mutex
	jobs       map[prefetchKey]*prefetchJob
	loadPolicy func(string) (RestorePolicy, error)
	closed     bool
}

// NewPrefetchManager returns a manager for the node-local checkpoint Base.
func NewPrefetchManager(base string, hostMountNS bool) *PrefetchManager {
	return &PrefetchManager{Base: base, HostMountNS: hostMountNS}
}

// Capture records the current host page-cache residency ranges for an
// immutable checkpoint. This is a cache-footprint snapshot, not a guest-memory
// hotness profile. The sidecar is stored beside, never inside, the checkpoint.
// Capture runs only when a prefetch binary is configured; Prefetch itself may
// be false so callers can collect evaluation data without enabling prefetch.
func (m *PrefetchManager) Capture(ctx context.Context, owner, snapshot string) (string, error) {
	if m == nil || m.Base == "" {
		return "", nil
	}
	policy, err := m.restorePolicy()
	if err != nil {
		return "", err
	}
	if policy.PrefetchBinary == "" {
		return "", nil
	}
	checkpointDir, manifest, err := m.checkpointPaths(owner, snapshot)
	if err != nil {
		return "", err
	}
	if err := validateCheckpointDirectory(checkpointDir); err != nil {
		return "", err
	}
	cmdName, args := m.command(policy.PrefetchBinary,
		"-mode", "capture",
		"-checkpoint-dir", checkpointDir,
		"-checkpoint-id", owner+":"+snapshot,
		"-manifest", manifest)
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, prefetchCommandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, cmdName, args...)
	cmd.Stdin = nil
	output := &boundedOutput{}
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Run(); err != nil {
		return manifest, fmt.Errorf("capture checkpoint cache footprint for %s:%s: %w: %s", owner, snapshot, err, output.String())
	}
	if err := validateRegularSidecar(manifest); err != nil {
		return manifest, fmt.Errorf("capture did not create a safe manifest: %w", err)
	}
	if err := validateManifestIdentity(manifest, owner+":"+snapshot); err != nil {
		return manifest, fmt.Errorf("capture created a mismatched manifest: %w", err)
	}
	return manifest, nil
}

// Start asynchronously prefetches the selected ranges of one checkpoint. It
// returns after starting the external command, not after its I/O completes.
// Repeated calls for the same identity are idempotent, including after process
// completion, as long as its durable status file remains present.
func (m *PrefetchManager) Start(owner, snapshot string) error {
	if m == nil || m.Base == "" {
		return nil
	}
	policy, err := m.restorePolicy()
	if err != nil {
		return err
	}
	if !policy.Prefetch {
		return nil
	}
	checkpointDir, manifest, err := m.checkpointPaths(owner, snapshot)
	if err != nil {
		return err
	}
	if err := validateCheckpointDirectory(checkpointDir); err != nil {
		return err
	}
	if err := validateRegularSidecar(manifest); err != nil {
		return fmt.Errorf("checkpoint prefetch manifest unavailable: %w", err)
	}
	if err := validateManifestIdentity(manifest, owner+":"+snapshot); err != nil {
		return err
	}
	statusPath, err := PrefetchStatusPath(m.Base, owner, snapshot)
	if err != nil {
		return err
	}

	key := prefetchKey{owner: owner, snapshot: snapshot}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	if job := m.jobs[key]; job != nil {
		return nil
	}
	// Durable status prevents a completed or canceled one-shot checkpoint from
	// being launched again after its in-memory job has been reaped.
	if _, err := os.Lstat(statusPath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat prior prefetch status: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), prefetchCommandTimeout)
	cmdName, args := m.command(policy.PrefetchBinary,
		"-mode", "prefetch",
		"-checkpoint-dir", checkpointDir,
		"-checkpoint-id", owner+":"+snapshot,
		"-manifest", manifest)
	cmd := exec.CommandContext(ctx, cmdName, args...)
	cmd.Stdin = nil
	output := &boundedOutput{}
	cmd.Stdout, cmd.Stderr = output, output
	job := &prefetchJob{
		cancel: cancel,
		done:   make(chan struct{}),
		status: PrefetchStatus{
			Owner: owner, Snapshot: snapshot, CheckpointDir: checkpointDir,
			Manifest: manifest, State: "running", StartedAt: time.Now().UTC(), ExitCode: -1,
		},
	}
	if m.jobs == nil {
		m.jobs = make(map[prefetchKey]*prefetchJob)
	}
	m.jobs[key] = job
	if err := cmd.Start(); err != nil {
		cancel()
		job.status.EndedAt = time.Now().UTC()
		job.status.State = "failed"
		job.status.Error = err.Error()
		close(job.done)
		if statusErr := AtomicJSON(statusPath, job.status); statusErr != nil {
			return fmt.Errorf("start checkpoint prefetch: %v; write status: %w", err, statusErr)
		}
		return fmt.Errorf("start checkpoint prefetch: %w", err)
	}
	if err := AtomicJSON(statusPath, job.status); err != nil {
		cancel()
		_ = cmd.Wait()
		delete(m.jobs, key)
		close(job.done)
		return fmt.Errorf("write initial checkpoint prefetch status: %w", err)
	}
	go m.wait(job, cmd, output, statusPath)
	return nil
}

// Cancel cancels and reaps the selected process. A nil return means the
// process was reaped; its exit/error details remain available in Status.
func (m *PrefetchManager) Cancel(owner, snapshot string) error {
	if m == nil || m.Base == "" {
		return nil
	}
	if !ValidID(owner) || !ValidID(snapshot) {
		return fmt.Errorf("invalid checkpoint prefetch identity")
	}
	m.mu.Lock()
	job := m.jobs[prefetchKey{owner: owner, snapshot: snapshot}]
	if job != nil {
		job.canceled.Store(true)
		job.cancel()
	}
	m.mu.Unlock()
	if job != nil {
		<-job.done
		m.mu.Lock()
		delete(m.jobs, prefetchKey{owner: owner, snapshot: snapshot})
		statusErr := job.statusErr
		m.mu.Unlock()
		if statusErr != nil {
			return statusErr
		}
	}
	return nil
}

// Close cancels and reaps every process owned by the manager.
func (m *PrefetchManager) Close() error {
	if m == nil || m.Base == "" {
		return nil
	}
	m.mu.Lock()
	m.closed = true
	jobs := make([]struct {
		key prefetchKey
		job *prefetchJob
	}, 0, len(m.jobs))
	for key, job := range m.jobs {
		job.canceled.Store(true)
		job.cancel()
		jobs = append(jobs, struct {
			key prefetchKey
			job *prefetchJob
		}{key: key, job: job})
	}
	m.mu.Unlock()
	var resultErr error
	for _, item := range jobs {
		<-item.job.done
		if item.job.statusErr != nil {
			resultErr = errors.Join(resultErr, item.job.statusErr)
		}
	}
	m.mu.Lock()
	for _, item := range jobs {
		delete(m.jobs, item.key)
	}
	m.mu.Unlock()
	return resultErr
}

// Status returns the latest durable prefetch result, if one exists.
func (m *PrefetchManager) Status(owner, snapshot string) (PrefetchStatus, bool, error) {
	if m == nil || m.Base == "" {
		return PrefetchStatus{}, false, nil
	}
	path, err := PrefetchStatusPath(m.Base, owner, snapshot)
	if err != nil {
		return PrefetchStatus{}, false, err
	}
	var status PrefetchStatus
	if err := ReadJSON(path, &status); errors.Is(err, os.ErrNotExist) {
		return PrefetchStatus{}, false, nil
	} else if err != nil {
		return PrefetchStatus{}, false, err
	}
	return status, true, nil
}

// PrefetchStatusPath returns the durable status path, outside the checkpoint.
func PrefetchStatusPath(base, owner, snapshot string) (string, error) {
	if !ValidID(owner) || !ValidID(snapshot) {
		return "", fmt.Errorf("invalid checkpoint prefetch identity")
	}
	return filepath.Join(base, "logs", "prefetch", owner, snapshot+".json"), nil
}

func (m *PrefetchManager) wait(job *prefetchJob, cmd *exec.Cmd, output *boundedOutput, statusPath string) {
	err := cmd.Wait()
	job.status.EndedAt = time.Now().UTC()
	job.status.ToolOutput = output.String()
	job.status.Canceled = job.canceled.Load()
	if err != nil {
		if job.status.Canceled {
			job.status.State = "canceled"
		} else {
			job.status.State = "failed"
		}
		job.status.Error = err.Error()
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			job.status.ExitCode = exitErr.ExitCode()
		}
	} else {
		job.status.State = "succeeded"
		job.status.ExitCode = 0
	}
	if err := AtomicJSON(statusPath, job.status); err != nil {
		job.statusErr = fmt.Errorf("write checkpoint prefetch status: %w", err)
		job.status.Error = strings.TrimSpace(job.status.Error + "; write status: " + err.Error())
	}
	job.cancel()
	close(job.done)
}

func (m *PrefetchManager) restorePolicy() (RestorePolicy, error) {
	if m.loadPolicy != nil {
		return m.loadPolicy(m.Base)
	}
	return LoadRestorePolicy(m.Base)
}

func (m *PrefetchManager) checkpointPaths(owner, snapshot string) (string, string, error) {
	checkpointDir, err := SnapshotPath(m.Base, owner, snapshot)
	if err != nil {
		return "", "", err
	}
	manifest := checkpointDir + ".hotset.json"
	return checkpointDir, manifest, nil
}

func (m *PrefetchManager) command(binary string, toolArgs ...string) (string, []string) {
	if !m.HostMountNS {
		return binary, toolArgs
	}
	args := []string{"--target=1", "--mount", "--root", "--", binary}
	return "nsenter", append(args, toolArgs...)
}

func validateCheckpointDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("stat checkpoint directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("checkpoint path is not a real directory")
	}
	return nil
}

func validateRegularSidecar(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("manifest is not a regular file")
	}
	return nil
}

func validateManifestIdentity(path, expectedID string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open manifest: %w", err)
	}
	defer f.Close()
	var manifest struct {
		CheckpointID string `json:"checkpoint_id"`
	}
	decoder := json.NewDecoder(io.LimitReader(f, 16*1024*1024))
	if err := decoder.Decode(&manifest); err != nil {
		return fmt.Errorf("decode manifest identity: %w", err)
	}
	if manifest.CheckpointID != expectedID {
		return fmt.Errorf("manifest checkpoint ID %q does not match %q", manifest.CheckpointID, expectedID)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("manifest has trailing JSON value")
		}
		return fmt.Errorf("decode manifest trailer: %w", err)
	}
	return nil
}

type boundedOutput struct {
	mu        sync.Mutex
	bytes     []byte
	truncated bool
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	original := len(p)
	remaining := prefetchOutputLimit - len(b.bytes)
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
			b.truncated = true
		}
		b.bytes = append(b.bytes, p...)
	} else if len(p) > 0 {
		b.truncated = true
	}
	return original, nil
}

func (b *boundedOutput) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	value := string(b.bytes)
	if b.truncated {
		value += "\n[output truncated]"
	}
	return value
}
