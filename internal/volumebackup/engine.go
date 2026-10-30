// Package volumebackup stores incremental volume snapshots in an isolated
// restic repository per client and volume. Repositories use restic's native
// empty-password mode; access is controlled by object storage permissions.
package volumebackup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jingyugao/cell-box/internal/directoryarchive"
	"github.com/jingyugao/cell-box/internal/objectstorage"
)

var (
	ErrDisabled       = errors.New("volume backups are disabled")
	ErrNotFound       = errors.New("volume backup not found")
	ErrInvalid        = errors.New("invalid volume backup request")
	ErrCommand        = errors.New("restic command failed")
	ErrBusy           = errors.New("restic repository is busy")
	ErrUnknownOutcome = errors.New("restic mutation outcome is unknown")
	ErrOutputTooBig   = errors.New("restic output exceeded limit")
	ErrArchiveTooBig  = errors.New("volume backup export exceeded archive limits")
)

const (
	completeTag       = "cellbox-complete"
	pendingTag        = "cellbox-pending"
	outputLimit       = 8 << 20
	fileOutputLimit   = 16 << 20
	stderrLimit       = 8 << 10
	commandMaxRuntime = 24 * time.Hour
)

var snapshotIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

// Config controls the external restic executable and backup availability.
type Config struct {
	Enabled  bool   `json:"enabled,omitempty"`
	Binary   string `json:"binary,omitempty"`
	KeepLast int    `json:"keepLast,omitempty"`
}

func (c *Config) Validate() error {
	if c.Binary == "" {
		c.Binary = "restic"
	}
	if strings.TrimSpace(c.Binary) != c.Binary || c.Binary == "" {
		return errors.New("volumeBackup.binary must be a nonempty executable path")
	}
	if c.KeepLast == 0 {
		c.KeepLast = 3
	}
	if c.KeepLast < 1 || c.KeepLast > 100 {
		return errors.New("volumeBackup.keepLast must be between 1 and 100")
	}
	return nil
}

// Snapshot describes a completed backup. Size values remain nil when restic
// does not report them.
type Snapshot struct {
	ID         string    `json:"id"`
	CreatedAt  time.Time `json:"createdAt"`
	SizeBytes  *int64    `json:"sizeBytes,omitempty"`
	AddedBytes *int64    `json:"addedBytes,omitempty"`
}

// Result includes retention failures separately because the new backup can
// already be valid and queryable when pruning fails.
type Result struct {
	Snapshot       Snapshot `json:"snapshot"`
	RetentionError string   `json:"retentionError,omitempty"`
}

type Engine struct {
	config             Config
	storage            objectstorage.Config
	repositoryOverride string // test-only local repository
	cacheDir           string // test-only isolated restic cache
}

func New(config Config, storage objectstorage.Config) (*Engine, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err := storage.Validate(); err != nil {
		return nil, err
	}
	return &Engine{config: config, storage: storage}, nil
}

func (e *Engine) Backup(ctx context.Context, clientID, volumeID, sourceDir string) (Result, error) {
	if err := e.ready(clientID, volumeID); err != nil {
		return Result{}, err
	}
	if sourceDir == "" {
		return Result{}, fmt.Errorf("%w: source directory is required", ErrInvalid)
	}
	source, err := filepath.Abs(sourceDir)
	if err != nil {
		return Result{}, fmt.Errorf("%w: source directory is invalid", ErrInvalid)
	}
	info, err := os.Stat(source)
	if err != nil || !info.IsDir() {
		return Result{}, fmt.Errorf("%w: source directory is unavailable", ErrInvalid)
	}

	if err := e.ensureRepository(ctx, clientID, volumeID); err != nil {
		return Result{}, err
	}
	args := append(e.baseArgs(clientID, volumeID), "--json", "backup", "--host", "cellbox-volume", "--group-by", "", "--tag", pendingTag, ".")
	output, err := e.run(ctx, source, args...)
	if err != nil {
		return Result{}, err
	}
	created, err := decodeBackupOutput(output)
	if err != nil {
		return Result{}, ErrUnknownOutcome
	}
	// A failed backup can leave a native restic snapshot. It stays pending and
	// is neither returned by List nor eligible for retention.
	if created.ID == "" || !snapshotIDPattern.MatchString(created.ID) {
		return Result{}, ErrUnknownOutcome
	}
	if _, err := e.run(ctx, "", append(e.baseArgs(clientID, volumeID), "tag", "--set", completeTag, created.ID)...); err != nil {
		return Result{}, fmt.Errorf("%w: completion marker could not be confirmed", ErrUnknownOutcome)
	}

	listed, err := e.listSnapshots(ctx, clientID, volumeID)
	if err != nil {
		return Result{}, fmt.Errorf("%w: completion marker could not be confirmed", ErrUnknownOutcome)
	}
	var snapshot Snapshot
	for _, item := range listed {
		if item.Original == created.ID || item.ID == created.ID {
			snapshot = Snapshot{ID: item.ID, CreatedAt: item.Time, SizeBytes: item.Summary.TotalBytesProcessed, AddedBytes: item.Summary.DataAdded}
			break
		}
	}
	if snapshot.ID == "" {
		return Result{}, ErrUnknownOutcome
	}
	result := Result{Snapshot: snapshot}
	if _, err := e.run(ctx, "", append(e.baseArgs(clientID, volumeID), "forget", "--json", "--keep-last", fmt.Sprint(e.config.KeepLast), "--group-by", "", "--tag", completeTag, "--prune")...); err != nil {
		result.RetentionError = err.Error()
	}
	return result, nil
}

func (e *Engine) List(ctx context.Context, clientID, volumeID string) ([]Snapshot, error) {
	if err := e.ready(clientID, volumeID); err != nil {
		return nil, err
	}
	items, err := e.listSnapshots(ctx, clientID, volumeID)
	if err != nil {
		return nil, err
	}
	snapshots := make([]Snapshot, 0, len(items))
	for _, item := range items {
		if !contains(item.Tags, completeTag) || !snapshotIDPattern.MatchString(item.ID) {
			continue
		}
		if item.Time.IsZero() {
			continue
		}
		snapshots = append(snapshots, Snapshot{ID: item.ID, CreatedAt: item.Time, SizeBytes: item.Summary.TotalBytesProcessed, AddedBytes: item.Summary.DataAdded})
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].CreatedAt.After(snapshots[j].CreatedAt) })
	return snapshots, nil
}

func (e *Engine) listSnapshots(ctx context.Context, clientID, volumeID string) ([]resticSnapshot, error) {
	output, err := e.run(ctx, "", append(e.baseArgs(clientID, volumeID), "--json", "snapshots")...)
	if commandExitCode(err) == 10 {
		return []resticSnapshot{}, nil
	}
	if err != nil {
		return nil, err
	}
	var raw []resticSnapshot
	if err := json.Unmarshal(output, &raw); err != nil {
		return nil, fmt.Errorf("%w: invalid snapshot listing", ErrCommand)
	}
	return raw, nil
}

func (e *Engine) ensureRepository(ctx context.Context, clientID, volumeID string) error {
	_, err := e.run(ctx, "", append(e.baseArgs(clientID, volumeID), "--json", "snapshots")...)
	if commandExitCode(err) != 10 {
		return err
	}
	_, err = e.run(ctx, "", append(e.baseArgs(clientID, volumeID), "init")...)
	return err
}

func (e *Engine) Restore(ctx context.Context, clientID, volumeID, snapshotID, targetDir string) error {
	if err := e.ready(clientID, volumeID); err != nil {
		return err
	}
	if !snapshotIDPattern.MatchString(snapshotID) {
		return fmt.Errorf("%w: snapshot ID must be a full 64-character hexadecimal ID", ErrInvalid)
	}
	if targetDir == "" {
		return fmt.Errorf("%w: restore target is required", ErrInvalid)
	}
	target, err := filepath.Abs(targetDir)
	if err != nil {
		return fmt.Errorf("%w: restore target is invalid", ErrInvalid)
	}
	info, err := os.Lstat(target)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: restore target must be an existing directory", ErrInvalid)
	}
	entries, err := os.ReadDir(target)
	if err != nil || len(entries) != 0 {
		return fmt.Errorf("%w: restore target must be empty", ErrInvalid)
	}
	available, err := e.listSnapshots(ctx, clientID, volumeID)
	if err != nil {
		return err
	}
	completed := false
	for _, item := range available {
		if strings.EqualFold(item.ID, snapshotID) && contains(item.Tags, completeTag) {
			completed = true
			break
		}
	}
	if !completed {
		return ErrNotFound
	}
	_, err = e.run(ctx, "", append(e.baseArgs(clientID, volumeID), "restore", snapshotID, "--target", target)...)
	return err
}

// ReadFile prints one regular file from a completed snapshot. Restic receives a
// single relative path and the output remains bounded before it reaches HTTP.
func (e *Engine) ReadFile(ctx context.Context, clientID, volumeID, snapshotID, rel string) ([]byte, error) {
	if err := e.ready(clientID, volumeID); err != nil {
		return nil, err
	}
	if !snapshotIDPattern.MatchString(snapshotID) || !safeSnapshotPath(rel) {
		return nil, fmt.Errorf("%w: full snapshot ID and clean relative path are required", ErrInvalid)
	}
	items, err := e.listSnapshots(ctx, clientID, volumeID)
	if err != nil {
		return nil, err
	}
	completed := false
	for _, item := range items {
		if strings.EqualFold(item.ID, snapshotID) && contains(item.Tags, completeTag) {
			completed = true
			break
		}
	}
	if !completed {
		return nil, ErrNotFound
	}
	listingPath := "/" + rel
	listing, err := e.run(ctx, "", append(e.baseArgs(clientID, volumeID), "--json", "ls", snapshotID, listingPath)...)
	if err != nil {
		if commandExitCode(err) == 10 {
			return nil, ErrNotFound
		}
		return nil, err
	}
	found := false
	decoder := json.NewDecoder(bytes.NewReader(listing))
	for {
		var node struct {
			MessageType string `json:"message_type"`
			Path        string `json:"path"`
			Type        string `json:"type"`
			Size        uint64 `json:"size"`
		}
		if err := decoder.Decode(&node); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("%w: invalid snapshot file listing", ErrCommand)
		}
		if node.MessageType == "node" && snapshotPathMatches(node.Path, rel) {
			if node.Type != "file" {
				return nil, fmt.Errorf("%w: snapshot path is not a regular file", ErrInvalid)
			}
			if node.Size > fileOutputLimit {
				return nil, ErrOutputTooBig
			}
			found = true
		}
	}
	if !found {
		return nil, ErrNotFound
	}
	args := append(e.baseArgs(clientID, volumeID), "dump", "--", snapshotID, rel)
	contents, err := e.runWithOutputLimit(ctx, "", fileOutputLimit, args...)
	if commandExitCode(err) == 10 {
		return nil, ErrNotFound
	}
	return contents, err
}

// ExportDirectory writes a bounded tar.gz of a directory from a completed
// snapshot. It reads the retained repository directly and does not require the
// source Volume record or filesystem to remain present.
func (e *Engine) ExportDirectory(ctx context.Context, clientID, volumeID, snapshotID, rel string, output io.Writer) (directoryarchive.Result, error) {
	if err := e.ready(clientID, volumeID); err != nil {
		return directoryarchive.Result{}, err
	}
	if !snapshotIDPattern.MatchString(snapshotID) || !safeSnapshotDirectoryPath(rel) || output == nil {
		return directoryarchive.Result{}, fmt.Errorf("%w: full snapshot ID, clean directory path, and output are required", ErrInvalid)
	}
	items, err := e.listSnapshots(ctx, clientID, volumeID)
	if err != nil {
		return directoryarchive.Result{}, err
	}
	completed := false
	for _, item := range items {
		if strings.EqualFold(item.ID, snapshotID) && contains(item.Tags, completeTag) {
			completed = true
			break
		}
	}
	if !completed {
		return directoryarchive.Result{}, ErrNotFound
	}
	if err := e.requireSnapshotDirectory(ctx, clientID, volumeID, snapshotID, rel); err != nil {
		return directoryarchive.Result{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	commandCtx, cancel := context.WithTimeout(ctx, commandMaxRuntime)
	defer cancel()
	args := append(e.baseArgs(clientID, volumeID), "dump", "--archive", "tar", "--")
	snapshotPath := snapshotID + ":/"
	if rel != "." {
		snapshotPath += rel
	}
	args = append(args, snapshotPath, "/")
	cmd := exec.CommandContext(commandCtx, e.config.Binary, args...)
	cmd.WaitDelay = 2 * time.Second
	cmd.Stdin = nil
	cmd.Env = e.commandEnv()
	stderr := &boundedBuffer{limit: stderrLimit}
	cmd.Stderr = stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return directoryarchive.Result{}, fmt.Errorf("%w: unable to read snapshot export", ErrCommand)
	}
	if err := cmd.Start(); err != nil {
		return directoryarchive.Result{}, fmt.Errorf("%w: unable to start snapshot export", ErrCommand)
	}
	result, archiveErr := directoryarchive.WriteTar(commandCtx, stdout, output)
	if archiveErr != nil {
		cancel()
		_ = stdout.Close()
	}
	commandErr := cmd.Wait()
	if stderr.exceeded {
		return directoryarchive.Result{}, ErrOutputTooBig
	}
	// A bound violation cancels the producer, so its expected nonzero exit
	// must not hide the archive-limit error returned by the parser.
	if errors.Is(archiveErr, directoryarchive.ErrTooBig) {
		return directoryarchive.Result{}, ErrArchiveTooBig
	}
	if commandErr != nil {
		if commandExitCode(commandErr) == 10 {
			return directoryarchive.Result{}, ErrNotFound
		}
		return directoryarchive.Result{}, &commandError{err: commandErr}
	}
	if archiveErr != nil {
		return directoryarchive.Result{}, archiveErr
	}
	return result, nil
}

func (e *Engine) requireSnapshotDirectory(ctx context.Context, clientID, volumeID, snapshotID, rel string) error {
	listingPath := "/"
	expected := ""
	if rel != "." {
		listingPath += rel
		expected = rel
	}
	listing, err := e.run(ctx, "", append(e.baseArgs(clientID, volumeID), "--json", "ls", "--recursive=false", snapshotID, listingPath)...)
	if commandExitCode(err) == 10 {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(listing))
	found := false
	for {
		var node struct {
			MessageType string `json:"message_type"`
			Path        string `json:"path"`
			Type        string `json:"type"`
			Tree        string `json:"tree"`
		}
		if err := decoder.Decode(&node); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("%w: invalid snapshot directory listing", ErrCommand)
		}
		if rel == "." && node.MessageType == "snapshot" && node.Tree != "" {
			// Restic ls represents the root with snapshot metadata rather than a
			// node whose path is "/".
			found = true
			continue
		}
		if node.MessageType != "node" || !snapshotPathMatches(node.Path, expected) {
			continue
		}
		if node.Type != "dir" {
			return ErrInvalid
		}
		found = true
	}
	if !found {
		return ErrNotFound
	}
	return nil
}

func safeSnapshotDirectoryPath(rel string) bool {
	if rel == "." {
		return true
	}
	if rel == "" || strings.HasPrefix(rel, "/") || strings.Contains(rel, "\\") || len(rel) > 4096 ||
		strings.IndexFunc(rel, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 || len(strings.Split(rel, "/")) > 64 {
		return false
	}
	for _, part := range strings.Split(rel, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return path.Clean(rel) == rel
}

func snapshotPathMatches(candidate, rel string) bool {
	candidate = strings.TrimPrefix(path.Clean(candidate), "/")
	rel = strings.TrimPrefix(path.Clean(rel), "/")
	return candidate == rel
}

func safeSnapshotPath(rel string) bool {
	if rel == "" || strings.HasPrefix(rel, "/") || strings.ContainsRune(rel, 0) || len(rel) > 4096 {
		return false
	}
	for _, part := range strings.Split(rel, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return path.Clean(rel) == rel
}

func (e *Engine) ready(clientID, volumeID string) error {
	if clientID == "" || volumeID == "" {
		return fmt.Errorf("%w: client and volume IDs are required", ErrInvalid)
	}
	if !e.config.Enabled {
		return ErrDisabled
	}
	return nil
}

func (e *Engine) baseArgs(clientID, volumeID string) []string {
	repository := e.repository(clientID, volumeID)
	if e.repositoryOverride != "" {
		repository = e.repositoryOverride
	}
	args := []string{"-r", repository, "--insecure-no-password"}
	if e.cacheDir != "" {
		args = append(args, "--cache-dir", e.cacheDir)
	}
	if strings.HasPrefix(repository, "s3:") {
		args = append(args, "-o", "s3.region="+e.storage.Region, "-o", "s3.bucket-lookup="+map[bool]string{true: "path", false: "auto"}[e.storage.PathStyle])
	}
	return args
}

func (e *Engine) repository(clientID, volumeID string) string {
	hash := sha256.Sum256([]byte(clientID + "\x00" + volumeID))
	prefix := path.Join(e.storage.Prefix, "volume-backups", hex.EncodeToString(hash[:]))
	endpoint := e.storage.Endpoint
	if endpoint == "" {
		endpoint = "https://s3." + e.storage.Region + ".amazonaws.com"
	}
	endpoint = strings.TrimRight(endpoint, "/")
	return "s3:" + endpoint + "/" + e.storage.Bucket + "/" + prefix
}

func (e *Engine) run(ctx context.Context, dir string, args ...string) ([]byte, error) {
	return e.runWithOutputLimit(ctx, dir, outputLimit, args...)
}

func (e *Engine) runWithOutputLimit(ctx context.Context, dir string, limit int, args ...string) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, commandMaxRuntime)
	defer cancel()
	cmd := exec.CommandContext(ctx, e.config.Binary, args...)
	cmd.Dir = dir
	cmd.WaitDelay = 2 * time.Second
	cmd.Stdin = nil
	cmd.Env = e.commandEnv()
	stdout := &boundedBuffer{limit: limit}
	stderr := &boundedBuffer{limit: stderrLimit}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	err := cmd.Run()
	if stdout.exceeded || stderr.exceeded {
		if isMutation(args) {
			return nil, ErrUnknownOutcome
		}
		return nil, ErrOutputTooBig
	}
	if err != nil {
		if isTagMutation(args) {
			if commandExitCode(err) == 11 {
				return nil, ErrBusy
			}
			return nil, ErrUnknownOutcome
		}
		return nil, &commandError{err: err, unknown: isMutation(args) && ctx.Err() != nil}
	}
	return append([]byte(nil), stdout.Bytes()...), nil
}

func (e *Engine) commandEnv() []string {
	env := os.Environ()
	env = filterEnv(env, "RESTIC_REPOSITORY", "RESTIC_REPOSITORY_FILE", "RESTIC_PASSWORD", "RESTIC_PASSWORD_FILE", "RESTIC_PASSWORD_COMMAND")
	env = append(env, "AWS_DEFAULT_REGION="+e.storage.Region)
	if e.storage.AccessKeyEnv != "" {
		env = filterEnv(env, "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
		env = append(env, "AWS_ACCESS_KEY_ID="+os.Getenv(e.storage.AccessKeyEnv), "AWS_SECRET_ACCESS_KEY="+os.Getenv(e.storage.SecretKeyEnv))
		if e.storage.SessionTokenEnv != "" {
			env = append(env, "AWS_SESSION_TOKEN="+os.Getenv(e.storage.SessionTokenEnv))
		}
	}
	return env
}

type resticSnapshot struct {
	ID       string    `json:"id"`
	Original string    `json:"original"`
	Time     time.Time `json:"time"`
	Tags     []string  `json:"tags"`
	Summary  struct {
		TotalBytesProcessed *int64 `json:"total_bytes_processed"`
		DataAdded           *int64 `json:"data_added"`
	} `json:"summary"`
}

type backupOutput struct {
	ID      string    `json:"snapshot_id"`
	Time    time.Time `json:"time"`
	Summary struct {
		TotalBytesProcessed *int64 `json:"total_bytes_processed"`
		DataAdded           *int64 `json:"data_added"`
	} `json:"summary"`
}

func decodeBackupOutput(output []byte) (backupOutput, error) {
	var result backupOutput
	decoder := json.NewDecoder(bytes.NewReader(output))
	found := false
	for {
		var message struct {
			MessageType string    `json:"message_type"`
			SnapshotID  string    `json:"snapshot_id"`
			BackupEnd   time.Time `json:"backup_end"`
			Summary     struct {
				TotalBytesProcessed *int64 `json:"total_bytes_processed"`
				DataAdded           *int64 `json:"data_added"`
			} `json:"summary"`
		}
		err := decoder.Decode(&message)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return result, err
		}
		if message.MessageType == "summary" {
			result.ID, result.Time = message.SnapshotID, message.BackupEnd
			result.Summary.TotalBytesProcessed = message.Summary.TotalBytesProcessed
			result.Summary.DataAdded = message.Summary.DataAdded
			found = true
		}
	}
	if !found {
		return result, errors.New("summary missing")
	}
	return result, nil
}

type commandError struct {
	err     error
	unknown bool
}

func (e *commandError) Error() string {
	if errors.Is(e, ErrBusy) {
		return ErrBusy.Error()
	}
	if e.unknown {
		return ErrUnknownOutcome.Error()
	}
	if code := commandExitCode(e.err); code >= 0 {
		return fmt.Sprintf("%s (exit code %d)", ErrCommand, code)
	}
	return ErrCommand.Error()
}
func (e *commandError) Unwrap() error { return e.err }
func (e *commandError) Is(target error) bool {
	if target == ErrBusy && commandExitCode(e.err) == 11 {
		return true
	}
	if target == ErrUnknownOutcome && e.unknown {
		return true
	}
	return target == ErrCommand
}

func isMutation(args []string) bool {
	for _, arg := range args {
		switch arg {
		case "init", "backup", "tag", "forget", "restore":
			return true
		}
	}
	return false
}

func isTagMutation(args []string) bool {
	for _, arg := range args {
		if arg == "tag" {
			return true
		}
	}
	return false
}

func commandExitCode(err error) int {
	var cmdErr *exec.ExitError
	if errors.As(err, &cmdErr) {
		return cmdErr.ExitCode()
	}
	var wrapped *commandError
	if errors.As(err, &wrapped) {
		return commandExitCode(wrapped.err)
	}
	return -1
}

type boundedBuffer struct {
	bytes.Buffer
	limit    int
	exceeded bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	original := len(p)
	remaining := b.limit - b.Len()
	if remaining <= 0 {
		b.exceeded = true
		return original, nil
	}
	if len(p) > remaining {
		b.exceeded = true
		p = p[:remaining]
	}
	_, _ = b.Buffer.Write(p)
	return original, nil
}

func filterEnv(env []string, names ...string) []string {
	blocked := make(map[string]struct{}, len(names))
	for _, name := range names {
		blocked[name] = struct{}{}
	}
	filtered := env[:0]
	for _, item := range env {
		name, _, _ := strings.Cut(item, "=")
		if _, remove := blocked[name]; !remove {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func contains(items []string, wanted string) bool {
	for _, item := range items {
		if item == wanted {
			return true
		}
	}
	return false
}

var _ io.Writer = (*boundedBuffer)(nil)
