package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/catalog"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/inventory"
	"cellbox.local/cellbox/internal/objectstorage"
	"cellbox.local/cellbox/internal/runtimecontrol"
	"cellbox.local/cellbox/internal/telemetry"
	"cellbox.local/cellbox/internal/volumebackup"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"cellbox.local/cellbox/internal/image"

	"go.opentelemetry.io/otel/codes"
)

type Service struct {
	objects      objectstorage.Objects
	config       Config
	store        *Store
	catalog      *catalog.Manager
	providers    map[string]boxprovider.Provider
	backups      volumeBackupEngine
	http         *http.Client
	toolHTTP     *http.Client
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	mu           sync.Mutex
	admissions   keyedLocks
	observerMu   sync.Mutex
	runtimeFeeds map[string]*runtimeFeed
	streams      map[string]activeStream
}
type activeStream struct {
	boxID  string
	cancel context.CancelFunc
}

func New(config Config, providers map[string]boxprovider.Provider) (*Service, error) {
	return NewContext(context.Background(), config, providers)
}

// RuntimeObjects borrows the service's optional archive payload store for the
// embedded node runtime. Stop all runtime workers before closing the service.
func (s *Service) RuntimeObjects() objectstorage.Objects { return s.objects }

func (s *Service) CheckpointStore() inventory.CheckpointStore { return s.store }

// RuntimeJournal borrows the same durable database as operations and metadata.
// Stop the runtime synchronizer before closing the service.
func (s *Service) RuntimeJournal() *runtimecontrol.Journal {
	return runtimecontrol.NewJournal(s.store.db)
}

func NewContext(parent context.Context, config Config, providers map[string]boxprovider.Provider) (*Service, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	for _, p := range config.Profiles {
		if providers[p.Provider] == nil {
			return nil, fmt.Errorf("provider %s is not configured", p.Provider)
		}
	}
	ctx, cancel := context.WithCancel(parent)
	store, err := OpenStore(config.DataDir)
	if err != nil {
		cancel()
		return nil, err
	}
	var objects objectstorage.Objects
	if config.ObjectStorage != (objectstorage.Config{}) {
		objects, err = objectstorage.New(ctx, config.ObjectStorage)
		if err != nil {
			store.Close()
			cancel()
			return nil, err
		}
	}
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	s := &Service{objects: objects, config: config, store: store, providers: providers,
		http:     &http.Client{Transport: &http.Transport{Proxy: nil, ResponseHeaderTimeout: 35 * time.Second}, CheckRedirect: noRedirect},
		toolHTTP: &http.Client{Transport: &http.Transport{Proxy: nil, ResponseHeaderTimeout: 5*time.Minute + 5*time.Second}, CheckRedirect: noRedirect},
		ctx:      ctx, cancel: cancel, streams: map[string]activeStream{}}
	if config.VolumeBackups.Enabled {
		s.backups, err = volumebackup.New(config.VolumeBackups, config.ObjectStorage)
		if err != nil {
			cancel()
			store.Close()
			return nil, err
		}
	}
	var imageBuild image.BuildKitOptions
	if config.ImageBuild != (ImageBuildConfig{}) {
		imageBuild = image.BuildKitOptions{
			Address: config.ImageBuild.Address, Binary: config.ImageBuild.BuildctlBinary,
			Repository: config.ImageBuild.Repository, GuestBinary: config.ImageBuild.GuestBinary,
		}
	}
	s.catalog, err = catalog.Open(catalog.Config{
		Dir: store.dir, DB: store.db,
		ImageBuild: imageBuild, InsecureRegistry: config.ImageBuild.InsecureRegistry,
		Builder: config.CatalogBuilder,
		ResolveProfile: func(client, id string) (catalog.Profile, error) {
			p, profileErr := s.profile(client, id)
			if profileErr != nil {
				var api *APIError
				if errors.As(profileErr, &api) {
					return catalog.Profile{}, &catalog.Error{Code: api.Code, Message: api.Message}
				}
				return catalog.Profile{}, profileErr
			}
			snapshot, marshalErr := json.Marshal(p)
			return catalog.Profile{ID: p.ID, CPU: p.CPU, MemoryMiB: p.MemoryMiB, Snapshot: snapshot}, marshalErr
		},
		FinalizeProfile: func(base catalog.Profile, result catalog.BuildResult) (json.RawMessage, error) {
			var p Profile
			if err := json.Unmarshal(base.Snapshot, &p); err != nil {
				return nil, err
			}
			p = applyImageConfiguration(p, result.Image, result.Command, result.WorkingDir, result.Env)
			return json.Marshal(p)
		},
	})
	if err != nil {
		cancel()
		store.Close()
		return nil, err
	}
	if err = s.cleanupStartupTemps(); err != nil {
		cancel()
		s.catalog.Close()
		store.Close()
		return nil, err
	}
	err = store.InterruptOperations(ctx)
	if err != nil {
		cancel()
		s.catalog.Close()
		store.Close()
		return nil, err
	}
	if err := s.pruneMetadata(time.Now().UTC()); err != nil {
		cancel()
		s.catalog.Close()
		store.Close()
		return nil, err
	}
	s.startRetention()
	s.startRuntimeObservers()
	return s, nil
}
func (s *Service) Done() <-chan struct{} { return s.ctx.Done() }

func (s *Service) Close() error {
	s.cancel()
	s.mu.Lock()
	for _, stream := range s.streams {
		stream.cancel()
	}
	s.mu.Unlock()
	s.wg.Wait()
	s.http.CloseIdleConnections()
	return errors.Join(s.catalog.Close(), s.store.Close())
}

func randomID(prefix string) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b)
}
func hash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func cloneError(err error) *APIError {
	var e *APIError
	if errors.As(err, &e) {
		return &APIError{Code: e.Code, Message: e.Message}
	}
	if errors.Is(err, boxprovider.ErrNotFound) {
		return &APIError{Code: "NOT_FOUND", Message: "Runtime resource is unavailable"}
	}
	if errors.Is(err, boxprovider.ErrUnsupported) {
		return &APIError{Code: "UNSUPPORTED_CAPABILITY", Message: "The selected provider does not support this operation"}
	}
	if errors.Is(err, boxprovider.ErrStaleTransition) {
		return &APIError{Code: "STALE_TRANSITION", Message: "Runtime transition token is stale or not yet observed"}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &APIError{Code: "TIMEOUT", Message: "Operation deadline exceeded; inspect resource state"}
	}
	return &APIError{Code: "RUNTIME_ERROR", Message: "Runtime operation failed; inspect Cellbox and provider health"}
}
func profileCapabilities(p Profile) Capabilities {
	k8s := p.Provider == "resumable-k8s-pod"
	localRunsc := p.Provider == "local-runsc"
	c := Capabilities{Exec: true, Files: true, HTTP: true, WebSocket: true, InternalServices: true, Suspend: "none", Archives: "none"}
	if k8s || localRunsc {
		c.Suspend = "same-node-checkpoint"
		if p.PersistentHome {
			c.Archives = "stopped-workspace"
		}
		c.ProtectedTools = len(p.Guest.Tools) > 0
		c.CredentialBatch = true
		c.RootDebug = p.Guest.Debug.UID == 0 && p.Guest.Debug.GID == 0
		c.SharedDirectory = p.SharedReadOnlyHostPath != ""
		c.PersistentHome = p.PersistentHome
		c.MountedToolRuntime = c.SharedDirectory && c.ProtectedTools && c.RootDebug && slices.Contains(p.MountedToolRuntimeImages, p.Image)
		c.MountedDebugHome = c.MountedToolRuntime && p.DebugReadWriteHostPath == path.Join(p.SharedReadOnlyHostPath, "runtime/debug-homes") && slices.Contains(p.DebugHomeImages, p.Image)
	}
	return c
}

func (s *Service) box(client, id string) (boxRecord, error) {
	b, err := s.store.BoxRecord(id)
	if err == nil && b.ClientID != client {
		return boxRecord{}, apiError("NOT_FOUND", "Box not found")
	}
	b.Box.Capabilities = boxCapabilities(b)
	return b, err
}
func (s *Service) profile(client, id string) (Profile, error) {
	for _, p := range s.config.Profiles {
		if p.ID == id {
			return p, nil
		}
	}
	return Profile{}, apiError("NOT_FOUND", "Profile not found")
}
func busy(tx *storeTx, b boxRecord) error {
	if b.Box.OperationID == "" {
		return nil
	}
	record, err := tx.Operation(b.Box.OperationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if record.Operation.Status == "queued" || record.Operation.Status == "running" {
		return apiError("BUSY", "Box has an active operation")
	}
	return nil
}

func (s *Service) streamBusy(id string) error {
	for _, stream := range s.streams {
		if stream.boxID == id {
			return apiError("BUSY", "Box has active service requests")
		}
	}
	return nil
}
func (s *Service) prepareOperation(client, kind, target string, prepare func(*storeTx, *Operation) error) (Operation, error) {
	return s.prepareOperationContext(context.Background(), client, kind, target, prepare)
}
func (s *Service) prepareOperationContext(ctx context.Context, client, kind, target string, prepare func(*storeTx, *Operation) error) (Operation, error) {
	ctx, span := telemetry.Start(ctx, "cellbox.operation.admission", telemetry.String("operation.kind", kind), telemetry.String("sandbox.id", target))
	defer span.End()
	unlock := s.admissions.lock(target)
	defer unlock()
	if target != "" && kind != "exec" {
		s.mu.Lock()
		err := s.streamBusy(target)
		s.mu.Unlock()
		if err != nil {
			return Operation{}, err
		}
	}
	op := Operation{ID: randomID("op-"), Kind: kind, TargetID: target, Status: "running", Version: 1, CreatedAt: time.Now().UTC()}
	err := s.store.Write(ctx, func(tx *storeTx) error {
		if err := prepare(tx, &op); err != nil {
			return err
		}
		return tx.PutOperation(operationRecord{Operation: op, ClientID: client})
	})
	return op, err
}

func (s *Service) launch(op Operation, work func(context.Context) (map[string]string, error)) {
	s.launchWithCommitContext(s.ctx, op, work, nil)
}
func (s *Service) launchWithCommitContext(request context.Context, op Operation, work func(context.Context) (map[string]string, error), commit func(*storeTx) error) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithTimeout(telemetry.Detached(request, s.ctx), 15*time.Minute)
		defer cancel()
		ctx, span := telemetry.Start(ctx, "cellbox.operation."+op.Kind, telemetry.String("sandbox.id", op.TargetID), telemetry.String("operation.id", op.ID))
		defer span.End()
		result, workErr := work(ctx)
		if s.ctx.Err() != nil {
			workErr = apiError("UNKNOWN_OUTCOME", "Service stopped before completion; inspect the resource before retrying")
		}
		persist, cancelPersist := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancelPersist()
		saveErr := s.store.Write(persist, func(tx *storeTx) error {
			record, err := tx.Operation(op.ID)
			if err != nil {
				return err
			}
			if record.Operation.Status != "running" && record.Operation.Status != "queued" {
				return boxprovider.ErrStaleTransition
			}
			if workErr == nil && commit != nil {
				workErr = commit(tx)
			}
			now := time.Now().UTC()
			record.Operation.FinishedAt = &now
			record.Operation.Version++
			record.Operation.Result = result
			record.Operation.Status = "succeeded"
			if workErr != nil {
				record.Operation.Status = "failed"
				record.Operation.Error = cloneError(workErr)
			}
			if op.Kind == "volume-backup" || op.Kind == "volume-restore" {
				if err := finishVolumeBackupOperation(tx, op, workErr); err != nil {
					return err
				}
			}
			b, boxErr := tx.Box("", op.TargetID)
			if boxErr == nil && b.Box.OperationID == op.ID {
				if workErr != nil && op.Kind == "resume" {
					s.observerMu.Lock()
					feed := s.runtimeFeeds[b.Box.ID]
					var latest boxprovider.Observation
					observed := feed != nil && feed.handle == b.Handle && feed.observed
					if observed {
						latest = feed.observation
					}
					s.observerMu.Unlock()
					if observed {
						if _, err = projectRuntimeObservation(persist, &b, latest); err != nil {
							return err
						}
					}
					b.Box.Error = cloneError(workErr)
				}
				b.Box.OperationID = ""
				b.Box.Version++
				if workErr != nil && (op.Kind == "create" || op.Kind == "restore") {
					b.Box.Phase = "failed"
					b.Box.Error = cloneError(workErr)
				}
				if err = tx.PutBox(b); err != nil {
					return err
				}
				if workErr == nil && lifecycleOperationKind(op.Kind) {
					box := b.Box
					record.Operation.Box = &box
				}
			} else if lifecycleOperationKind(op.Kind) && workErr == nil {
				workErr = boxprovider.ErrStaleTransition
				record.Operation.Status = "failed"
				record.Operation.Error = cloneError(workErr)
			} else if boxErr != nil {
				var apiErr *APIError
				if !errors.As(boxErr, &apiErr) || apiErr.Code != "NOT_FOUND" {
					return boxErr
				}
			}
			return tx.PutOperation(record)
		})
		if workErr != nil || saveErr != nil {
			span.SetStatus(codes.Error, "")
		}
		if saveErr != nil {
			fmt.Printf("Cellbox could not persist operation completion %s: %v\n", op.ID, saveErr)
			if !errors.Is(saveErr, boxprovider.ErrStaleTransition) && s.cancel != nil {
				s.cancel()
			}
		}
	}()
}
func lifecycleOperationKind(kind string) bool {
	switch kind {
	case "create", "restore", "suspend", "resume", "destroy", "activate":
		return true
	}
	return false
}

// The same event shape as CoCell runtime logs lets the read-only timing tool
// correlate control-plane work with project operations without logging input.
func runtimeTiming(boxID, phase string, started time.Time, err error) {
	finished := time.Now()
	status := "succeeded"
	if err != nil {
		status = "failed"
	}
	data, _ := json.Marshal(map[string]any{
		"event": "sandbox.runtime_stage", "phase": phase, "sandboxId": boxID,
		"status": status, "startedAt": started.UTC(), "finishedAt": finished.UTC(),
		"timestamp": finished.UTC(), "durationMs": finished.Sub(started).Milliseconds(),
	})
	fmt.Printf("%s\n", data)
}
func runtimeSpec(b boxRecord) boxprovider.Spec {
	config := b.Profile.Guest
	if b.Profile.DebugReadWriteHostPath != "" {
		config.DebugHome = "/home/debug"
	}
	metadata, _ := json.Marshal(inventory.Record{ClientID: b.ClientID, Box: mustBoxJSON(b.Box), Staged: b.Staged})
	return boxprovider.Spec{Inventory: metadata, Mounts: b.VolumeMounts, HomeVolumeUID: b.HomeVolumeUID, BoxID: b.Box.ID, Image: b.Profile.Image, Config: config, CPU: b.Profile.CPU, MemoryMiB: b.Profile.MemoryMiB, Namespace: b.Profile.Namespace, NodeName: b.Profile.NodeName, SharedReadOnlyHostPath: b.Profile.SharedReadOnlyHostPath, DebugReadOnlyHostPath: b.Profile.DebugReadOnlyHostPath, DebugReadWriteHostPath: b.Profile.DebugReadWriteHostPath, PersistentHome: b.Profile.PersistentHome, Staged: b.Staged}
}
func (s *Service) rawBox(id string) (boxRecord, error) {
	return s.store.BoxRecord(id)
}
func (s *Service) saveHandle(id string, handle boxprovider.Handle) error {
	return s.store.ChangeBox(context.Background(), id, func(b *boxRecord) error {
		b.Handle = handle
		b.Box.ImageID = handle.ImageID
		b.Box.Version++
		return nil
	})
}
func (s *Service) observe(ctx context.Context, b boxRecord) (result Box, retErr error) {
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return Box{}, err
		}
		ob := boxprovider.Observation{Phase: b.Box.Phase}
		var err error
		if b.Handle.ID != "" {
			ob, err = s.providers[b.Profile.Provider].Inspect(ctx, b.Handle)
		}
		if err != nil && !errors.Is(err, boxprovider.ErrNotFound) {
			return Box{}, err
		}
		missing := errors.Is(err, boxprovider.ErrNotFound)
		if missing {
			ob.Phase = "deleted"
		}
		updated, err := s.applyRuntimeObservation(ctx, b, ob)
		if errors.Is(err, errBoxObservationConflict) {
			if ctx.Err() != nil {
				return Box{}, ctx.Err()
			}
			b, err = s.rawBox(b.Box.ID)
			if err != nil {
				return Box{}, err
			}
			continue
		}
		if err != nil {
			return Box{}, err
		}
		return updated, nil
	}
	return Box{}, apiError("CONFLICT", "Box changed repeatedly while observing runtime")
}

func (s *Service) applyRuntimeObservation(ctx context.Context, b boxRecord, ob boxprovider.Observation) (Box, error) {
	updated, err := s.store.UpdateObservedBox(b, func(current *boxRecord) (bool, error) {
		return projectRuntimeObservation(ctx, current, ob)
	})
	return updated.Box, err
}

func projectRuntimeObservation(ctx context.Context, current *boxRecord, ob boxprovider.Observation) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	previousError := current.Box.Error
	phase := ob.Phase
	if current.Box.Phase == "resuming" && phase == "running" && !ob.GuestReady {
		phase = "resuming"
	}
	if phase == "deleted" {
		if current.Box.Phase == "deleting" || current.Box.Phase == "deleted" {
			phase = "deleted"
		} else {
			phase = "failed"
			current.Box.Error = &APIError{Code: "RUNTIME_LOST", Message: "Runtime disappeared; automatic cold restart is disabled"}
		}
	}
	if current.Staged && phase == "running" {
		if current.Box.Phase == "failed" {
			phase = "failed"
		} else if current.RestoreComplete {
			phase = "staged"
		} else {
			phase = "restoring"
		}
	}
	if current.Box.Phase == "deleting" && phase != "deleted" {
		phase = "deleting"
	}
	target := map[string]string{"suspending": "suspended", "resuming": "running"}[current.Box.Phase]
	if current.Box.OperationID != "" && target != "" && phase != target && phase != "failed" && phase != "deleted" {
		phase = current.Box.Phase
	}
	versionChanged := false
	if previousError != nil && current.Box.Error != nil {
		versionChanged = versionChanged || *previousError != *current.Box.Error
	} else {
		versionChanged = versionChanged || previousError != current.Box.Error
	}
	// The provider generation is authoritative even when there is no
	// current execution identity (for example, a suspended ResumablePod).
	// Keep it monotonic so a stale observation cannot weaken generation
	// fencing after a newer execution has already been observed.
	if ob.Generation > current.Box.Generation {
		current.Box.Generation = ob.Generation
		versionChanged = true
	}
	if current.ExecutionID != ob.ExecutionID {
		current.ExecutionID = ob.ExecutionID
		if ob.ExecutionID != "" && ob.Generation == 0 {
			current.Box.Generation++
		}
		versionChanged = true
	}
	current.Box.ExecutionID = current.ExecutionID
	if current.TransitionID != ob.TransitionID {
		current.TransitionID = ob.TransitionID
		versionChanged = true
	}
	if current.ObservedTransitionID != ob.ObservedTransitionID {
		current.ObservedTransitionID = ob.ObservedTransitionID
		versionChanged = true
	}
	versionChanged = versionChanged || current.Box.Phase != phase
	current.Box.Phase = phase
	if versionChanged {
		current.Box.Version++
	}
	persistChanged := versionChanged
	if current.Box.ObservedAt == nil || time.Since(*current.Box.ObservedAt) >= 5*time.Second || versionChanged {
		now := time.Now().UTC()
		current.Box.ObservedAt = &now
		persistChanged = true
	}
	return persistChanged, nil
}

func (s *Service) connection(ctx context.Context, b boxRecord) (boxprovider.Connection, error) {
	p := s.providers[b.Profile.Provider]
	if fenced, ok := p.(boxprovider.FencedGuestProvider); ok {
		conn, err := fenced.GuestForExecution(ctx, b.Handle, b.ExecutionID)
		if errors.Is(err, boxprovider.ErrStaleExecution) {
			return boxprovider.Connection{}, apiError("STALE_GENERATION", "Runtime changed; fetch the box and inspect its generation")
		}
		return conn, err
	}
	check := func() error {
		ob, err := p.Inspect(ctx, b.Handle)
		if err != nil {
			return err
		}
		if ob.Phase != "running" {
			return apiError("CONFLICT", "Runtime is not running")
		}
		if b.ExecutionID != "" && b.ExecutionID != ob.ExecutionID {
			return apiError("STALE_GENERATION", "Runtime changed; fetch the box and inspect its generation")
		}
		return nil
	}
	if err := check(); err != nil {
		return boxprovider.Connection{}, err
	}
	conn, err := p.Guest(ctx, b.Handle)
	if err != nil {
		return conn, err
	}
	if err = check(); err != nil {
		return boxprovider.Connection{}, err
	}
	return conn, nil
}
func (s *Service) guestRequest(ctx context.Context, b boxRecord, method, path string, body io.Reader) (*http.Response, error) {
	return s.guestRequestWith(s.http, ctx, b, method, path, body)
}
func (s *Service) guestRequestWith(client *http.Client, ctx context.Context, b boxRecord, method, path string, body io.Reader) (*http.Response, error) {
	return s.guestRequestHeaders(client, ctx, b, method, path, body, nil)
}
func (s *Service) guestRequestHeaders(client *http.Client, ctx context.Context, b boxRecord, method, path string, body io.Reader, headers http.Header) (*http.Response, error) {
	conn, err := s.connection(ctx, b)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(conn.URL, "/")+path, body)
	if err != nil {
		return nil, err
	}
	r.Header = headers.Clone()
	if r.Header == nil {
		r.Header = make(http.Header)
	}
	if r.Header.Get("Content-Type") == "" {
		r.Header.Set("Content-Type", "application/json")
	}
	return client.Do(r)
}
func guestSuccess(response *http.Response) error {
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}
	code := "GUEST_REJECTED"
	message := "Guest rejected the requested operation"
	if response.StatusCode == 409 {
		code = "BUSY"
		message = "Guest has active work or is quiesced"
	}
	if response.StatusCode == 400 {
		code = "INVALID_REQUEST"
	}
	if response.StatusCode == 404 {
		code = "NOT_FOUND"
	}
	return apiError(code, message)
}
func (s *Service) guestAction(ctx context.Context, b boxRecord, path string) error {
	res, err := s.guestRequest(ctx, b, "POST", path, nil)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	return guestSuccess(res)
}
func (s *Service) waitState(ctx context.Context, id, want string, checkHealth bool) (Box, error) {
	return s.waitStateTransition(ctx, id, want, checkHealth, "")
}

func (s *Service) waitStateTransition(ctx context.Context, id, want string, checkHealth bool, transitionID string) (Box, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.config.StartupTimeoutSeconds)*time.Second)
	defer cancel()
	var changes <-chan struct{}
	if b, err := s.rawBox(id); err == nil {
		if provider, ok := s.providers[b.Profile.Provider].(boxprovider.ChangeProvider); ok {
			changes, _ = provider.WatchChanges(ctx, b.Handle)
		}
	}
	for {
		b, err := s.rawBox(id)
		if err != nil {
			return Box{}, err
		}
		box, err := s.observe(ctx, b)
		transitionReady := transitionID == ""
		if err == nil && transitionID != "" {
			latest, latestErr := s.rawBox(id)
			transitionReady = latestErr == nil && latest.Box.Version == box.Version &&
				latest.Box.OperationID == transitionID && latest.TransitionID == transitionID &&
				latest.ObservedTransitionID == transitionID
		}
		if err == nil && box.Phase == want && transitionReady {
			if !checkHealth || (want != "running" && want != "staged" && want != "restoring") {
				return box, nil
			}
			updated, _ := s.rawBox(id)
			// Endpoint publication can lag Pod readiness. Bound each attempt so
			// an early dropped SYN does not consume the whole startup deadline.
			probeCtx, probeCancel := context.WithTimeout(ctx, time.Second)
			probeStarted := time.Now()
			res, e := s.guestRequest(probeCtx, updated, "GET", "/healthz", nil)
			runtimeTiming(id, "cellbox.health_probe", probeStarted, e)
			if e == nil {
				res.Body.Close()
				probeCancel()
				if res.StatusCode == 200 {
					return box, nil
				}
			} else {
				probeCancel()
			}
		}
		if err == nil && box.Phase == "failed" {
			return box, apiError("READINESS_FAILED", "Runtime entered failed state")
		}
		interval := 250 * time.Millisecond
		if changes != nil {
			interval = 5 * time.Second
		}
		select {
		case <-ctx.Done():
			return Box{}, ctx.Err()
		case _, ok := <-changes:
			if !ok {
				changes = nil
			}
		case <-time.After(interval):
		}
	}
}

// Startup and read RPCs can spawn short protected-tool calls after the RPC
// itself finishes. Wait briefly for those admitted calls to drain before
// checkpointing; Guest quiesce remains the atomic admission boundary.
func (s *Service) quiesceGuest(ctx context.Context, b boxRecord) error {
	ctx, cancel := context.WithTimeout(ctx, min(5*time.Second, time.Duration(s.config.StartupTimeoutSeconds)*time.Second))
	defer cancel()
	for {
		err := s.guestAction(ctx, b, "/v1/quiesce")
		var rejected *APIError
		if !errors.As(err, &rejected) || rejected.Code != "BUSY" {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (s *Service) provision(ctx context.Context, id string) (Box, error) {
	b, err := s.rawBox(id)
	if err != nil {
		return Box{}, err
	}
	if err := s.prepareBoxVolume(ctx, b); err != nil {
		return Box{}, err
	}
	createStarted := time.Now()
	handle, err := s.providers[b.Profile.Provider].Create(ctx, runtimeSpec(b))
	runtimeTiming(id, "cellbox.create_runtime", createStarted, err)
	if err != nil {
		return Box{}, err
	}
	handleStarted := time.Now()
	err = s.saveHandle(id, handle)
	runtimeTiming(id, "cellbox.persist_handle", handleStarted, err)
	if err != nil {
		return Box{}, err
	}
	want := "running"
	if b.Staged {
		want = "staged"
		if !b.RestoreComplete {
			want = "restoring"
		}
	}
	waitStarted := time.Now()
	box, err := s.waitState(ctx, id, want, true)
	runtimeTiming(id, "cellbox.wait_"+want, waitStarted, err)
	return box, err
}

type createRequest struct {
	Mounts       []Mount `json:"mounts,omitempty"`
	ID           string  `json:"id,omitempty"`
	HomeVolumeID string  `json:"homeVolumeId,omitempty"`
	TemplateID   string  `json:"templateId,omitempty"`
	RevisionID   string  `json:"revisionId,omitempty"`
	OwnerKey     string  `json:"ownerKey"`
	Staged       bool    `json:"staged,omitempty"`
}

func (s *Service) create(client string, input createRequest, archiveID string, acceptImageChange bool) (Operation, error) {
	if input.ID == "" {
		input.ID = randomID("box-")
	}
	if err := validateResourceID(input.ID); err != nil {
		return Operation{}, err
	}
	if input.OwnerKey == "" || len(input.OwnerKey) > 256 {
		return Operation{}, apiError("INVALID_REQUEST", "ownerKey must contain 1..256 bytes")
	}
	if (input.TemplateID == "") == (input.RevisionID == "") {
		return Operation{}, apiError("INVALID_REQUEST", "Specify exactly one of templateId or revisionId")
	}
	tmpl, err := s.resolveTemplate(client, input.TemplateID, input.RevisionID, archiveID != "")
	if err != nil {
		return Operation{}, err
	}
	profile := tmpl.Profile
	if len(input.Mounts) > 0 && input.HomeVolumeID != "" {
		return Operation{}, apiError("INVALID_REQUEST", "Specify mounts or homeVolumeId, not both")
	}
	if archiveID != "" && (input.HomeVolumeID != "" || len(input.Mounts) > 0) {
		return Operation{}, apiError("INVALID_REQUEST", "Archive restore requires a new HOME volume")
	}
	if (input.HomeVolumeID != "" || len(input.Mounts) > 0) &&
		((profile.Provider != "resumable-k8s-pod" && profile.Provider != "local-runsc") || !profile.PersistentHome) {
		return Operation{}, boxprovider.ErrUnsupported
	}
	if profile.PersistentHome {
		if _, err := s.volumeProvider(profile); err != nil {
			return Operation{}, err
		}
	}
	if (archiveID != "" || input.Staged) && !supportsStaging(profile.Provider, s.providers[profile.Provider]) {
		return Operation{}, boxprovider.ErrUnsupported
	}
	if archiveID != "" && (s.objects == nil || !profile.PersistentHome || !workspaceUnderPersistentHome(profile.Guest.Workspace)) {
		return Operation{}, apiError("UNSUPPORTED_CAPABILITY", "Restore requires object storage and a persistent HOME workspace")
	}
	if archiveID != "" {
		if !supportsWorkspaceArchives(profile.Provider, s.providers[profile.Provider]) {
			return Operation{}, apiError("UNSUPPORTED_CAPABILITY", "Restore requires provider workspace archive support")
		}
	}
	kind := "create"
	if archiveID != "" {
		kind = "restore"
	}
	var source Archive
	op, err := s.prepareOperation(client, kind, "", func(tx *storeTx, op *Operation) error {
		if err := tx.reserveID("boxes", input.ID, client); err != nil {
			return err
		}
		if archiveID != "" {
			a, err := tx.Archive(client, archiveID)
			if err != nil {
				return err
			}
			if a.Deleting {
				return apiError("CONFLICT", "Archive is being deleted")
			}
			source = a.Archive
			if source.Agent != profile.Guest.Agent {
				return apiError("ARCHIVE_INCOMPATIBLE", "Archive agent UID/GID differs from target profile")
			}
			if source.ImageID != profile.Image && !(source.Portable && acceptImageChange) {
				return apiError("ARCHIVE_INCOMPATIBLE", "Archive image differs from target image")
			}
		}
		id := input.ID
		op.TargetID = id
		if archiveID != "" {
			op.Result = map[string]string{"archiveId": archiveID}
		}
		phase := "creating"
		if archiveID != "" {
			phase = "restoring"
		}
		b := boxRecord{Box: Box{ID: id, TemplateID: tmpl.Template.ID, RevisionID: tmpl.RevisionID, TemplateName: tmpl.Template.Name, TemplateVersion: tmpl.Template.Version,
			OwnerKey: input.OwnerKey, ProfileID: profile.ID, Phase: phase, Version: 1, Image: profile.Image, Workspace: profile.Guest.Workspace,
			Capabilities: profileCapabilities(profile), OperationID: op.ID, CreatedAt: time.Now().UTC()},
			ClientID: client, Profile: profile, Staged: archiveID != "" || input.Staged, RestoreComplete: input.Staged && archiveID == "", RestoreArchiveID: archiveID, AcceptImageChange: acceptImageChange}
		if err := s.admitBoxMounts(tx, &b, input.Mounts, input.HomeVolumeID); err != nil {
			return err
		}
		return tx.PutBox(b)
	})
	if err != nil {
		return op, err
	}
	s.launch(op, func(ctx context.Context) (map[string]string, error) {
		box, err := s.provision(ctx, op.TargetID)
		if err != nil {
			return nil, err
		}
		if archiveID != "" {
			if err = s.restoreArchive(ctx, box.ID, source); err != nil {
				return nil, err
			}
			if err = s.store.ChangeBox(ctx, box.ID, func(b *boxRecord) error {
				b.RestoreComplete = true
				b.Box.Phase = "staged"
				b.Box.Version++
				return nil
			}); err != nil {
				return nil, err
			}
		}
		return map[string]string{"boxId": box.ID, "templateId": tmpl.Template.ID, "revisionId": tmpl.RevisionID}, nil
	})
	return op, nil
}
func (s *Service) action(client, id, action string) (Operation, error) {
	return s.actionContextWithGeneration(context.Background(), client, id, action, nil)
}
func (s *Service) actionContextWithGeneration(ctx context.Context, client, id, action string, expectedGeneration *uint64) (Operation, error) {
	switch action {
	case "suspend", "resume", "destroy", "activate":
	default:
		return Operation{}, apiError("NOT_FOUND", "Endpoint not found")
	}
	var b boxRecord
	op, err := s.prepareOperationContext(ctx, client, action, id, func(tx *storeTx, op *Operation) error {
		var err error
		b, err = tx.Box(client, id)
		if err != nil {
			return err
		}
		switch action {
		case "destroy":
		case "suspend", "resume":
			if b.Profile.Provider != "resumable-k8s-pod" && b.Profile.Provider != "local-runsc" {
				return boxprovider.ErrUnsupported
			}
		case "activate":
			if !supportsStaging(b.Profile.Provider, s.providers[b.Profile.Provider]) {
				return boxprovider.ErrUnsupported
			}
		}
		if expectedGeneration != nil && *expectedGeneration != b.Box.Generation {
			return apiError("STALE_GENERATION", "Lifecycle request targets another Box generation")
		}
		if err = busy(tx, b); err != nil {
			return err
		}
		if action == "resume" || action == "activate" {
			if err := requireAvailableBoxVolumes(tx, b); err != nil {
				return err
			}
		}
		active, err := tx.HasActiveExecution(id)
		if err != nil {
			return err
		}
		if active {
			return apiError("BUSY", "Box has an active execution")
		}
		if action != "destroy" && b.Staged && (b.Box.Phase == "failed" || !b.RestoreComplete) {
			return apiError("CONFLICT", "Failed or incomplete restore candidate must be discarded")
		}
		switch action {
		case "suspend":
			if _, ok := s.providers[b.Profile.Provider].(boxprovider.TransitionProvider); !ok {
				return boxprovider.ErrUnsupported
			}
			if b.Box.Phase != "running" {
				return apiError("CONFLICT", "Only a running box can suspend")
			}
		case "resume":
			if b.Box.Phase != "suspended" {
				return apiError("CONFLICT", "Only a suspended box can resume")
			}
			if _, ok := s.providers[b.Profile.Provider].(boxprovider.ResumeRequestProvider); !ok {
				return boxprovider.ErrUnsupported
			}
			if _, ok := s.providers[b.Profile.Provider].(boxprovider.CompletionObservationProvider); !ok {
				return boxprovider.ErrUnsupported
			}
		case "activate":
			if !b.Staged || !b.RestoreComplete || b.Box.Phase != "staged" {
				return apiError("CONFLICT", "Box is not a staged candidate")
			}
		case "destroy":
			if b.Box.Phase == "deleted" {
				return apiError("CONFLICT", "Box was already deleted")
			}
			if err := s.registerLegacyVolume(tx, &b); err != nil {
				return err
			}
		}
		b.Box.OperationID = op.ID
		b.Box.Error = nil
		b.Box.Version++
		if phase := map[string]string{"destroy": "deleting", "suspend": "suspending", "resume": "resuming"}[action]; phase != "" {
			b.Box.Phase = phase
		}
		return tx.PutBox(b)
	})
	if err != nil {
		return op, err
	}
	if action == "resume" {
		s.launchNodeResume(ctx, op, b, s.providers[b.Profile.Provider].(boxprovider.ResumeRequestProvider))
		return op, nil
	}
	s.launchWithCommitContext(ctx, op, func(ctx context.Context) (map[string]string, error) {
		p := s.providers[b.Profile.Provider]
		switch action {
		case "destroy":
			if b.Handle.ID != "" {
				if err := p.Destroy(ctx, b.Handle); err != nil {
					return nil, err
				}
				if _, err := s.waitState(ctx, id, "deleted", false); err != nil {
					return nil, err
				}
			} else if err := s.store.ChangeBox(ctx, id, func(b *boxRecord) error { b.Box.Phase = "deleted"; b.Box.Version++; return nil }); err != nil {
				return nil, err
			}
		case "activate":
			if err := s.guestAction(ctx, b, "/v1/activate"); err != nil {
				return nil, err
			}
			if err := s.setInventoryStage(ctx, b, "running"); err != nil {
				return nil, err
			}
		case "suspend":
			if err := s.quiesceGuest(ctx, b); err != nil {
				slog.Error("sandbox quiesce failed", "box", id, "error", err)
				return nil, err
			}
			transition := p.(boxprovider.TransitionProvider)
			if err := transition.ActionTransition(ctx, b.Handle, "suspend", b.TransitionID, op.ID); err != nil {
				return nil, err
			}
			if _, err := s.waitStateTransition(ctx, id, "suspended", false, op.ID); err != nil {
				return nil, err
			}
		}
		return map[string]string{"boxId": id}, nil
	}, func(tx *storeTx) error {
		if action != "activate" {
			return nil
		}
		current, err := tx.Box(client, id)
		if err != nil {
			return err
		}
		if current.Box.OperationID != op.ID {
			return boxprovider.ErrStaleTransition
		}
		current.Staged = false
		current.Box.Phase = "running"
		current.Box.Version++
		return tx.PutBox(current)
	})
	return op, nil
}

type execInput struct {
	guestapi.ExecRequest
	ExpectedGeneration uint64 `json:"expectedGeneration"`
}

func (s *Service) execute(client, id string, input execInput) (Operation, error) {
	if len(input.Argv) == 0 || len(input.Argv) > 128 {
		return Operation{}, apiError("INVALID_REQUEST", "argv must contain 1..128 arguments")
	}
	if input.TimeoutMS == 0 {
		input.TimeoutMS = 30000
	}
	if input.TimeoutMS < 1 || input.TimeoutMS > 300000 {
		return Operation{}, apiError("INVALID_REQUEST", "timeoutMs must be 1..300000")
	}
	var b boxRecord
	eid := ""
	op, err := s.prepareOperation(client, "exec", id, func(tx *storeTx, op *Operation) error {
		var err error
		b, err = tx.Box(client, id)
		if err != nil {
			return err
		}
		if err = busy(tx, b); err != nil {
			return err
		}
		if b.Box.Phase != "running" && b.Box.Phase != "staged" {
			return apiError("CONFLICT", "Box is not ready for execution")
		}
		if input.ExpectedGeneration == 0 || input.ExpectedGeneration != b.Box.Generation {
			return apiError("STALE_GENERATION", "expectedGeneration must match the current box generation")
		}
		eid = randomID("exec-")
		if err := tx.PutExecution(executionRecord{Execution: Execution{ID: eid, BoxID: id, OperationID: op.ID, State: "running"}}); err != nil {
			return err
		}
		op.Result = map[string]string{"execId": eid}
		return nil
	})
	if err != nil {
		return op, err
	}
	s.launch(op, func(ctx context.Context) (map[string]string, error) {
		ctx, cancel := context.WithTimeout(ctx, time.Duration(input.TimeoutMS)*time.Millisecond+5*time.Second)
		defer cancel()
		body, _ := json.Marshal(input.ExecRequest)
		res, err := s.guestRequest(ctx, b, "POST", "/v1/exec", strings.NewReader(string(body)))
		var result guestapi.ExecResult
		if err == nil {
			defer res.Body.Close()
			err = guestSuccess(res)
			if err == nil {
				err = json.NewDecoder(io.LimitReader(res.Body, 4<<20)).Decode(&result)
			}
		}
		saveCtx, stopSave := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stopSave()
		saveErr := s.store.Write(saveCtx, func(tx *storeTx) error {
			e, readErr := tx.Execution(eid)
			if readErr != nil {
				return readErr
			}
			if err != nil {
				e.State = "unknown"
			} else {
				e.State = "exited"
				e.Result = &result
			}
			return tx.PutExecution(e)
		})
		if err == nil {
			err = saveErr
		}
		return map[string]string{"execId": eid}, err
	})
	return op, nil
}

func mustBoxJSON(box Box) json.RawMessage { data, _ := json.Marshal(box); return data }

func (s *Service) setInventoryStage(ctx context.Context, b boxRecord, stage string) error {
	if p, ok := s.providers[b.Profile.Provider].(inventoryStageProvider); ok {
		return p.SetInventoryStage(ctx, b.Handle, stage)
	}
	return nil
}

type inventoryStageProvider interface {
	SetInventoryStage(context.Context, boxprovider.Handle, string) error
}

func supportsStaging(provider string, p boxprovider.Provider) bool {
	if provider == "resumable-k8s-pod" {
		return true
	}
	if p == nil || provider != "local-runsc" {
		return false
	}
	_, ok := p.(inventoryStageProvider)
	return ok
}

func supportsWorkspaceArchives(provider string, p boxprovider.Provider) bool {
	if provider == "resumable-k8s-pod" {
		return true
	}
	if p == nil || provider != "local-runsc" {
		return false
	}
	_, ok := p.(boxprovider.WorkspaceArchiveProvider)
	return ok
}

func supportsProtectedTools(profile Profile) bool {
	return profile.Provider == "resumable-k8s-pod" || profile.Provider == "local-runsc"
}
