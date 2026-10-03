package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/inventory"
	"cellbox.local/cellbox/internal/objectstorage"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Service struct {
	objects   objectstorage.Objects
	config    Config
	store     *Store
	providers map[string]boxprovider.Provider
	http      *http.Client
	toolHTTP  *http.Client
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.Mutex
	gcMu      sync.Mutex
	streams   map[string]activeStream
}
type activeStream struct {
	boxID  string
	cancel context.CancelFunc
}

func New(config Config, providers map[string]boxprovider.Provider) (*Service, error) {
	return NewContext(context.Background(), config, providers)
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
	var store *Store
	var objects objectstorage.Objects
	var err error
	if config.ObjectStorage != (objectstorage.Config{}) {
		objects, err = objectstorage.New(ctx, config.ObjectStorage)
		if err == nil {
			store, err = OpenObjectStore(ctx, objects)
		}
	} else {
		store, err = OpenStore(config.DataDir)
	}
	if err != nil {
		cancel()
		return nil, err
	}
	store.onFailure = cancel
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	s := &Service{objects: objects, config: config, store: store, providers: providers,
		http:     &http.Client{Transport: &http.Transport{Proxy: nil, ResponseHeaderTimeout: 35 * time.Second}, CheckRedirect: noRedirect},
		toolHTTP: &http.Client{Transport: &http.Transport{Proxy: nil, ResponseHeaderTimeout: 5*time.Minute + 5*time.Second}, CheckRedirect: noRedirect},
		ctx:      ctx, cancel: cancel, streams: map[string]activeStream{}}
	err = store.Update(func(st *State) error {
		st.Routes = map[string]Route{}
		now := time.Now().UTC()
		for id, record := range st.Operations {
			if record.Operation.Status == "queued" || record.Operation.Status == "running" {
				if record.Operation.Kind == "image-delete" {
					if _, exists := st.ImportedImages[record.Operation.TargetID]; !exists {
						record.Operation.Status = "succeeded"
						record.Operation.Version++
						record.Operation.FinishedAt = &now
						if record.Operation.Result == nil {
							record.Operation.Result = map[string]string{"deleted": "true"}
						}
						st.Operations[id] = record
						continue
					}
				}
				record.Operation.Status = "failed"
				record.Operation.Version++
				record.Operation.Error = &APIError{Code: "OPERATION_INTERRUPTED", Message: "Service restarted during this operation; inspect the resource before retrying"}
				record.Operation.FinishedAt = &now
				st.Operations[id] = record
				for bid, b := range st.Boxes {
					if b.Box.OperationID == id {
						b.Box.OperationID = ""
						b.Box.Version++
						st.Boxes[bid] = b
					}
				}
				for eid, e := range st.Executions {
					if e.OperationID == id {
						e.State = "unknown"
						st.Executions[eid] = e
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		cancel()
		store.Close()
		return nil, err
	}
	if store.records != nil {
		var records []boxRecord
		err := store.View(func(st State) error {
			for _, b := range st.Boxes {
				records = append(records, b)
			}
			return nil
		})
		if err != nil {
			cancel()
			store.Close()
			return nil, err
		}
		// Bound total startup probing, rather than waiting ten seconds for each
		// unavailable runtime. Unobserved boxes stay unknown until the next GET.
		probe, stop := context.WithTimeout(ctx, 10*time.Second)
		jobs := make(chan boxRecord, len(records))
		for _, b := range records {
			if b.Profile.Provider == "resumable-k8s-pod" && b.Handle.ID != "" {
				jobs <- b
			}
		}
		close(jobs)
		var probes sync.WaitGroup
		for n := 0; n < min(8, len(records)); n++ {
			probes.Add(1)
			go func() {
				defer probes.Done()
				for b := range jobs {
					if probe.Err() != nil {
						return
					}
					_, _ = s.observe(probe, b)
				}
			}()
		}
		probes.Wait()
		stop()
	}
	if err := s.pruneMetadata(time.Now().UTC()); err != nil {
		cancel()
		store.Close()
		return nil, err
	}
	s.startRetention()
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
	return s.store.Close()
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
func inputHash(input any) string { b, _ := json.Marshal(input); return hash(string(b)) }
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
	if errors.Is(err, context.DeadlineExceeded) {
		return &APIError{Code: "TIMEOUT", Message: "Operation deadline exceeded; inspect resource state"}
	}
	return &APIError{Code: "RUNTIME_ERROR", Message: "Runtime operation failed; inspect Cellbox and provider health"}
}
func profileCapabilities(p Profile) Capabilities {
	return Capabilities{Exec: true, Files: true, HTTP: true, WebSocket: true, Freeze: p.Provider == "docker", Suspend: map[bool]string{true: "same-node-checkpoint", false: "none"}[p.Provider == "resumable-k8s-pod"], Archives: "workspace-best-effort", ProtectedTools: len(p.Guest.Tools) > 0, CredentialBatch: true, InternalServices: true, RootDebug: p.Guest.Debug.UID == 0 && p.Guest.Debug.GID == 0, SharedDirectory: p.SharedReadOnlyHostPath != ""}
}
func owned(st *State, client, id string) (boxRecord, error) {
	b, ok := st.Boxes[id]
	if !ok || b.ClientID != client {
		return b, apiError("NOT_FOUND", "Box not found")
	}
	return b, nil
}
func (s *Service) box(client, id string) (boxRecord, error) {
	records, err := s.store.BoxRecords(client, []string{id})
	if err != nil {
		return boxRecord{}, err
	}
	return records[0], nil
}
func (s *Service) profile(client, id string) (Profile, error) {
	for _, p := range s.config.Profiles {
		if p.ID == id {
			return p, nil
		}
	}
	return Profile{}, apiError("NOT_FOUND", "Profile not found")
}
func busy(st *State, b boxRecord, leases bool) error {
	if b.Box.OperationID != "" {
		if op, ok := st.Operations[b.Box.OperationID]; ok && (op.Operation.Status == "queued" || op.Operation.Status == "running") {
			return apiError("BUSY", "Box has an active operation")
		}
	}
	if leases {
		for _, lease := range st.Leases {
			if lease.BoxID == b.Box.ID && lease.ExpiresAt.After(time.Now()) {
				return apiError("BUSY", "Box has active usage leases")
			}
		}
	}
	return nil
}
func activeExec(st *State, boxID string) bool {
	for _, record := range st.Operations {
		op := record.Operation
		if op.Kind == "exec" && op.TargetID == boxID && (op.Status == "queued" || op.Status == "running") {
			return true
		}
	}
	return false
}
func (s *Service) streamBusy(id string) error {
	for _, stream := range s.streams {
		if stream.boxID == id {
			return apiError("BUSY", "Box has active service requests")
		}
	}
	return nil
}
func (s *Service) prepareOperation(client, key, kind, target string, input any, prepare func(*State, *Operation) error) (Operation, bool, error) {
	if key == "" || len(key) > 200 {
		return Operation{}, false, apiError("INVALID_REQUEST", "Idempotency-Key header (1..200 bytes) is required")
	}
	started := time.Now()
	var operation Operation
	var err error
	defer func() {
		if operation.TargetID != "" {
			runtimeTiming(operation.TargetID, "cellbox.accept_"+kind, started, err)
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	key = client + ":" + kind + ":" + target + ":" + key
	digest := inputHash(input)
	fresh := false
	err = s.store.Update(func(st *State) error {
		if previous, ok := st.Keys[key]; ok {
			if previous.Expired {
				return apiError("OPERATION_EXPIRED", "Operation history has expired; this idempotency key cannot be reused")
			}
			if previous.Hash != digest {
				return apiError("CONFLICT", "Idempotency key was already used with different input")
			}
			operation = st.Operations[previous.OperationID].Operation
			return nil
		}
		if target != "" && kind != "exec" && kind != "archive" {
			if err := s.streamBusy(target); err != nil {
				return err
			}
		}
		operation = Operation{ID: randomID("op-"), Kind: kind, TargetID: target, Status: "running", Version: 1, CreatedAt: time.Now().UTC()}
		if err := prepare(st, &operation); err != nil {
			return err
		}
		st.Operations[operation.ID] = operationRecord{Operation: operation, ClientID: client}
		st.Keys[key] = keyRecord{Hash: digest, OperationID: operation.ID}
		fresh = true
		return nil
	})
	return operation, fresh, err
}
func (s *Service) launch(op Operation, work func(context.Context) (map[string]string, error)) {
	s.launchWithCommit(op, work, nil)
}

func (s *Service) launchWithCommit(op Operation, work func(context.Context) (map[string]string, error), commit func(*State) error) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithTimeout(s.ctx, 15*time.Minute)
		defer cancel()
		result, err := work(ctx)
		saveStarted := time.Now()
		saveErr := s.store.Update(func(st *State) error {
			if err == nil && commit != nil {
				err = commit(st)
			}
			record := st.Operations[op.ID]
			now := time.Now().UTC()
			record.Operation.FinishedAt = &now
			record.Operation.Version++
			record.Operation.Result = result
			if err != nil {
				record.Operation.Status = "failed"
				record.Operation.Error = cloneError(err)
			} else {
				record.Operation.Status = "succeeded"
			}
			st.Operations[op.ID] = record
			if b, ok := st.Boxes[op.TargetID]; ok && b.Box.OperationID == op.ID {
				b.Box.OperationID = ""
				b.Box.Version++
				st.Boxes[op.TargetID] = b
			}
			return nil
		})
		runtimeTiming(op.TargetID, "cellbox.persist_completion", saveStarted, saveErr)
		if saveErr != nil {
			fmt.Printf("Cellbox could not persist operation completion %s\n", op.ID)
		}
	}()
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
	return boxprovider.Spec{Inventory: metadata, BoxID: b.Box.ID, Image: b.Profile.Image, Config: config, CPU: b.Profile.CPU, MemoryMiB: b.Profile.MemoryMiB, Namespace: b.Profile.Namespace, NodeName: b.Profile.NodeName, SharedReadOnlyHostPath: b.Profile.SharedReadOnlyHostPath, DebugReadOnlyHostPath: b.Profile.DebugReadOnlyHostPath, DebugReadWriteHostPath: b.Profile.DebugReadWriteHostPath, Staged: b.Staged}
}
func (s *Service) rawBox(id string) (boxRecord, error) {
	return s.store.BoxRecord(id)
}
func (s *Service) saveHandle(id string, handle boxprovider.Handle) error {
	return s.store.Update(func(st *State) error {
		b := st.Boxes[id]
		b.Handle = handle
		b.Box.ImageID = handle.ImageID
		b.Box.Version++
		st.Boxes[id] = b
		return nil
	})
}
func (s *Service) observe(ctx context.Context, b boxRecord) (Box, error) {
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
		updated, err := s.store.UpdateObservedBox(b, func(current *boxRecord) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			previousError := current.Box.Error
			phase := ob.Phase
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
			target := map[string]string{"freezing": "frozen", "unfreezing": "running", "suspending": "suspended", "resuming": "running"}[current.Box.Phase]
			if current.Box.OperationID != "" && target != "" && phase != target && phase != "failed" && phase != "deleted" {
				phase = current.Box.Phase
			}
			changed := current.Box.Phase != phase
			if previousError != nil && current.Box.Error != nil {
				changed = changed || *previousError != *current.Box.Error
			} else {
				changed = changed || previousError != current.Box.Error
			}
			if ob.ExecutionID != "" && current.ExecutionID != ob.ExecutionID {
				current.ExecutionID = ob.ExecutionID
				if ob.Generation > 0 {
					current.Box.Generation = ob.Generation
				} else {
					current.Box.Generation++
				}
				changed = true
			}
			current.Box.Phase = phase
			if changed {
				current.Box.Version++
			}
			return changed, nil
		})
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
		return updated.Box, nil
	}
	return Box{}, apiError("CONFLICT", "Box changed repeatedly while observing runtime")
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
func (s *Service) waitState(ctx context.Context, id, want string) (Box, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.config.StartupTimeoutSeconds)*time.Second)
	defer cancel()
	for {
		b, err := s.rawBox(id)
		if err != nil {
			return Box{}, err
		}
		box, err := s.observe(ctx, b)
		if err == nil && box.Phase == want {
			if want != "running" && want != "staged" && want != "restoring" {
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
		select {
		case <-ctx.Done():
			return Box{}, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
func (s *Service) provision(ctx context.Context, id string) (Box, error) {
	b, err := s.rawBox(id)
	if err != nil {
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
	box, err := s.waitState(ctx, id, want)
	runtimeTiming(id, "cellbox.wait_"+want, waitStarted, err)
	return box, err
}

type createRequest struct {
	ProfileID       string `json:"profileId"`
	OwnerKey        string `json:"ownerKey"`
	ImportedImageID string `json:"importedImageId,omitempty"`
	Staged          bool   `json:"staged,omitempty"`
}

func (s *Service) create(client, key string, input createRequest, archiveID string, acceptImageChange bool) (Operation, error) {
	if input.OwnerKey == "" || len(input.OwnerKey) > 256 {
		return Operation{}, apiError("INVALID_REQUEST", "ownerKey must contain 1..256 bytes")
	}
	profile, err := s.profile(client, input.ProfileID)
	if err != nil {
		return Operation{}, err
	}
	if archiveID != "" && input.ImportedImageID == "" && !acceptImageChange {
		err := s.store.View(func(st State) error {
			a, ok := st.Archives[archiveID]
			if !ok || a.ClientID != client || a.Deleting {
				return apiError("NOT_FOUND", "Archive not found")
			}
			input.ImportedImageID = a.Archive.ImportedImageID
			return nil
		})
		if err != nil {
			return Operation{}, err
		}
	}
	if input.ImportedImageID != "" {
		profile, err = s.importedProfile(client, input.ImportedImageID, profile)
		if err != nil {
			return Operation{}, err
		}
	}
	kind := "create"
	if archiveID != "" {
		kind = "restore"
	}
	var source Archive
	op, fresh, err := s.prepareOperation(client, key, kind, "", struct {
		createRequest
		ArchiveID         string
		AcceptImageChange bool
	}{input, archiveID, acceptImageChange}, func(st *State, op *Operation) error {
		if input.ImportedImageID != "" {
			record, ok := st.ImportedImages[input.ImportedImageID]
			if !ok || record.ClientID != client {
				return apiError("NOT_FOUND", "Imported image not found")
			}
			if record.ImportedImage.Deleting {
				return apiError("CONFLICT", "Imported image is being deleted")
			}
		}
		if archiveID != "" {
			a, ok := st.Archives[archiveID]
			if !ok || a.ClientID != client || a.Deleting {
				return apiError("NOT_FOUND", "Archive not found")
			}
			source = a.Archive
			if source.Agent != profile.Guest.Agent {
				return apiError("ARCHIVE_INCOMPATIBLE", "Archive agent UID/GID differs from target profile")
			}
			if source.ImageID != profile.Image && profile.Provider == "resumable-k8s-pod" && !(source.Portable && acceptImageChange) {
				return apiError("ARCHIVE_INCOMPATIBLE", "Archive image differs from target image")
			}
		}
		id := randomID("box-")
		op.TargetID = id
		if archiveID != "" {
			op.Result = map[string]string{"archiveId": archiveID}
		}
		state := "creating"
		if archiveID != "" {
			state = "restoring"
		}
		st.Boxes[id] = boxRecord{Box: Box{ID: id, OwnerKey: input.OwnerKey, ProfileID: profile.ID, Phase: state, Version: 1, Image: profile.Image, Workspace: profile.Guest.Workspace, Capabilities: profileCapabilities(profile), OperationID: op.ID, CreatedAt: time.Now().UTC()}, ClientID: client, Profile: profile, Staged: archiveID != "" || input.Staged, RestoreComplete: input.Staged && archiveID == "", RestoreArchiveID: archiveID, AcceptImageChange: acceptImageChange}
		record := st.Boxes[id]
		record.Box.ImportedImageID = input.ImportedImageID
		st.Boxes[id] = record
		return nil
	})
	if err != nil || !fresh {
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
			if err = s.store.Update(func(st *State) error {
				v := st.Boxes[box.ID]
				v.RestoreComplete = true
				v.Box.Phase = "staged"
				v.Box.Version++
				st.Boxes[box.ID] = v
				return nil
			}); err != nil {
				return nil, err
			}
		}
		return map[string]string{"boxId": box.ID}, nil
	})
	return op, nil
}
func (s *Service) action(client, key, id, action string) (Operation, error) {
	allowed := map[string]bool{"freeze": true, "unfreeze": true, "suspend": true, "resume": true, "destroy": true, "activate": true, "reconcile": true}
	if !allowed[action] {
		return Operation{}, apiError("INVALID_REQUEST", "Unknown lifecycle action")
	}
	var b boxRecord
	op, fresh, err := s.prepareOperation(client, key, action, id, nil, func(st *State, op *Operation) error {
		var err error
		b, err = owned(st, client, id)
		if err != nil {
			return err
		}
		if err = busy(st, b, true); err != nil {
			return err
		}
		if activeExec(st, id) {
			return apiError("BUSY", "Box has an active execution")
		}
		if b.Box.Phase == "deleted" && action != "destroy" {
			return apiError("CONFLICT", "Box was deleted")
		}
		if b.Staged && (b.Box.Phase == "failed" || !b.RestoreComplete) && action != "destroy" {
			return apiError("CONFLICT", "Failed or incomplete restore candidate must be discarded")
		}
		if (action == "suspend" || action == "resume") && b.Profile.Provider != "resumable-k8s-pod" {
			return boxprovider.ErrUnsupported
		}
		if (action == "freeze" || action == "unfreeze") && b.Profile.Provider != "docker" {
			return boxprovider.ErrUnsupported
		}
		if action == "activate" && (!b.Staged || !b.RestoreComplete || b.Box.Phase != "staged") {
			return apiError("CONFLICT", "Box is not a staged candidate")
		}
		if action == "suspend" && b.Box.Phase != "running" {
			return apiError("CONFLICT", "Only a running box can suspend")
		}
		if action == "resume" && b.Box.Phase != "suspended" {
			return apiError("CONFLICT", "Only a suspended box can resume")
		}
		if action == "freeze" && b.Box.Phase != "running" {
			return apiError("CONFLICT", "Only a running box can freeze")
		}
		if action == "unfreeze" && b.Box.Phase != "frozen" {
			return apiError("CONFLICT", "Only a frozen box can unfreeze")
		}
		b.Box.OperationID = op.ID
		b.Box.Error = nil
		if phase := map[string]string{"destroy": "deleting", "freeze": "freezing", "unfreeze": "unfreezing", "suspend": "suspending", "resume": "resuming"}[action]; phase != "" {
			b.Box.Phase = phase
		}
		b.Box.Version++
		st.Boxes[id] = b
		return nil
	})
	if err != nil || !fresh {
		return op, err
	}
	s.launchWithCommit(op, func(ctx context.Context) (map[string]string, error) {
		p := s.providers[b.Profile.Provider]
		switch action {
		case "destroy":
			if b.Handle.ID != "" {
				if err := p.Destroy(ctx, b.Handle); err != nil {
					return nil, err
				}
				if _, err := s.waitState(ctx, id, "deleted"); err != nil {
					return nil, err
				}
			} else {
				if err := s.store.Update(func(st *State) error {
					v := st.Boxes[id]
					v.Box.Phase = "deleted"
					v.Box.Version++
					st.Boxes[id] = v
					return nil
				}); err != nil {
					return nil, err
				}
			}
		case "activate":
			if err := s.guestAction(ctx, b, "/v1/activate"); err != nil {
				return nil, err
			}
			if err := s.setInventoryStage(ctx, b, "running"); err != nil {
				return nil, err
			}
		case "reconcile":
			if b.Staged && !b.RestoreComplete {
				return nil, apiError("CONFLICT", "Failed or interrupted restore candidate must be discarded")
			}
			if b.Handle.ID == "" {
				if _, err := s.provision(ctx, id); err != nil {
					return nil, err
				}
			} else {
				if _, err := s.observe(ctx, b); err != nil {
					return nil, err
				}
			}
		default:
			if action == "suspend" {
				if err := s.guestAction(ctx, b, "/v1/quiesce"); err != nil {
					return nil, err
				}
			}
			if err := p.Action(ctx, b.Handle, action); err != nil {
				if action == "suspend" {
					_ = s.guestAction(ctx, b, "/v1/unquiesce")
				}
				return nil, err
			}
			want := map[string]string{"freeze": "frozen", "unfreeze": "running", "suspend": "suspended", "resume": "running"}[action]
			if _, err := s.waitState(ctx, id, want); err != nil {
				return nil, err
			}
			if action == "resume" {
				updated, _ := s.rawBox(id)
				if err := s.guestAction(ctx, updated, "/v1/unquiesce"); err != nil {
					return nil, err
				}
			}
		}
		return map[string]string{"boxId": id}, nil
	}, func(st *State) error {
		if action != "activate" {
			return nil
		}
		v, ok := st.Boxes[id]
		if !ok || v.Box.OperationID != op.ID {
			return apiError("CONFLICT", "Box changed during activation")
		}
		// Activation and operation completion become visible in one commit.
		v.Staged = false
		v.Box.Phase = "running"
		v.Box.Version++
		st.Boxes[id] = v
		return nil
	})
	return op, nil
}

type execInput struct {
	guestapi.ExecRequest
	ExpectedGeneration uint64 `json:"expectedGeneration"`
}

func (s *Service) execute(client, key, id string, input execInput) (Operation, error) {
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
	op, fresh, err := s.prepareOperation(client, key, "exec", id, input, func(st *State, op *Operation) error {
		var err error
		b, err = owned(st, client, id)
		if err != nil {
			return err
		}
		if err = busy(st, b, false); err != nil {
			return err
		}
		if b.Box.Phase != "running" && b.Box.Phase != "staged" {
			return apiError("CONFLICT", "Box is not ready for execution")
		}
		if input.ExpectedGeneration == 0 || input.ExpectedGeneration != b.Box.Generation {
			return apiError("STALE_GENERATION", "expectedGeneration must match the current box generation")
		}
		eid = randomID("exec-")
		st.Executions[eid] = executionRecord{Execution: Execution{ID: eid, BoxID: id, OperationID: op.ID, State: "running"}}
		op.Result = map[string]string{"execId": eid}
		return nil
	})
	if err != nil || !fresh {
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
		saveErr := s.store.Update(func(st *State) error {
			e := st.Executions[eid]
			if err != nil {
				e.State = "unknown"
			} else {
				e.State = "exited"
				e.Result = &result
			}
			st.Executions[eid] = e
			return nil
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
	if p, ok := s.providers[b.Profile.Provider].(interface {
		SetInventoryStage(context.Context, boxprovider.Handle, string) error
	}); ok {
		return p.SetInventoryStage(ctx, b.Handle, stage)
	}
	return nil
}
