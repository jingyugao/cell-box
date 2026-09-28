package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
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
	config    Config
	store     *Store
	providers map[string]boxprovider.Provider
	http      *http.Client
	toolHTTP  *http.Client
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.Mutex
	streams   map[string]activeStream
}
type activeStream struct {
	grantID, boxID string
	cancel         context.CancelFunc
}

func New(config Config, providers map[string]boxprovider.Provider) (*Service, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	for _, p := range config.Profiles {
		if providers[p.Provider] == nil {
			return nil, fmt.Errorf("provider %s is not configured", p.Provider)
		}
	}
	store, err := OpenStore(config.DataDir)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	s := &Service{config: config, store: store, providers: providers,
		http:     &http.Client{Transport: &http.Transport{Proxy: nil, ResponseHeaderTimeout: 35 * time.Second}, CheckRedirect: noRedirect},
		toolHTTP: &http.Client{Transport: &http.Transport{Proxy: nil, ResponseHeaderTimeout: 5*time.Minute + 5*time.Second}, CheckRedirect: noRedirect},
		ctx:      ctx, cancel: cancel, streams: map[string]activeStream{}}
	err = store.Update(func(st *State) error {
		now := time.Now().UTC()
		for id, record := range st.Operations {
			if record.Operation.Status == "queued" || record.Operation.Status == "running" {
				record.Operation.Status = "failed"
				record.Operation.Version++
				record.Operation.Error = &APIError{Code: "OPERATION_INTERRUPTED", Message: "Service restarted during this operation; inspect the resource before retrying"}
				record.Operation.FinishedAt = &now
				st.Operations[id] = record
				for bid, b := range st.Boxes {
					if b.Box.OperationID == id {
						b.Box.OperationID = ""
						b.Box.Version++
						if b.Box.State == "provisioning" || b.Box.State == "restoring" {
							b.Box.State = "failed"
							b.Box.Error = record.Operation.Error
						} else {
							b.Box.State = "unknown"
						}
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
	return s, nil
}
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
func token() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
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
	return Capabilities{Exec: true, Files: true, HTTP: true, WebSocket: true, Freeze: p.Provider == "docker", Suspend: map[bool]string{true: "same-node-checkpoint", false: "none"}[p.Provider == "resumable-k8s-pod"], Archives: "workspace-best-effort", ProtectedTools: len(p.Guest.Tools) > 0}
}
func owned(st *State, client, id string) (boxRecord, error) {
	b, ok := st.Boxes[id]
	if !ok || b.ClientID != client {
		return b, apiError("NOT_FOUND", "Box not found")
	}
	return b, nil
}
func (s *Service) box(client, id string) (boxRecord, error) {
	var b boxRecord
	err := s.store.View(func(st State) error { var err error; b, err = owned(&st, client, id); return err })
	return b, err
}
func (s *Service) profile(client, id string) (Profile, error) {
	for _, p := range s.config.Profiles {
		if p.ID == id {
			for _, c := range p.Clients {
				if c == client {
					return p, nil
				}
			}
		}
	}
	return Profile{}, apiError("NOT_FOUND", "Profile not found")
}
func (s *Service) client(id string) Client {
	for _, c := range s.config.Clients {
		if c.ID == id {
			return c
		}
	}
	return Client{}
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
	s.mu.Lock()
	defer s.mu.Unlock()
	key = client + ":" + kind + ":" + target + ":" + key
	digest := inputHash(input)
	var operation Operation
	fresh := false
	err := s.store.Update(func(st *State) error {
		if previous, ok := st.Keys[key]; ok {
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
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithTimeout(s.ctx, 15*time.Minute)
		defer cancel()
		result, err := work(ctx)
		saveErr := s.store.Update(func(st *State) error {
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
				if err != nil {
					b.Box.Error = cloneError(err)
					if op.Kind == "create" || op.Kind == "restore" {
						b.Box.State = "failed"
					} else if op.Kind != "exec" && op.Kind != "archive" {
						b.Box.State = "unknown"
					}
				}
				st.Boxes[op.TargetID] = b
			}
			return nil
		})
		if saveErr != nil {
			fmt.Printf("Cellbox could not persist operation completion %s\n", op.ID)
		}
	}()
}
func runtimeSpec(b boxRecord) boxprovider.Spec {
	config := b.Profile.Guest
	if b.Profile.DebugReadWriteHostPath != "" {
		config.DebugHome = "/home/debug"
	}
	return boxprovider.Spec{BoxID: b.Box.ID, Image: b.Profile.Image, Config: config, CPU: b.Profile.CPU, MemoryMiB: b.Profile.MemoryMiB, Namespace: b.Profile.Namespace, NodeName: b.Profile.NodeName, DebugReadOnlyHostPath: b.Profile.DebugReadOnlyHostPath, DebugReadWriteHostPath: b.Profile.DebugReadWriteHostPath, Staged: b.Staged}
}
func (s *Service) rawBox(id string) (boxRecord, error) {
	var b boxRecord
	err := s.store.View(func(st State) error {
		var ok bool
		b, ok = st.Boxes[id]
		if !ok {
			return apiError("NOT_FOUND", "Box not found")
		}
		return nil
	})
	return b, err
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
	if b.Handle.ID == "" {
		return b.Box, nil
	}
	ob, err := s.providers[b.Profile.Provider].Inspect(ctx, b.Handle)
	if err != nil && !errors.Is(err, boxprovider.ErrNotFound) {
		return b.Box, err
	}
	if errors.Is(err, boxprovider.ErrNotFound) {
		ob.State = "deleted"
	}
	if ob.State == "ready" {
		// A running container is ready only after the authenticated guest is healthy.
		conn, e := s.providers[b.Profile.Provider].Guest(ctx, b.Handle)
		if e == nil {
			var req *http.Request
			req, e = http.NewRequestWithContext(ctx, "GET", strings.TrimRight(conn.URL, "/")+"/healthz", nil)
			if e == nil {
				req.Header.Set("Authorization", "Bearer "+conn.Token)
				var res *http.Response
				res, e = s.http.Do(req)
				if e == nil {
					res.Body.Close()
					if res.StatusCode != 200 {
						e = fmt.Errorf("guest unhealthy")
					}
				}
			}
		}
		if e != nil {
			ob.State = "unavailable"
		}
	}
	var out Box
	err = s.store.Update(func(st *State) error {
		current := st.Boxes[b.Box.ID]
		if current.Box.Version != b.Box.Version || current.Handle != b.Handle || current.Box.OperationID != b.Box.OperationID {
			out = current.Box
			return nil
		}
		state := ob.State
		if current.Box.State == "deleting" && state != "deleted" {
			state = "deleting"
		}
		if state == "ready" && current.Staged {
			state = "staged"
			if current.Box.State == "failed" {
				state = "failed"
			} else if !current.RestoreComplete {
				state = "restoring"
			}
		}
		if state == "deleted" && current.Box.State != "deleting" && current.Box.State != "deleted" {
			state = "failed"
			current.Box.Error = &APIError{Code: "RUNTIME_LOST", Message: "Runtime disappeared; automatic cold restart is disabled"}
		}
		changed := current.Box.State != state
		if ob.ExecutionID != "" && current.ExecutionID != ob.ExecutionID {
			current.ExecutionID = ob.ExecutionID
			current.Box.Generation++
			changed = true
		}
		current.Box.State = state
		if changed {
			current.Box.Version++
		}
		st.Boxes[b.Box.ID] = current
		out = current.Box
		return nil
	})
	return out, err
}
func (s *Service) connection(ctx context.Context, b boxRecord) (boxprovider.Connection, error) {
	p := s.providers[b.Profile.Provider]
	check := func() error {
		ob, err := p.Inspect(ctx, b.Handle)
		if err != nil {
			return err
		}
		if ob.State != "ready" {
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
	conn, err := s.connection(ctx, b)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(conn.URL, "/")+path, body)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Authorization", "Bearer "+conn.Token)
	r.Header.Set("Content-Type", "application/json")
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
		if err == nil && box.State == want {
			if want != "ready" && want != "staged" && want != "restoring" {
				return box, nil
			}
			updated, _ := s.rawBox(id)
			res, e := s.guestRequest(ctx, updated, "GET", "/healthz", nil)
			if e == nil {
				res.Body.Close()
				if res.StatusCode == 200 {
					return box, nil
				}
			}
		}
		if err == nil && box.State == "failed" {
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
	handle, err := s.providers[b.Profile.Provider].Create(ctx, runtimeSpec(b))
	if err != nil {
		return Box{}, err
	}
	if err = s.saveHandle(id, handle); err != nil {
		return Box{}, err
	}
	want := "ready"
	if b.Staged {
		want = "staged"
		if !b.RestoreComplete {
			want = "restoring"
		}
	}
	return s.waitState(ctx, id, want)
}

type createRequest struct {
	ProfileID string `json:"profileId"`
	OwnerKey  string `json:"ownerKey"`
}

func (s *Service) create(client, key string, input createRequest, archiveID string) (Operation, error) {
	if input.OwnerKey == "" || len(input.OwnerKey) > 256 {
		return Operation{}, apiError("INVALID_REQUEST", "ownerKey must contain 1..256 bytes")
	}
	profile, err := s.profile(client, input.ProfileID)
	if err != nil {
		return Operation{}, err
	}
	kind := "create"
	if archiveID != "" {
		kind = "restore"
	}
	var source Archive
	op, fresh, err := s.prepareOperation(client, key, kind, "", struct {
		createRequest
		ArchiveID string
	}{input, archiveID}, func(st *State, op *Operation) error {
		if archiveID != "" {
			a, ok := st.Archives[archiveID]
			if !ok || a.ClientID != client {
				return apiError("NOT_FOUND", "Archive not found")
			}
			source = a.Archive
			if source.Agent != profile.Guest.Agent {
				return apiError("ARCHIVE_INCOMPATIBLE", "Archive agent UID/GID differs from target profile")
			}
		}
		id := randomID("box-")
		op.TargetID = id
		if archiveID != "" {
			op.Result = map[string]string{"archiveId": archiveID}
		}
		state := "provisioning"
		if archiveID != "" {
			state = "restoring"
		}
		st.Boxes[id] = boxRecord{Box: Box{ID: id, OwnerKey: input.OwnerKey, ProfileID: profile.ID, State: state, Version: 1, Image: profile.Image, Workspace: profile.Guest.Workspace, Capabilities: profileCapabilities(profile), OperationID: op.ID, CreatedAt: time.Now().UTC()}, ClientID: client, Profile: profile, Staged: archiveID != "", RestoreArchiveID: archiveID}
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
				v.Box.State = "staged"
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
		if b.Box.State == "deleted" && action != "destroy" {
			return apiError("CONFLICT", "Box was deleted")
		}
		if b.Staged && (b.Box.State == "failed" || !b.RestoreComplete) && action != "destroy" {
			return apiError("CONFLICT", "Failed or incomplete restore candidate must be discarded")
		}
		if (action == "suspend" || action == "resume") && b.Profile.Provider != "resumable-k8s-pod" {
			return boxprovider.ErrUnsupported
		}
		if (action == "freeze" || action == "unfreeze") && b.Profile.Provider != "docker" {
			return boxprovider.ErrUnsupported
		}
		if action == "activate" && (!b.Staged || !b.RestoreComplete || b.Box.State != "staged") {
			return apiError("CONFLICT", "Box is not a staged candidate")
		}
		if action == "suspend" && b.Box.State != "ready" {
			return apiError("CONFLICT", "Only a ready box can suspend")
		}
		if action == "resume" && b.Box.State != "suspended" {
			return apiError("CONFLICT", "Only a suspended box can resume")
		}
		if action == "freeze" && b.Box.State != "ready" {
			return apiError("CONFLICT", "Only a ready box can freeze")
		}
		if action == "unfreeze" && b.Box.State != "frozen" {
			return apiError("CONFLICT", "Only a frozen box can unfreeze")
		}
		b.Box.OperationID = op.ID
		b.Box.Error = nil
		if action == "destroy" {
			b.Box.State = "deleting"
		}
		b.Box.Version++
		st.Boxes[id] = b
		return nil
	})
	if err != nil || !fresh {
		return op, err
	}
	s.launch(op, func(ctx context.Context) (map[string]string, error) {
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
				if err := s.store.Update(func(st *State) error { v := st.Boxes[id]; v.Box.State = "deleted"; st.Boxes[id] = v; return nil }); err != nil {
					return nil, err
				}
			}
		case "activate":
			if err := s.guestAction(ctx, b, "/v1/activate"); err != nil {
				return nil, err
			}
			if err := s.store.Update(func(st *State) error {
				v := st.Boxes[id]
				v.Staged = false
				v.Box.State = "ready"
				st.Boxes[id] = v
				return nil
			}); err != nil {
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
			want := map[string]string{"freeze": "frozen", "unfreeze": "ready", "suspend": "suspended", "resume": "ready"}[action]
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
		if b.Box.State != "ready" && b.Box.State != "staged" {
			return apiError("CONFLICT", "Box is not ready for execution")
		}
		if input.ExpectedGeneration == 0 || input.ExpectedGeneration != b.Box.Generation {
			return apiError("STALE_GENERATION", "expectedGeneration must match the current box generation")
		}
		eid = randomID("exec-")
		st.Executions[eid] = Execution{ID: eid, BoxID: id, OperationID: op.ID, State: "running"}
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
