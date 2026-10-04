package service

import (
	"bytes"
	"cellbox.local/cellbox/internal/guestapi"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type clientContextKey struct{}

func clientID(r *http.Request) string {
	value, _ := r.Context().Value(clientContextKey{}).(string)
	return value
}
func (s *Service) scopeClient(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Cellbox-Client-ID")
		if id == "" {
			id = s.config.ClientID
		}
		if !validName.MatchString(id) {
			fail(w, apiError("INVALID_REQUEST", "Invalid client namespace"))
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), clientContextKey{}, id)))
	})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func fail(w http.ResponseWriter, err error) {
	e := cloneError(err)
	status := http.StatusBadGateway
	switch e.Code {
	case "INVALID_REQUEST":
		status = 400
	case "UNAUTHENTICATED":
		status = 401
	case "FORBIDDEN":
		status = 403
	case "NOT_FOUND":
		status = 404
	case "OPERATION_EXPIRED", "RESULT_EXPIRED":
		status = http.StatusGone
	case "CONFLICT", "BUSY", "STALE_GENERATION", "ARCHIVE_INCOMPATIBLE":
		status = 409
	case "UNSUPPORTED_CAPABILITY":
		status = 422
	case "TIMEOUT":
		status = 504
	}
	writeJSON(w, status, map[string]any{"error": e})
}
func decode(w http.ResponseWriter, r *http.Request, target any) error {
	return decodeLimit(w, r, target, 1<<20)
}
func decodeLimit(w http.ResponseWriter, r *http.Request, target any, limit int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return apiError("INVALID_REQUEST", "Invalid JSON request")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return apiError("INVALID_REQUEST", "Request must contain one JSON object")
	}
	return nil
}
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/profiles", s.listProfiles)
	mux.HandleFunc("POST /v1/images", s.buildImage)
	mux.HandleFunc("POST /v1/images:import", s.importImage)
	mux.HandleFunc("GET /v1/images", s.listImportedImages)
	mux.HandleFunc("GET /v1/images/{id}", s.getImportedImage)
	mux.HandleFunc("GET /v1/images/{id}/usage", s.imageUsageHandler)
	mux.HandleFunc("DELETE /v1/images/{id}", s.deleteImportedImage)
	mux.HandleFunc("POST /v1/boxes", func(w http.ResponseWriter, r *http.Request) {
		var input createRequest
		if err := decode(w, r, &input); err != nil {
			fail(w, err)
			return
		}
		op, err := s.create(clientID(r), r.Header.Get("Idempotency-Key"), input, "", false)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 202, op)
	})
	mux.HandleFunc("POST /v1/boxes:restore", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			createRequest
			ArchiveID         string `json:"archiveId"`
			AcceptImageChange bool   `json:"acceptImageChange,omitempty"`
		}
		if err := decode(w, r, &input); err != nil {
			fail(w, err)
			return
		}
		if input.ArchiveID == "" {
			fail(w, apiError("INVALID_REQUEST", "archiveId is required"))
			return
		}
		op, err := s.create(clientID(r), r.Header.Get("Idempotency-Key"), input.createRequest, input.ArchiveID, input.AcceptImageChange)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 202, op)
	})
	mux.HandleFunc("GET /v1/boxes", s.listBoxes)
	mux.HandleFunc("GET /v1/checkpoints", s.listCheckpoints)
	mux.HandleFunc("GET /v1/boxes/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		b, err := s.box(clientID(r), r.PathValue("id"))
		if err != nil {
			fail(w, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		box, err := s.observe(ctx, b)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 200, box)
	})
	mux.HandleFunc("POST /v1/boxes/{action}", func(w http.ResponseWriter, r *http.Request) {
		id, action, ok := strings.Cut(r.PathValue("action"), ":")
		if !ok {
			fail(w, apiError("NOT_FOUND", "Endpoint not found"))
			return
		}
		op, err := s.action(clientID(r), r.Header.Get("Idempotency-Key"), id, action)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 202, op)
	})
	mux.HandleFunc("POST /v1/boxes/{id}/execs", func(w http.ResponseWriter, r *http.Request) {
		var input execInput
		if err := decode(w, r, &input); err != nil {
			fail(w, err)
			return
		}
		op, err := s.execute(clientID(r), r.Header.Get("Idempotency-Key"), r.PathValue("id"), input)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 202, op)
	})
	mux.HandleFunc("GET /v1/execs/{id}", s.getExec)
	mux.HandleFunc("GET /v1/operations/{id}", s.getOperation)
	mux.HandleFunc("GET /v1/operations/{id}/events", s.operationEvents)
	mux.HandleFunc("POST /v1/boxes/{id}/leases", s.createLease)
	mux.HandleFunc("PATCH /v1/leases/{id}", s.renewLease)
	mux.HandleFunc("DELETE /v1/leases/{id}", s.releaseLease)
	mux.HandleFunc("GET /v1/boxes/{id}/files", s.files)
	mux.HandleFunc("PUT /v1/boxes/{id}/files", s.files)
	mux.HandleFunc("PUT /v1/boxes/{id}/credentials/{slot}", s.credentials)
	mux.HandleFunc("PUT /v1/boxes/{id}/credentials", s.credentialBatch)
	mux.HandleFunc("/v1/boxes/{id}/services/{port}/{path...}", s.internalService)
	mux.HandleFunc("POST /v1/boxes/{id}/tools/{tool}", s.tool)
	mux.HandleFunc("POST /v1/routes", s.createRoute)
	mux.HandleFunc("POST /v1/boxes/{id}/archives", s.captureArchive)
	mux.HandleFunc("GET /v1/archives", s.listArchives)
	mux.HandleFunc("GET /v1/archives/{id}", s.getArchive)
	mux.HandleFunc("GET /v1/archives/{id}/content", s.archiveContent)
	mux.HandleFunc("DELETE /v1/archives/{id}", s.deleteArchive)
	api := s.scopeClient(mux)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-s.ctx.Done():
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": &APIError{Code: "UNAVAILABLE", Message: "Cellbox service is stopping"}})
			return
		default:
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.URL.Path == "/healthz" {
			writeJSON(w, 200, map[string]string{"status": "ok"})
			return
		}
		if routeID := s.hostRoute(r.Host); routeID != "" {
			s.gateway(w, r, routeID, r.URL.Path)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/s/") {
			parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/s/"), "/", 2)
			tail := "/"
			if len(parts) == 2 {
				tail += parts[1]
			}
			s.gateway(w, r, parts[0], tail)
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/v1/") {
			fail(w, apiError("NOT_FOUND", "Endpoint not found"))
			return
		}
		api.ServeHTTP(w, r)
	})
}
func (s *Service) listProfiles(w http.ResponseWriter, r *http.Request) {
	out := []map[string]any{}
	for _, p := range s.config.Profiles {
		if _, err := s.profile(clientID(r), p.ID); err == nil {
			var runtime, behavior, kind string
			switch p.Provider {
			case "docker":
				runtime, behavior, kind = "docker", "normal", "docker-normal"
			case "resumable-k8s-pod":
				runtime, behavior, kind = "k8s", "resumable", "k8s-resumable"
			default:
				continue
			}
			out = append(out, map[string]any{"id": p.ID, "provider": p.Provider,
				"runtime": runtime, "behavior": behavior, "kind": kind,
				"image": p.Image, "workspace": p.Guest.Workspace, "agent": p.Guest.Agent,
				"cpu": p.CPU, "memoryMiB": p.MemoryMiB, "capabilities": profileCapabilities(p)})
		}
	}
	writeJSON(w, 200, out)
}
func (s *Service) listLocalBoxes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	ids := r.URL.Query()["id"]
	if len(ids) > 0 {
		seen := make(map[string]bool, len(ids))
		unique := make([]string, 0, len(ids))
		for _, id := range ids {
			if !seen[id] {
				seen[id] = true
				unique = append(unique, id)
			}
		}
		ids = unique
	}
	records, err := s.store.BoxRecords(clientID(r), ids)
	if err != nil {
		fail(w, err)
		return
	}
	boxes := make([]Box, len(records))
	workers := 8
	if len(records) < workers {
		workers = len(records)
	}
	if workers > 0 {
		jobs := make(chan int, len(records))
		for i := range records {
			jobs <- i
		}
		close(jobs)
		var wg sync.WaitGroup
		var once sync.Once
		var observeErr error
		for n := 0; n < workers; n++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range jobs {
					if ctx.Err() != nil {
						return
					}
					box, err := s.observe(ctx, records[i])
					if err != nil {
						once.Do(func() { observeErr = err; cancel() })
						return
					}
					boxes[i] = box
				}
			}()
		}
		wg.Wait()
		if observeErr != nil {
			fail(w, observeErr)
			return
		}
	}
	if err := ctx.Err(); err != nil {
		fail(w, err)
		return
	}
	filtered := make([]Box, 0, len(boxes))
	for _, box := range boxes {
		checkpoint := r.URL.Path == "/v1/checkpoints"
		if (checkpoint && box.Phase == "suspended") || !checkpoint {
			filtered = append(filtered, box)
		}
	}
	boxes = filtered
	sort.Slice(boxes, func(i, j int) bool {
		if boxes[i].CreatedAt.Equal(boxes[j].CreatedAt) {
			return boxes[i].ID < boxes[j].ID
		}
		return boxes[i].CreatedAt.Before(boxes[j].CreatedAt)
	})
	writeJSON(w, 200, boxes)
}
func (s *Service) operation(client, id string) (Operation, error) {
	return s.store.Operation(client, id)
}
func (s *Service) getOperation(w http.ResponseWriter, r *http.Request) {
	wait := 0
	if value := r.URL.Query().Get("waitMs"); value != "" {
		var err error
		wait, err = strconv.Atoi(value)
		if err != nil || wait < 0 || wait > 10000 {
			fail(w, apiError("INVALID_REQUEST", "waitMs must be 0..10000"))
			return
		}
	}
	op, err := s.waitOperation(r.Context(), clientID(r), r.PathValue("id"), time.Duration(wait)*time.Millisecond)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, op)
}
func (s *Service) getExec(w http.ResponseWriter, r *http.Request) {
	var e executionRecord
	err := s.store.View(func(st State) error {
		var ok bool
		e, ok = st.Executions[r.PathValue("id")]
		if !ok {
			return apiError("NOT_FOUND", "Execution not found")
		}
		_, err := owned(&st, clientID(r), e.BoxID)
		return err
	})
	if err != nil {
		fail(w, err)
		return
	}
	result, err := s.store.executionResult(r.Context(), e)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, result)
}
func (s *Service) operationEvents(w http.ResponseWriter, r *http.Request) {
	op, err := s.operation(clientID(r), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	f, ok := w.(http.Flusher)
	if !ok {
		fail(w, apiError("UNSUPPORTED_CAPABILITY", "Streaming is unavailable"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	version := uint64(0)
	for {
		changed := s.store.Changes()
		op, err = s.operation(clientID(r), op.ID)
		if err != nil {
			return
		}
		if op.Version != version {
			data, _ := json.Marshal(op)
			fmt.Fprintf(w, "id: %d\nevent: snapshot\ndata: %s\n\n", op.Version, data)
			f.Flush()
			version = op.Version
		}
		if op.Status == "succeeded" || op.Status == "failed" {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-s.ctx.Done():
			return
		case <-changed:
		case <-time.After(15 * time.Second):
			fmt.Fprint(w, ": keepalive\n\n")
			f.Flush()
		}
	}
}

type leaseInput struct {
	Purpose    string `json:"purpose"`
	TTLSeconds int    `json:"ttlSeconds"`
}

func leaseTTL(input *leaseInput) error {
	if input.TTLSeconds == 0 {
		input.TTLSeconds = 60
	}
	if input.TTLSeconds < 1 || input.TTLSeconds > 3600 || len(input.Purpose) > 256 {
		return apiError("INVALID_REQUEST", "Lease ttlSeconds must be 1..3600 and purpose at most 256 bytes")
	}
	return nil
}
func (s *Service) createLease(w http.ResponseWriter, r *http.Request) {
	var in leaseInput
	if err := decode(w, r, &in); err != nil {
		fail(w, err)
		return
	}
	if err := leaseTTL(&in); err != nil {
		fail(w, err)
		return
	}
	lease := Lease{ID: randomID("lease-"), BoxID: r.PathValue("id"), Purpose: in.Purpose, ExpiresAt: time.Now().Add(time.Duration(in.TTLSeconds) * time.Second)}
	err := s.store.Update(func(st *State) error {
		b, err := owned(st, clientID(r), lease.BoxID)
		if err != nil {
			return err
		}
		if err = busy(st, b, false); err != nil {
			return err
		}
		if b.Box.Phase != "running" {
			return apiError("CONFLICT", "Lease requires a ready box; request resume explicitly")
		}
		st.Leases[lease.ID] = lease
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 201, lease)
}
func (s *Service) renewLease(w http.ResponseWriter, r *http.Request) {
	var in leaseInput
	if err := decode(w, r, &in); err != nil {
		fail(w, err)
		return
	}
	if err := leaseTTL(&in); err != nil {
		fail(w, err)
		return
	}
	var out Lease
	err := s.store.Update(func(st *State) error {
		lease, ok := st.Leases[r.PathValue("id")]
		if !ok {
			return apiError("NOT_FOUND", "Lease not found")
		}
		b, err := owned(st, clientID(r), lease.BoxID)
		if err != nil {
			return err
		}
		if !lease.ExpiresAt.After(time.Now()) || b.Box.Phase != "running" {
			return apiError("CONFLICT", "Expired or inactive lease cannot be renewed")
		}
		lease.ExpiresAt = time.Now().Add(time.Duration(in.TTLSeconds) * time.Second)
		st.Leases[lease.ID] = lease
		out = lease
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, out)
}
func (s *Service) releaseLease(w http.ResponseWriter, r *http.Request) {
	err := s.store.Update(func(st *State) error {
		lease, ok := st.Leases[r.PathValue("id")]
		if !ok {
			return nil
		}
		if _, err := owned(st, clientID(r), lease.BoxID); err != nil {
			return err
		}
		delete(st.Leases, lease.ID)
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}

func (s *Service) beginIO(client, id string) (boxRecord, func(), error) {
	return s.beginIOWithPolicy(client, id, false)
}

func (s *Service) beginIOWithPolicy(client, id string, service bool) (boxRecord, func(), error) {
	// Share admission with lifecycle acceptance, including its durable commit.
	// Never hold the stream mutex while waiting for the metadata store: writers
	// inspect streams while holding the store lock.
	unlockAdmission := s.admissions.lock(id)
	defer unlockAdmission()
	key := randomID("io-")
	var b boxRecord
	err := s.store.View(func(st State) error {
		var err error
		b, err = owned(&st, client, id)
		if err != nil {
			return err
		}
		op := st.Operations[b.Box.OperationID].Operation
		if !service || (op.Kind != "exec" && op.Kind != "archive") {
			if err = busy(&st, b, false); err != nil {
				return err
			}
		}
		if b.Box.Phase != "running" && b.Box.Phase != "staged" {
			return apiError("CONFLICT", "Box is not ready")
		}
		return nil
	})
	if err != nil {
		return b, nil, err
	}
	s.mu.Lock()
	s.streams[key] = activeStream{boxID: id, cancel: func() {}}
	s.mu.Unlock()
	return b, func() { s.mu.Lock(); delete(s.streams, key); s.mu.Unlock() }, nil
}
func (s *Service) files(w http.ResponseWriter, r *http.Request) {
	b, release, err := s.beginIO(clientID(r), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	defer release()
	query := url.Values{"path": {r.URL.Query().Get("path")}}
	if r.URL.Query().Get("list") == "1" {
		query.Set("list", "1")
	}
	// File reads keep their lease for the entire stream, including cancellation.
	if (r.Method == "GET" || r.Method == "HEAD") && query.Get("list") != "1" {
		ctx, cancel := context.WithCancel(r.Context())
		stop := context.AfterFunc(s.ctx, cancel)
		defer stop()
		defer cancel()
		headers := make(http.Header)
		headers.Set("Accept-Encoding", "identity")
		for _, name := range []string{"Range", "If-Range", "If-Match", "If-Unmodified-Since", "If-None-Match", "If-Modified-Since"} {
			if value := r.Header.Get(name); value != "" {
				headers.Set(name, value)
			}
		}
		res, err := s.guestRequestHeaders(s.http, ctx, b, r.Method, "/v1/files?"+query.Encode(), nil, headers)
		if err != nil {
			fail(w, err)
			return
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusNotModified && res.StatusCode != http.StatusRequestedRangeNotSatisfiable && res.StatusCode != http.StatusPreconditionFailed {
			if err := guestSuccess(res); err != nil {
				fail(w, err)
				return
			}
		}
		for _, name := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges", "ETag", "Last-Modified", "X-Content-Type-Options", "Content-Security-Policy"} {
			if value := res.Header.Get(name); value != "" {
				w.Header().Set(name, value)
			}
		}
		w.Header().Set("Cache-Control", "private, no-cache")
		w.WriteHeader(res.StatusCode)
		if r.Method != "HEAD" {
			if _, err := io.Copy(w, res.Body); err != nil {
				panic(http.ErrAbortHandler)
			}
		}
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	r.Body = http.MaxBytesReader(w, r.Body, 16<<20)
	res, err := s.guestRequest(ctx, b, r.Method, "/v1/files?"+query.Encode(), r.Body)
	if err != nil {
		fail(w, err)
		return
	}
	defer res.Body.Close()
	if err = guestSuccess(res); err != nil {
		fail(w, err)
		return
	}
	if res.ContentLength > 16<<20 {
		fail(w, apiError("INVALID_REQUEST", "File exceeds the 16 MiB REST transfer limit"))
		return
	}
	content, readErr := io.ReadAll(io.LimitReader(res.Body, (16<<20)+1))
	if readErr != nil {
		fail(w, readErr)
		return
	}
	if len(content) > 16<<20 {
		fail(w, apiError("INVALID_REQUEST", "Response exceeds the 16 MiB REST transfer limit"))
		return
	}
	w.Header().Set("Content-Type", res.Header.Get("Content-Type"))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(res.StatusCode)
	_, _ = w.Write(content)
}
func (s *Service) credentials(w http.ResponseWriter, r *http.Request) {
	slot := r.PathValue("slot")
	if !validName.MatchString(slot) {
		fail(w, apiError("INVALID_REQUEST", "Invalid credential slot"))
		return
	}
	b, release, err := s.beginIO(clientID(r), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	defer release()
	declared := false
	for _, tool := range b.Profile.Guest.Tools {
		for _, name := range tool.CredentialEnv {
			if name == slot {
				declared = true
			}
		}
	}
	if !declared {
		fail(w, apiError("FORBIDDEN", "Credential slot is not declared by an admitted tool"))
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, guestapi.MaxCredentialBytes))
	if err != nil || len(data) == 0 {
		fail(w, apiError("INVALID_REQUEST", "Credential must contain 1..1048576 bytes"))
		return
	}
	defer clear(data)
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	res, err := s.guestRequest(ctx, b, "PUT", "/v1/credentials/"+slot, bytes.NewReader(data))
	if err != nil {
		fail(w, err)
		return
	}
	defer res.Body.Close()
	if err = guestSuccess(res); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(204)
}

func (s *Service) credentialBatch(w http.ResponseWriter, r *http.Request) {
	var input struct {
		guestapi.CredentialBatch
		ExpectedGeneration uint64 `json:"expectedGeneration"`
	}
	if err := decodeLimit(w, r, &input, 3<<20); err != nil {
		fail(w, err)
		return
	}
	defer func() {
		for _, data := range input.Slots {
			clear(data)
		}
	}()
	if err := input.CredentialBatch.Validate(); err != nil {
		fail(w, apiError("INVALID_REQUEST", err.Error()))
		return
	}
	b, release, err := s.beginIO(clientID(r), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	defer release()
	if input.ExpectedGeneration == 0 || input.ExpectedGeneration != b.Box.Generation {
		fail(w, apiError("STALE_GENERATION", "Credential batch targets another generation"))
		return
	}
	declared := map[string]bool{}
	for _, tool := range b.Profile.Guest.Tools {
		for _, slot := range tool.CredentialEnv {
			declared[slot] = true
		}
	}
	for slot := range input.Slots {
		if !declared[slot] {
			fail(w, apiError("FORBIDDEN", "Credential slot is not declared by an admitted tool"))
			return
		}
	}
	data, err := json.Marshal(input.CredentialBatch)
	if err != nil {
		fail(w, err)
		return
	}
	defer clear(data)
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	res, err := s.guestRequest(ctx, b, "PUT", "/v1/credentials", bytes.NewReader(data))
	if err != nil {
		fail(w, err)
		return
	}
	defer res.Body.Close()
	if err = guestSuccess(res); err != nil {
		fail(w, err)
		return
	}
	current, err := s.observe(ctx, b)
	if err != nil {
		fail(w, err)
		return
	}
	if current.Generation != input.ExpectedGeneration || (current.Phase != "running" && current.Phase != "staged") {
		fail(w, apiError("STALE_GENERATION", "Runtime changed during credential provisioning"))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (s *Service) tool(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("tool")
	if !validName.MatchString(id) {
		fail(w, apiError("INVALID_REQUEST", "Invalid tool ID"))
		return
	}
	b, release, err := s.beginIO(clientID(r), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	defer release()
	var input struct {
		Args      []string `json:"args"`
		TimeoutMS int64    `json:"timeoutMs"`
	}
	if err = decode(w, r, &input); err != nil {
		fail(w, err)
		return
	}
	if input.TimeoutMS == 0 {
		input.TimeoutMS = 30000
	}
	if input.TimeoutMS < 1 || input.TimeoutMS > 300000 {
		fail(w, apiError("INVALID_REQUEST", "Invalid timeoutMs"))
		return
	}
	data, _ := json.Marshal(input)
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(input.TimeoutMS)*time.Millisecond+time.Second)
	defer cancel()
	res, err := s.guestRequestWith(s.toolHTTP, ctx, b, "POST", "/v1/tools/"+id, strings.NewReader(string(data)))
	if err != nil {
		fail(w, err)
		return
	}
	defer res.Body.Close()
	if err = guestSuccess(res); err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_, _ = io.Copy(w, io.LimitReader(res.Body, 4<<20))
}
