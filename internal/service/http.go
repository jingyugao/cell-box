package service

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type clientContextKey struct{}

func clientID(r *http.Request) string {
	value, _ := r.Context().Value(clientContextKey{}).(string)
	return value
}
func bearer(r *http.Request) string {
	v := r.Header.Get("Authorization")
	if !strings.HasPrefix(v, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(v, "Bearer ")
}
func (s *Service) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		candidate := hash(bearer(r))
		for _, c := range s.config.Clients {
			if subtle.ConstantTimeCompare([]byte(candidate), []byte(hash(c.Token))) == 1 {
				next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), clientContextKey{}, c.ID)))
				return
			}
		}
		w.Header().Set("WWW-Authenticate", "Bearer")
		fail(w, apiError("UNAUTHENTICATED", "A valid client bearer token is required"))
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
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
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
	mux.HandleFunc("POST /v1/boxes", func(w http.ResponseWriter, r *http.Request) {
		var input createRequest
		if err := decode(w, r, &input); err != nil {
			fail(w, err)
			return
		}
		op, err := s.create(clientID(r), r.Header.Get("Idempotency-Key"), input, "")
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 202, op)
	})
	mux.HandleFunc("POST /v1/boxes:restore", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			createRequest
			ArchiveID string `json:"archiveId"`
		}
		if err := decode(w, r, &input); err != nil {
			fail(w, err)
			return
		}
		if input.ArchiveID == "" {
			fail(w, apiError("INVALID_REQUEST", "archiveId is required"))
			return
		}
		op, err := s.create(clientID(r), r.Header.Get("Idempotency-Key"), input.createRequest, input.ArchiveID)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, 202, op)
	})
	mux.HandleFunc("GET /v1/boxes", s.listBoxes)
	mux.HandleFunc("GET /v1/boxes/{id}", func(w http.ResponseWriter, r *http.Request) {
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
	mux.HandleFunc("POST /v1/boxes/{id}/tools/{tool}", s.tool)
	mux.HandleFunc("POST /v1/routes", s.createRoute)
	mux.HandleFunc("POST /v1/routes/{id}/grants", s.createGrant)
	mux.HandleFunc("DELETE /v1/grants/{id}", s.revokeGrant)
	mux.HandleFunc("PATCH /v1/grants/{id}", s.renewGrant)
	mux.HandleFunc("GET /v1/access-requests/{id}", s.getAccess)
	mux.HandleFunc("POST /v1/access-requests/{action}", func(w http.ResponseWriter, r *http.Request) {
		id, action, ok := strings.Cut(r.PathValue("action"), ":")
		if !ok || action != "approve" {
			fail(w, apiError("NOT_FOUND", "Endpoint not found"))
			return
		}
		r.SetPathValue("id", id)
		s.approveAccess(w, r)
	})
	mux.HandleFunc("POST /v1/boxes/{id}/archives", s.captureArchive)
	mux.HandleFunc("GET /v1/archives", s.listArchives)
	mux.HandleFunc("GET /v1/archives/{id}", s.getArchive)
	mux.HandleFunc("GET /v1/archives/{id}/content", s.archiveContent)
	mux.HandleFunc("DELETE /v1/archives/{id}", s.deleteArchive)
	api := s.authenticate(mux)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
func (s *Service) listBoxes(w http.ResponseWriter, r *http.Request) {
	out := []Box{}
	err := s.store.View(func(st State) error {
		for _, b := range st.Boxes {
			if b.ClientID == clientID(r) {
				out = append(out, b.Box)
			}
		}
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	writeJSON(w, 200, out)
}
func (s *Service) operation(client, id string) (Operation, error) {
	var op Operation
	err := s.store.View(func(st State) error {
		record, ok := st.Operations[id]
		if !ok || record.ClientID != client {
			return apiError("NOT_FOUND", "Operation not found")
		}
		op = record.Operation
		return nil
	})
	return op, err
}
func (s *Service) getOperation(w http.ResponseWriter, r *http.Request) {
	op, err := s.operation(clientID(r), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, op)
}
func (s *Service) getExec(w http.ResponseWriter, r *http.Request) {
	var e Execution
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
	writeJSON(w, 200, e)
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
		case <-time.After(time.Second):
			op, err = s.operation(clientID(r), op.ID)
			if err != nil {
				return
			}
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
		if b.Box.State != "ready" {
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
		if !lease.ExpiresAt.After(time.Now()) || b.Box.State != "ready" {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	key := randomID("io-")
	var b boxRecord
	err := s.store.View(func(st State) error {
		var err error
		b, err = owned(&st, client, id)
		if err != nil {
			return err
		}
		if err = busy(&st, b, false); err != nil {
			return err
		}
		if b.Box.State != "ready" && b.Box.State != "staged" {
			return apiError("CONFLICT", "Box is not ready")
		}
		return nil
	})
	if err != nil {
		return b, nil, err
	}
	s.streams[key] = activeStream{boxID: id, cancel: func() {}}
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
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 65536))
	if err != nil || len(data) == 0 {
		fail(w, apiError("INVALID_REQUEST", "Credential must contain 1..65536 bytes"))
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
