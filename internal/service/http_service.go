package service

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/guestapi"
)

// HTTPService opts one business port into direct Kubernetes Service routing.
// All business requests must pass through the Cellbox Traefik middleware.
type HTTPService struct {
	Port               int `json:"port"`
	IdleTimeoutSeconds int `json:"idleTimeoutSeconds"`
}

func (h *HTTPService) validate() error {
	if h.Port < 1 || h.Port > 65535 || h.Port == guestapi.Port {
		return apiError("INVALID_REQUEST", "httpService.port must be a business TCP port")
	}
	if h.IdleTimeoutSeconds == 0 {
		h.IdleTimeoutSeconds = 300
	}
	if h.IdleTimeoutSeconds < 1 || h.IdleTimeoutSeconds > 86400 {
		return apiError("INVALID_REQUEST", "httpService.idleTimeoutSeconds must be 1..86400")
	}
	return nil
}

type httpActivity struct {
	LastActivityAt   time.Time `json:"lastActivityAt"`
	IdleAt           int64     `json:"idleAt"`
	ReadyExecutionID string    `json:"readyExecutionId,omitempty"`
}

// HTTPRequest persists until explicitly released. Expiry cannot prove that a
// remote proxy has stopped forwarding a request, so uncertain requests block
// suspension across server restarts rather than expiring into a false idle.
type HTTPRequest struct {
	ID        string    `json:"id"`
	StartedAt time.Time `json:"startedAt"`
	Admitted  bool      `json:"admitted"`
}

type HTTPServiceStatus struct {
	Configuration  HTTPService   `json:"configuration"`
	ServiceName    string        `json:"serviceName"`
	Namespace      string        `json:"namespace"`
	LastActivityAt time.Time     `json:"lastActivityAt"`
	Requests       []HTTPRequest `json:"requests"`
}

func (tx *storeTx) hasHTTPRequests(id string) (bool, error) {
	var active bool
	err := tx.tx.QueryRowContext(tx.ctx, "SELECT EXISTS(SELECT 1 FROM runtime_records WHERE kind='http_requests' AND box_id=?)", id).Scan(&active)
	return active, err
}

func (tx *storeTx) touchHTTPService(b boxRecord, now time.Time) error {
	a, err := readRecord[httpActivity](tx.ctx, tx.tx, "http_activity", b.Box.ID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	a.LastActivityAt = now
	a.IdleAt = now.Add(time.Duration(b.Box.HTTPService.IdleTimeoutSeconds) * time.Second).UnixMilli()
	return tx.put("http_activity", b.Box.ID, b.ClientID, b.Box.ID, a)
}

func (s *Service) registerHTTPService(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/boxes/{id}/http-service", s.httpServiceStatus)
	mux.HandleFunc("PUT /v1/boxes/{id}/http-requests/{requestId}", s.admitHTTPRequest)
	mux.HandleFunc("DELETE /v1/boxes/{id}/http-requests/{requestId}", s.finishHTTPRequest)
}

func (s *Service) httpServiceStatus(w http.ResponseWriter, r *http.Request) {
	b, err := s.box(clientID(r), r.PathValue("id"))
	if err == nil && b.Box.HTTPService == nil {
		err = apiError("NOT_FOUND", "Box has no HTTP service")
	}
	if err != nil {
		fail(w, err)
		return
	}
	a, err := readRecord[httpActivity](r.Context(), s.store.db, "http_activity", b.Box.ID)
	if err != nil {
		fail(w, err)
		return
	}
	requests, err := listRecords[HTTPRequest](r.Context(), s.store.db, "http_requests", "AND box_id=? ORDER BY id", b.Box.ID)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, HTTPServiceStatus{Configuration: *b.Box.HTTPService, ServiceName: b.Handle.Name,
		Namespace: b.Handle.Namespace, LastActivityAt: a.LastActivityAt, Requests: requests})
}

func (s *Service) addHTTPRequest(ctx context.Context, client, id, requestID string) error {
	unlock := s.admissions.lock(id)
	defer unlock()
	return s.store.Write(ctx, func(tx *storeTx) error {
		b, err := tx.Box(client, id)
		if err != nil {
			return err
		}
		if b.Box.HTTPService == nil {
			return apiError("NOT_FOUND", "Box has no HTTP service")
		}
		switch b.Box.Phase {
		case "creating", "running", "suspended", "suspending", "resuming", "checkpointing", "restoring":
		default:
			return apiError("CONFLICT", "Box cannot accept HTTP requests in its current phase")
		}
		key := id + "/" + requestID
		if _, err := readRecord[HTTPRequest](ctx, tx.tx, "http_requests", key); err == nil {
			return nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var count int
		if err := tx.tx.QueryRowContext(ctx, "SELECT count(*) FROM runtime_records WHERE kind='http_requests' AND box_id=?", id).Scan(&count); err != nil {
			return err
		}
		if count >= 1024 {
			return apiError("BUSY", "HTTP service has reached its 1024 active and waiting request limit")
		}
		now := time.Now().UTC()
		if err := tx.put("http_requests", key, client, id, HTTPRequest{ID: requestID, StartedAt: now}); err != nil {
			return err
		}
		return tx.touchHTTPService(b, now)
	})
}

func (s *Service) releaseHTTPRequest(ctx context.Context, client, id, requestID string, onlyWaiting bool) error {
	unlock := s.admissions.lock(id)
	defer unlock()
	return s.store.Write(ctx, func(tx *storeTx) error {
		b, err := tx.Box(client, id)
		if err != nil {
			return err
		}
		key := id + "/" + requestID
		request, err := readRecord[HTTPRequest](ctx, tx.tx, "http_requests", key)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		// A retried PUT may have admitted this same request while an earlier
		// handler was canceled. Its cleanup must not release the live request.
		if onlyWaiting && request.Admitted {
			return nil
		}
		if err := tx.Delete("http_requests", key); err != nil {
			return err
		}
		return tx.touchHTTPService(b, time.Now().UTC())
	})
}

func (s *Service) admitHTTPRequest(w http.ResponseWriter, r *http.Request) {
	client, id, requestID := clientID(r), r.PathValue("id"), r.PathValue("requestId")
	if !validName.MatchString(requestID) {
		fail(w, apiError("INVALID_REQUEST", "Invalid HTTP request ID"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(s.config.StartupTimeoutSeconds)*time.Second)
	defer cancel()
	if err := s.addHTTPRequest(ctx, client, id, requestID); err != nil {
		fail(w, err)
		return
	}
	if err := s.waitHTTPService(ctx, client, id, requestID); err != nil {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stop()
		if releaseErr := s.releaseHTTPRequest(cleanup, client, id, requestID, true); releaseErr != nil {
			slog.Error("HTTP request admission cleanup failed", "box", id, "request", requestID, "error", releaseErr)
		}
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) finishHTTPRequest(w http.ResponseWriter, r *http.Request) {
	if err := s.releaseHTTPRequest(r.Context(), clientID(r), r.PathValue("id"), r.PathValue("requestId"), false); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) waitHTTPService(ctx context.Context, client, id, requestID string) error {
	for {
		changed := s.store.Changes()
		b, err := s.box(client, id)
		if err != nil {
			return err
		}
		if b.Box.OperationID != "" {
			op, err := s.operation(client, b.Box.OperationID)
			if err != nil {
				return err
			}
			if (op.Status == "running" || op.Status == "queued") && op.Kind != "exec" {
				op, err = s.waitOperation(ctx, client, op.ID, 10*time.Second)
				if err != nil {
					return err
				}
				if op.Status == "failed" {
					if op.Error != nil {
						return op.Error
					}
					return apiError("RUNTIME_ERROR", "HTTP service lifecycle operation failed")
				}
				continue
			}
		}
		switch b.Box.Phase {
		case "suspended":
			op, err := s.actionContextWithGeneration(ctx, client, id, "resume", &b.Box.Generation, b.Box.HTTPService.Port)
			if err != nil {
				var apiErr *APIError
				if errors.As(err, &apiErr) && (apiErr.Code == "BUSY" || apiErr.Code == "CONFLICT" || apiErr.Code == "STALE_GENERATION") {
					break
				}
				return err
			}
			op, err = s.waitOperation(ctx, client, op.ID, 10*time.Second)
			if err != nil {
				return err
			}
			if op.Status == "failed" {
				if op.Error != nil {
					return op.Error
				}
				return apiError("RUNTIME_ERROR", "HTTP service resume failed")
			}
			continue
		case "running":
			if err := s.admitRunningHTTPRequest(ctx, b, requestID); err == nil {
				return nil
			} else if !errors.Is(err, boxprovider.ErrNotReady) && !errors.Is(err, boxprovider.ErrStaleExecution) {
				return err
			}
		case "creating", "resuming", "suspending", "checkpointing", "restoring":
		default:
			return apiError("CONFLICT", "HTTP service is unavailable; inspect Box state")
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-s.ctx.Done():
			timer.Stop()
			return s.ctx.Err()
		case <-changed:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (s *Service) admitRunningHTTPRequest(ctx context.Context, b boxRecord, requestID string) error {
	a, err := readRecord[httpActivity](ctx, s.store.db, "http_activity", b.Box.ID)
	if err != nil {
		return err
	}
	if b.ExecutionID == "" || a.ReadyExecutionID != b.ExecutionID {
		provider, ok := s.providers[b.Profile.Provider].(boxprovider.HTTPServiceProvider)
		if !ok {
			return boxprovider.ErrUnsupported
		}
		address, err := provider.HTTPServiceAddress(ctx, b.Handle, b.Box.HTTPService.Port)
		if err != nil {
			return err
		}
		probeCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		conn, err := (&net.Dialer{}).DialContext(probeCtx, "tcp", address)
		cancel()
		if err != nil {
			return boxprovider.ErrNotReady
		}
		_ = conn.Close()
	}
	unlock := s.admissions.lock(b.Box.ID)
	defer unlock()
	return s.store.Write(ctx, func(tx *storeTx) error {
		current, err := tx.Box(b.ClientID, b.Box.ID)
		if err != nil {
			return err
		}
		if current.Box.Phase != "running" || current.ExecutionID != b.ExecutionID || current.Box.Generation != b.Box.Generation {
			return boxprovider.ErrStaleExecution
		}
		key := b.Box.ID + "/" + requestID
		request, err := readRecord[HTTPRequest](ctx, tx.tx, "http_requests", key)
		if errors.Is(err, sql.ErrNoRows) {
			return apiError("CONFLICT", "HTTP request was already released")
		}
		if err != nil {
			return err
		}
		request.Admitted = true
		if err := tx.put("http_requests", key, b.ClientID, b.Box.ID, request); err != nil {
			return err
		}
		activity, err := readRecord[httpActivity](ctx, tx.tx, "http_activity", b.Box.ID)
		if err != nil {
			return err
		}
		activity.ReadyExecutionID = b.ExecutionID
		return tx.put("http_activity", b.Box.ID, b.ClientID, b.Box.ID, activity)
	})
}

func (s *Service) startHTTPIdleWorker() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case now := <-ticker.C:
				s.suspendIdleHTTPServices(now)
			}
		}
	}()
}

func (s *Service) suspendIdleHTTPServices(now time.Time) {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	// Only read opted-in, due services with no pending requests. Do not scan
	// unrelated Box records or infer idle from process-local connection state.
	boxes, err := listRecords[boxRecord](ctx, s.store.db, "boxes", `AND json_extract(record,'$.box.phase')='running'
		AND id IN (SELECT id FROM runtime_records WHERE kind='http_activity' AND json_extract(record,'$.idleAt')<=?)
		AND NOT EXISTS (SELECT 1 FROM runtime_records requests WHERE requests.kind='http_requests' AND requests.box_id=runtime_records.id)`, now.UnixMilli())
	if err != nil {
		slog.Error("HTTP service idle query failed", "error", err)
		return
	}
	for _, b := range boxes {
		_, err := s.actionWithCheck(ctx, b.ClientID, b.Box.ID, "suspend", &b.Box.Generation, func(tx *storeTx, current boxRecord) error {
			a, err := readRecord[httpActivity](tx.ctx, tx.tx, "http_activity", current.Box.ID)
			if err != nil {
				return err
			}
			if a.IdleAt > now.UnixMilli() {
				return apiError("BUSY", "HTTP service became active")
			}
			// Bound retries if this accepted suspend later fails in the runtime.
			a.IdleAt = now.Add(30 * time.Second).UnixMilli()
			return tx.put("http_activity", current.Box.ID, current.ClientID, current.Box.ID, a)
		})
		if err != nil {
			var apiErr *APIError
			if !errors.As(err, &apiErr) || (apiErr.Code != "BUSY" && apiErr.Code != "CONFLICT" && apiErr.Code != "STALE_GENERATION") {
				slog.Error("HTTP service auto-suspend failed", "box", b.Box.ID, "error", err)
			}
		}
	}
}
