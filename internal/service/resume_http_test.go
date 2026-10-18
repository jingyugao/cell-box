package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
)

func TestResumeHTTPReturnsAcceptedAndSecondPendingRequestIsBusy(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	p := &resumeTestProvider{
		fakeCoreProvider: f.provider,
		stream:           make(chan boxprovider.Observation, 8),
		initial:          boxprovider.Observation{Phase: "suspended", Generation: 1},
		requestIn:        make(chan struct{}, 1), release: make(chan struct{}), holdRequest: true,
	}
	defer p.unblock()
	b := newResumeTestBox(t, f, "http-resume", p)
	request := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/boxes/"+b.Box.ID+":resume", strings.NewReader(`{"expectedGeneration":1}`))
		r.Header.Set("Idempotency-Key", "same-key")
		r.Header.Set("Authorization", "Bearer "+testClientToken)
		w := httptest.NewRecorder()
		f.service.Handler().ServeHTTP(w, r)
		return w
	}
	first := request()
	if first.Code != http.StatusAccepted {
		t.Fatalf("first resume status=%d body=%s", first.Code, first.Body)
	}
	op := decodeResponse[Operation](t, first.Body.Bytes())
	if op.Status != "running" {
		t.Fatalf("HTTP 202 returned non-pending operation: %+v", op)
	}
	select {
	case <-p.requestIn:
	case <-time.After(time.Second):
		t.Fatal("accepted resume did not reach runtime action")
	}
	second := request()
	if second.Code != http.StatusConflict {
		t.Fatalf("second pending request status=%d body=%s; want BUSY", second.Code, second.Body)
	}
	var body struct {
		Error APIError `json:"error"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &body); err != nil || body.Error.Code != "BUSY" {
		t.Fatalf("second request error=%+v decode=%v", body.Error, err)
	}
	p.unblock()
	pending, err := f.service.waitOperation(context.Background(), b.ClientID, op.ID, 50*time.Millisecond)
	if err != nil || pending.Status != "running" {
		t.Fatalf("operation completed before GuestReady: %+v, %v", pending, err)
	}
	p.stream <- boxprovider.Observation{Phase: "running", Generation: 2, ExecutionID: "restored-execution", TransitionID: op.ID, ObservedTransitionID: op.ID, GuestReady: true}
	finished, err := f.service.waitOperation(context.Background(), b.ClientID, op.ID, 2*time.Second)
	if err != nil || finished.Status != "succeeded" {
		t.Fatalf("accepted operation did not complete: %+v, %v", finished, err)
	}
	if p.requests.Load() != 1 || p.watches.Load() != 1 || p.guestCount() != 0 || p.inspects.Load() != 0 {
		t.Fatalf("requests=%d watches=%d Guest=%d Inspect=%d", p.requests.Load(), p.watches.Load(), p.guestCount(), p.inspects.Load())
	}
}

func TestResumeRejectsStaleGenerationAtAdmission(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	p := &resumeTestProvider{fakeCoreProvider: f.provider, stream: make(chan boxprovider.Observation, 1), initial: boxprovider.Observation{Phase: "suspended", Generation: 8}}
	b := newResumeTestBox(t, f, "stale-resume", p)
	if err := f.service.store.ChangeBox(context.Background(), b.Box.ID, func(current *boxRecord) error {
		current.Box.Generation = 8
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/boxes/stale-resume:resume", strings.NewReader(`{"expectedGeneration":7}`))
	r.Header.Set("Idempotency-Key", "stale-generation")
	r.Header.Set("Authorization", "Bearer "+testClientToken)
	w := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(w, r)
	wantStatus(t, w.Code, http.StatusConflict, w.Body.Bytes())
	var response struct {
		Error APIError `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.Error.Code != "STALE_GENERATION" {
		t.Fatalf("stale generation response=%+v decode=%v", response.Error, err)
	}
	after, err := f.service.rawBox(b.Box.ID)
	if err != nil || after.Box.Phase != "suspended" || after.Box.OperationID != "" || p.requests.Load() != 0 {
		t.Fatalf("stale request changed admission: box=%+v requests=%d err=%v", after.Box, p.requests.Load(), err)
	}
}
