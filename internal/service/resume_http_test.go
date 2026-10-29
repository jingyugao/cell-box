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
		requestIn:        make(chan struct{}, 1), portsIn: make(chan []int, 1), release: make(chan struct{}), holdRequest: true,
		result: &boxprovider.Observation{Phase: "resuming", Generation: 2},
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
	select {
	case ports := <-p.portsIn:
		if len(ports) != 1 || ports[0] != 40000 {
			t.Fatalf("omitted waitPorts propagated as %v, want default [40000]", ports)
		}
	case <-time.After(time.Second):
		t.Fatal("resume did not propagate default waitPorts")
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
		t.Fatalf("operation completed before committed Running: %+v, %v", pending, err)
	}
	p.stream <- boxprovider.Observation{Phase: "running", Generation: 2, ExecutionID: "restored-execution", TransitionID: op.ID, ObservedTransitionID: op.ID}
	finished, err := f.service.waitOperation(context.Background(), b.ClientID, op.ID, 2*time.Second)
	if err != nil || finished.Status != "succeeded" {
		t.Fatalf("accepted operation did not complete: %+v, %v", finished, err)
	}
	if p.requests.Load() != 1 || p.watches.Load() != 1 || p.guestCount() != 0 || p.inspects.Load() != 0 {
		t.Fatalf("requests=%d watches=%d Guest=%d Inspect=%d", p.requests.Load(), p.watches.Load(), p.guestCount(), p.inspects.Load())
	}
}

func TestResumeHTTPPropagatesWaitPortsAndRejectsInvalidInput(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	p := &resumeTestProvider{
		fakeCoreProvider: f.provider,
		stream:           make(chan boxprovider.Observation, 8),
		initial:          boxprovider.Observation{Phase: "suspended", Generation: 1},
		requestIn:        make(chan struct{}, 1), portsIn: make(chan []int, 1),
	}
	b := newResumeTestBox(t, f, "wait-ports-http", p)
	post := func(action, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/boxes/"+b.Box.ID+":"+action, strings.NewReader(body))
		r.Header.Set("Idempotency-Key", "wait-ports-"+action+"-"+body)
		r.Header.Set("Authorization", "Bearer "+testClientToken)
		w := httptest.NewRecorder()
		f.service.Handler().ServeHTTP(w, r)
		return w
	}
	for _, body := range []string{
		`{"waitPorts":[]}`,
		`{"waitPorts":null}`,
		`{"waitPorts":"8080"}`,
		`{"waitPorts":[0]}`,
		`{"waitPorts":[65536]}`,
	} {
		w := post("resume", body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("resume body %s status=%d body=%s, want 400", body, w.Code, w.Body)
		}
	}
	if w := post("activate", `{"waitPorts":[8080]}`); w.Code != http.StatusBadRequest {
		t.Errorf("non-resume waitPorts status=%d body=%s, want 400", w.Code, w.Body)
	}
	if p.requests.Load() != 0 {
		t.Fatalf("invalid waitPorts reached provider %d times", p.requests.Load())
	}

	w := post("resume", `{"expectedGeneration":1,"waitPorts":[8080,80,8080]}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("valid resume status=%d body=%s", w.Code, w.Body)
	}
	select {
	case ports := <-p.portsIn:
		if len(ports) != 2 || ports[0] != 80 || ports[1] != 8080 {
			t.Fatalf("custom waitPorts propagated as %v, want [80 8080]", ports)
		}
	case <-time.After(time.Second):
		t.Fatal("resume did not propagate requested waitPorts")
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
