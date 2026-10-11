package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestResumeHTTPSynchronousRetryAndFailure(t *testing.T) {
	for _, guestStatus := range []int{204, 500} {
		t.Run(http.StatusText(guestStatus), func(t *testing.T) {
			f := newCoreFixture(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var calls atomic.Int32
			guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/unquiesce" {
					calls.Add(1)
					close(entered)
					<-release
					w.WriteHeader(guestStatus)
					return
				}
				w.WriteHeader(503) // health is deliberately unhealthy and must not gate resume.
			}))
			defer guest.Close()
			defer once.Do(func() { close(release) })
			f.provider.mu.Lock()
			f.provider.created = true
			f.provider.guestURL = guest.URL
			f.provider.mu.Unlock()
			f.service.providers["resumable-k8s-pod"] = f.provider
			profile := f.config.Profiles[0]
			profile.Provider = "resumable-k8s-pod"
			if err := f.service.store.Update(func(st *State) error {
				st.Boxes["sync"] = boxRecord{Box: Box{ID: "sync", Phase: "suspended", Generation: 1}, ClientID: "client-a", Profile: profile, Handle: boxprovider.Handle{Provider: profile.Provider, ID: "runtime"}, ExecutionID: "old"}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			request := func(ctx context.Context) <-chan *httptest.ResponseRecorder {
				done := make(chan *httptest.ResponseRecorder, 1)
				go func() {
					r := httptest.NewRequest("POST", "/v1/boxes/sync:resume", nil).WithContext(ctx)
					r.Header.Set("Idempotency-Key", "same-resume")
					r.Header.Set("Authorization", "Bearer "+testClientToken)
					w := httptest.NewRecorder()
					f.service.Handler().ServeHTTP(w, r)
					done <- w
				}()
				return done
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first := request(ctx)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("restore did not start")
			}
			select {
			case <-first:
				t.Fatal("resume returned before runtime work finished")
			default:
			}
			cancel()
			select {
			case r := <-first:
				if r.Code == 200 {
					t.Fatal("cancelled wait reported success")
				}
			case <-time.After(time.Second):
				t.Fatal("disconnected HTTP waiter did not exit")
			}
			retry := request(context.Background())
			select {
			case <-retry:
				t.Fatal("retry returned before original resume finished")
			case <-time.After(20 * time.Millisecond):
			}
			once.Do(func() { close(release) })
			var response *httptest.ResponseRecorder
			select {
			case response = <-retry:
			case <-time.After(2 * time.Second):
				t.Fatal("sync resume did not return terminal result")
			}
			if guestStatus == 204 {
				wantStatus(t, response.Code, 200, response.Body.Bytes())
				op := decodeResponse[Operation](t, response.Body.Bytes())
				if op.Status != "succeeded" || op.FinishedAt == nil {
					t.Fatal("HTTP 200 contained an unfinished operation")
				}
				stored, err := f.service.operation("client-a", op.ID)
				if err != nil || stored.Status != "succeeded" {
					t.Fatal("response preceded committed completion")
				}
			} else {
				wantStatus(t, response.Code, 502, response.Body.Bytes())
			}
			if calls.Load() != 1 {
				t.Fatalf("idempotent retry repeated restore: %d", calls.Load())
			}
		})
	}
}

func TestResumeRejectsStaleGenerationAtAdmission(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	f.provider.mu.Lock()
	f.provider.created = true
	f.provider.mu.Unlock()
	f.service.providers["resumable-k8s-pod"] = f.provider
	profile := f.config.Profiles[0]
	profile.Provider = "resumable-k8s-pod"
	if err := f.service.store.Update(func(st *State) error {
		st.Boxes["stale-resume"] = boxRecord{
			Box:      Box{ID: "stale-resume", Phase: "suspended", Generation: 8},
			ClientID: "client-a", Profile: profile,
			Handle: boxprovider.Handle{Provider: profile.Provider, ID: "runtime-stale"},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/v1/boxes/stale-resume:resume", strings.NewReader(`{"expectedGeneration":7}`))
	r.Header.Set("Idempotency-Key", "stale-generation")
	r.Header.Set("Authorization", "Bearer "+testClientToken)
	w := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(w, r)
	wantStatus(t, w.Code, http.StatusConflict, w.Body.Bytes())
	var response struct {
		Error APIError `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.Error.Code != "STALE_GENERATION" {
		t.Fatalf("stale generation response: %+v, %v", response, err)
	}
	b, err := f.service.rawBox("stale-resume")
	if err != nil || b.Box.Phase != "suspended" || b.Box.OperationID != "" {
		t.Fatalf("stale request changed Box admission: %+v, %v", b.Box, err)
	}
}

func TestResumeReplayKeepsLegacyIdempotencyHashBeforeGenerationCheck(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	f.provider.mu.Lock()
	f.provider.created = true
	f.provider.mu.Unlock()
	f.service.providers["resumable-k8s-pod"] = f.provider
	profile := f.config.Profiles[0]
	profile.Provider = "resumable-k8s-pod"
	if err := f.service.store.Update(func(st *State) error {
		st.Boxes["legacy-resume"] = boxRecord{
			Box:      Box{ID: "legacy-resume", Phase: "suspended", Generation: 8},
			ClientID: "client-a", Profile: profile,
			Handle: boxprovider.Handle{Provider: profile.Provider, ID: "runtime-legacy"},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	first, err := f.service.action("client-a", "legacy-no-body-key", "legacy-resume", "resume")
	if err != nil {
		t.Fatal(err)
	}
	stale := uint64(7)
	replay, err := f.service.actionContextWithGeneration(context.Background(), "client-a", "legacy-no-body-key", "legacy-resume", "resume", &stale)
	if err != nil || replay.ID != first.ID {
		t.Fatalf("legacy key replay was rejected or created another operation: first=%+v replay=%+v err=%v", first, replay, err)
	}
}

func TestConcurrentSameKeyResumeSharesOneOperation(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	var enteredOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var calls atomic.Int32
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/unquiesce" {
			calls.Add(1)
			enteredOnce.Do(func() { close(entered) })
			<-release
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer guest.Close()
	defer unblock()
	f.provider.mu.Lock()
	f.provider.created = true
	f.provider.guestURL = guest.URL
	f.provider.mu.Unlock()
	f.service.providers["resumable-k8s-pod"] = f.provider
	profile := f.config.Profiles[0]
	profile.Provider = "resumable-k8s-pod"
	if err := f.service.store.Update(func(st *State) error {
		st.Boxes["same-resume"] = boxRecord{
			Box:      Box{ID: "same-resume", Phase: "suspended", Generation: 12},
			ClientID: "client-a", Profile: profile,
			Handle: boxprovider.Handle{Provider: profile.Provider, ID: "runtime-same"},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	request := func() <-chan *httptest.ResponseRecorder {
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			r := httptest.NewRequest("POST", "/v1/boxes/same-resume:resume", strings.NewReader(`{"expectedGeneration":12}`))
			r.Header.Set("Idempotency-Key", "same-key")
			r.Header.Set("Authorization", "Bearer "+testClientToken)
			w := httptest.NewRecorder()
			f.service.Handler().ServeHTTP(w, r)
			done <- w
		}()
		return done
	}
	first := request()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first resume did not reach runtime work")
	}
	second := request()
	select {
	case <-second:
		t.Fatal("same-key replay returned before the original operation finished")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	var firstResponse, secondResponse *httptest.ResponseRecorder
	select {
	case firstResponse = <-first:
	case <-time.After(2 * time.Second):
		t.Fatal("first resume did not finish")
	}
	select {
	case secondResponse = <-second:
	case <-time.After(2 * time.Second):
		t.Fatal("idempotent resume replay did not finish")
	}
	if firstResponse.Code != http.StatusOK || secondResponse.Code != http.StatusOK {
		t.Fatalf("resume responses = %d, %d; bodies: %s / %s", firstResponse.Code, secondResponse.Code, firstResponse.Body, secondResponse.Body)
	}
	firstOp := decodeResponse[Operation](t, firstResponse.Body.Bytes())
	secondOp := decodeResponse[Operation](t, secondResponse.Body.Bytes())
	if firstOp.ID != secondOp.ID || firstOp.Status != "succeeded" || calls.Load() != 1 {
		t.Fatalf("same-key resume did not share one operation: first=%+v second=%+v runtimeCalls=%d", firstOp, secondOp, calls.Load())
	}
}
