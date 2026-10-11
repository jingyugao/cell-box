package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"context"
	"net/http"
	"net/http/httptest"
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
