package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestQuiesceWaitsOnlyForTransientGuestBusy(t *testing.T) {
	for _, tc := range []struct {
		name      string
		statuses  []int
		timeout   time.Duration
		wantCode  string
		wantCalls int32
	}{
		{name: "inflight tool drains", statuses: []int{409, 204}, timeout: time.Second, wantCalls: 2},
		{name: "invalid request is not retried", statuses: []int{400}, timeout: time.Second, wantCode: "INVALID_REQUEST", wantCalls: 1},
		{name: "busy exceeds caller deadline", statuses: []int{409}, timeout: 50 * time.Millisecond, wantCode: "BUSY", wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCoreFixture(t)
			_, box := f.createBox(t, "quiesce-"+tc.name)
			b, err := f.service.rawBox(box.ID)
			if err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.URL.Path != "/v1/quiesce" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
				}
				index := min(int(calls.Add(1))-1, len(tc.statuses)-1)
				w.WriteHeader(tc.statuses[index])
			}))
			defer guest.Close()
			f.provider.mu.Lock()
			f.provider.guestURL = guest.URL
			f.provider.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			err = f.service.quiesceGuest(ctx, b)
			if tc.wantCode == "" && err != nil {
				t.Fatal(err)
			}
			if tc.wantCode != "" {
				var apiErr *APIError
				if !errors.As(err, &apiErr) || apiErr.Code != tc.wantCode {
					t.Fatalf("got %v, want %s", err, tc.wantCode)
				}
			}
			if calls.Load() != tc.wantCalls {
				t.Fatalf("got %d calls, want %d", calls.Load(), tc.wantCalls)
			}
		})
	}
}
