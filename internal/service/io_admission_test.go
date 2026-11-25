package service

import (
	"encoding/json"
	"errors"
	"github.com/jingyugao/cell-box/internal/guestapi"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestInternalIOWaitsForLifecycleAdmission(t *testing.T) {
	for _, service := range []bool{false, true} {
		t.Run(map[bool]string{false: "files", true: "service"}[service], func(t *testing.T) {
			f := newCoreFixture(t)
			_, box := f.createBox(t, "io-admission")
			unlock := f.service.admissions.lock(box.ID)
			defer func() {
				if unlock != nil {
					unlock()
				}
			}()
			done := make(chan error, 1)
			go func() {
				_, release, err := f.service.beginIOWithPolicy("client-a", box.ID, service)
				if release != nil {
					release()
				}
				done <- err
			}()
			select {
			case err := <-done:
				t.Fatalf("I/O bypassed a pending lifecycle admission: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			err := f.service.store.Update(func(st *State) error {
				b := st.Boxes[box.ID]
				b.Box.OperationID = "pending-pause"
				st.Boxes[box.ID] = b
				st.Operations["pending-pause"] = operationRecord{ClientID: "client-a", Operation: Operation{ID: "pending-pause", Kind: "pause", TargetID: box.ID, Status: "running"}}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			unlock()
			unlock = nil
			select {
			case err := <-done:
				var api *APIError
				if !errors.As(err, &api) || api.Code != "BUSY" {
					t.Fatalf("I/O entered during lifecycle operation: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("I/O admission did not complete")
			}
		})
	}
}

func TestWorkloadUpdateRoutesToOwnedBoxDuringExec(t *testing.T) {
	f := newCoreFixture(t)
	_, box := f.createBox(t, "workload-revision")
	revision := strings.Repeat("a", 64)
	received := make(chan string, 1)
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/v1/workload" {
			http.NotFound(w, r)
			return
		}
		var input map[string]string
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
		}
		received <- input["revision"]
		w.WriteHeader(http.StatusNoContent)
	}))
	defer guest.Close()
	f.provider.guestURL = guest.URL
	err := f.service.store.Update(func(st *State) error {
		b := st.Boxes[box.ID]
		b.Profile.Guest.Env = map[string]string{guestapi.WorkloadReleasesEnv: "/var/lib/cellbox/shared/harness"}
		b.Box.OperationID = "active-tool-exec"
		st.Operations[b.Box.OperationID] = operationRecord{ClientID: b.ClientID, Operation: Operation{ID: b.Box.OperationID, Kind: "exec", Status: "running"}}
		st.Boxes[box.ID] = b
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	status, body := f.call(t, "PUT", "/v1/boxes/"+box.ID+"/workload", testClientToken, "", map[string]string{"revision": revision})
	if status != http.StatusNoContent {
		t.Fatalf("status %d: %s", status, body)
	}
	if got := <-received; got != revision {
		t.Fatalf("expected %s, got %s", revision, got)
	}
}
