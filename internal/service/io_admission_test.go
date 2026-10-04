package service

import (
	"errors"
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
				st.Operations["pending-pause"] = operationRecord{Operation: Operation{ID: "pending-pause", Kind: "pause", TargetID: box.ID, Status: "running"}}
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
