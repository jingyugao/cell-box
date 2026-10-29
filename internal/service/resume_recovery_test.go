package service

import (
	"context"
	"testing"
	"time"

	"github.com/jingyugao/cell-box/internal/boxprovider"
)

func TestRestartMarksInterruptedResumeUnknownWithoutRetry(t *testing.T) {
	f := newCoreFixture(t)
	provider := &resumeTestProvider{fakeCoreProvider: f.provider, stream: make(chan boxprovider.Observation, 1)}
	profile := f.config.Profiles[0]
	profile.Provider = "resumable-k8s-pod"
	for _, status := range []string{"queued", "running"} {
		id := "resume-restart-" + status
		b := boxRecord{
			Box:      Box{ID: id, Phase: "resuming", Generation: 3, OperationID: "op-" + status},
			ClientID: "client-a", Profile: profile,
			Handle:       boxprovider.Handle{Provider: "docker", ID: "handle-" + status},
			TransitionID: "before-resume", ExecutionID: "old-execution",
		}
		op := Operation{ID: "op-" + status, Kind: "resume", TargetID: id, Status: status, Version: 1}
		if err := f.service.store.Write(context.Background(), func(tx *storeTx) error {
			if err := tx.PutBox(b); err != nil {
				return err
			}
			return tx.PutOperation(operationRecord{ClientID: "client-a", Operation: op})
		}); err != nil {
			t.Fatal(err)
		}
	}
	dataDir := f.config.DataDir
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	f.service = nil
	service, err := New(f.config, map[string]boxprovider.Provider{"docker": f.provider, "resumable-k8s-pod": provider})
	if err != nil {
		t.Fatal(err)
	}
	f.service = service
	t.Cleanup(func() { _ = f.service.Close() })
	if f.service.store.dir != dataDir {
		t.Fatalf("reopened a different database directory: %q != %q", f.service.store.dir, dataDir)
	}
	for _, status := range []string{"queued", "running"} {
		op, err := f.service.operation("client-a", "op-"+status)
		if err != nil {
			t.Fatal(err)
		}
		if op.Status != "failed" || op.Error == nil || op.Error.Code != "UNKNOWN_OUTCOME" {
			t.Fatalf("interrupted %s operation = %+v, want UNKNOWN_OUTCOME", status, op)
		}
	}
	if provider.requests.Load() != 0 {
		t.Fatalf("restart repeated RequestResume %d times", provider.requests.Load())
	}
	// Give startup observers time to run; observation must not replay the action.
	time.Sleep(30 * time.Millisecond)
	if provider.requests.Load() != 0 {
		t.Fatalf("background recovery replayed RequestResume %d times", provider.requests.Load())
	}
}
