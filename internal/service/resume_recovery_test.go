package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
)

func newResumeRecoveryRuntime(t *testing.T, phase, transitionID, observedTransitionID, executionID string) (*transitionPhaseProvider, *httptest.Server, *atomic.Int32) {
	t.Helper()
	var unquiesceCalls atomic.Int32
	guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
		case "/v1/unquiesce":
			unquiesceCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	base := &fakeCoreProvider{guestURL: guest.URL, created: true, executionID: executionID}
	provider := &transitionPhaseProvider{fakeCoreProvider: base, phase: phase,
		transitionID: transitionID, observedTransition: observedTransitionID, execution: executionID}
	t.Cleanup(guest.Close)
	return provider, guest, &unquiesceCalls
}

func resumableRecoveryConfig(config Config) Config {
	profile := config.Profiles[0]
	profile.Provider = "resumable-k8s-pod"
	profile.Image = "registry.example.invalid/agent@sha256:" + strings.Repeat("a", 64)
	profile.Namespace = "boxes"
	profile.NodeName = "node-a"
	profile.PersistentHome = true
	config.Profiles[0] = profile
	return config
}

func reopenForResumeRecovery(t *testing.T, f *coreFixture, config Config, provider boxprovider.Provider) {
	t.Helper()
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	f.service = nil
	service, err := New(config, map[string]boxprovider.Provider{"resumable-k8s-pod": provider})
	if err != nil {
		t.Fatal(err)
	}
	f.service = service
}

func seedInterruptedResume(t *testing.T, f *coreFixture, config Config, operationID string, needsGuest bool,
	transitionID, observedTransitionID, executionID string) string {
	t.Helper()
	boxID := "resume-recovery-" + operationID
	box := coreBox(boxID)
	box.ClientID = "owner"
	box.Profile = config.Profiles[0]
	box.Handle.Provider = "resumable-k8s-pod"
	box.Box.Phase = "resuming"
	box.Box.OperationID = operationID
	box.TransitionID = transitionID
	box.ObservedTransitionID = observedTransitionID
	box.ExecutionID = executionID
	box.ResumePending = needsGuest
	box.ResumeNeedsGuest = needsGuest
	if needsGuest {
		box.ResumeIntentID = operationID
		box.ResumeExecutionID = executionID
		box.ResumeTransitionID = transitionID
		box.ResumeObservedTransitionID = observedTransitionID
	}
	operation := Operation{ID: operationID, Kind: "resume", TargetID: boxID, Status: "running", Version: 1}
	if err := f.service.store.Update(func(st *State) error {
		st.Boxes[boxID] = box
		st.Operations[operationID] = operationRecord{ClientID: "owner", Operation: operation}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return boxID
}

func waitForRecoveredResume(t *testing.T, service *Service, operationID string) Operation {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	op, err := service.waitOperation(ctx, "owner", operationID, 3*time.Second)
	if err != nil {
		t.Fatalf("wait for recovered resume: %v", err)
	}
	if op.Status != "succeeded" {
		t.Fatalf("recovered resume status = %q, want succeeded: %+v", op.Status, op)
	}
	return op
}

func assertRecoveredSnapshot(t *testing.T, service *Service, op Operation, boxID, operationID string) {
	t.Helper()
	if op.ID != operationID || op.Box == nil || op.Box.ID != boxID || op.Box.Phase != "running" || op.Box.OperationID != "" {
		t.Fatalf("resume did not publish its terminal Box snapshot: %+v", op)
	}
	box, err := service.rawBox(boxID)
	if err != nil {
		t.Fatal(err)
	}
	if box.Box.OperationID != "" || box.Box.Phase != "running" || box.ResumePending || box.ResumeNeedsGuest {
		t.Fatalf("resume intent did not clear on completion: %+v", box)
	}
}

func TestRestartMigratesLegacyResumeIntentAndContinuesSameOperation(t *testing.T) {
	f := newCoreFixture(t)
	provider, _, unquiesceCalls := newResumeRecoveryRuntime(t, "suspended", "old-transition", "old-transition", "old-execution")
	config := resumableRecoveryConfig(f.config)
	operationID := "resume-legacy-pending"
	boxID := seedInterruptedResume(t, f, config, operationID, false, "old-transition", "old-transition", "old-execution")

	reopenForResumeRecovery(t, f, config, provider)
	op := waitForRecoveredResume(t, f.service, operationID)
	assertRecoveredSnapshot(t, f.service, op, boxID, operationID)
	provider.mu.Lock()
	actionCalls := provider.actionCalls
	provider.mu.Unlock()
	if actionCalls != 1 {
		t.Fatalf("legacy pending resume transition calls = %d, want 1", actionCalls)
	}
	if got := unquiesceCalls.Load(); got != 1 {
		t.Fatalf("legacy pending resume unquiesce calls = %d, want 1", got)
	}
}

func TestRestartAtGuestStageDoesNotRepeatRuntimeTransition(t *testing.T) {
	f := newCoreFixture(t)
	operationID := "resume-needs-guest"
	provider, _, unquiesceCalls := newResumeRecoveryRuntime(t, "running", operationID, operationID, "execution-after-resume")
	config := resumableRecoveryConfig(f.config)
	boxID := seedInterruptedResume(t, f, config, operationID, true, operationID, operationID, "execution-after-resume")

	reopenForResumeRecovery(t, f, config, provider)
	op := waitForRecoveredResume(t, f.service, operationID)
	assertRecoveredSnapshot(t, f.service, op, boxID, operationID)
	provider.mu.Lock()
	actionCalls := provider.actionCalls
	provider.mu.Unlock()
	if actionCalls != 0 {
		t.Fatalf("recovery repeated runtime transition %d times, want 0", actionCalls)
	}
	if got := unquiesceCalls.Load(); got != 1 {
		t.Fatalf("guest-stage recovery unquiesce calls = %d, want 1", got)
	}
}
