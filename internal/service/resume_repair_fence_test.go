package service

import (
	"context"
	"errors"
	"testing"

	"cellbox.local/cellbox/internal/boxprovider"
)

func TestGuestResumeRepairCannotRetargetAnotherExecution(t *testing.T) {
	f := newCoreFixture(t)
	defer f.service.Close()
	provider := &transitionPhaseProvider{
		fakeCoreProvider: &fakeCoreProvider{created: true}, phase: "running",
		transitionID: "resume-original", observedTransition: "resume-original", execution: "replacement-pod",
	}
	f.service.providers["resumable-k8s-pod"] = provider
	profile := f.config.Profiles[0]
	profile.Provider = "resumable-k8s-pod"
	box := coreBox("guest-repair-fence")
	box.ClientID, box.Profile = "client-a", profile
	box.Handle = boxprovider.Handle{Provider: profile.Provider, ID: "runtime-fence"}
	box.Box.Phase = "resuming"
	box.ExecutionID = "original-pod"
	box.TransitionID, box.ObservedTransitionID = "resume-original", "resume-original"
	box.ResumePending, box.ResumeNeedsGuest = true, true
	box.ResumeIntentID = "resume-original"
	box.ResumeExecutionID = "original-pod"
	box.ResumeTransitionID, box.ResumeObservedTransitionID = "resume-original", "resume-original"
	if err := f.service.store.Update(func(st *State) error { st.Boxes[box.Box.ID] = box; return nil }); err != nil {
		t.Fatal(err)
	}
	_, err := f.service.ensureRunning(context.Background(), "client-a", "repair-new-pod", box.Box.ID, nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "STALE_TRANSITION" {
		t.Fatalf("Guest repair followed another execution: %v", err)
	}
	after, err := f.service.rawBox(box.Box.ID)
	if err != nil || after.ResumeExecutionID != "original-pod" || after.ExecutionID != "replacement-pod" || after.Box.OperationID != "" {
		t.Fatalf("observation changed the durable Guest repair fence: %+v, %v", after, err)
	}
}
