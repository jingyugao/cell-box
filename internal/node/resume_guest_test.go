// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/guestapi"
	"google.golang.org/grpc"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type resumeGuestCRI struct {
	*executionCRI
	items []*cri.PodSandbox
}

func (c *resumeGuestCRI) ListPodSandbox(context.Context, *cri.ListPodSandboxRequest, ...grpc.CallOption) (*cri.ListPodSandboxResponse, error) {
	return &cri.ListPodSandboxResponse{Items: c.items}, nil
}

func resumeGuestFixture(t *testing.T, warm bool, containerID string) (*Backend, *api.ResumablePod, *core.Pod) {
	t.Helper()
	const podIP = "127.0.0.1"
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "box-pod", Namespace: "boxes", UID: "pod-uid"}}
	w := &api.ResumablePod{
		ObjectMeta: meta.ObjectMeta{UID: "box-uid"},
		Spec:       api.Spec{Container: core.Container{Name: "guest"}},
		Status:     api.Status{PodUID: string(p.UID)},
	}
	encodedConfig, err := json.Marshal(guestapi.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	w.Spec.Container.Args = []string{"serve", "--config-base64", base64.StdEncoding.EncodeToString(encodedConfig)}
	container := &cri.Container{
		Id:           containerID,
		PodSandboxId: "sandbox-id",
		Metadata:     &cri.ContainerMetadata{Name: "guest", Attempt: 0},
		Labels:       map[string]string{"io.kubernetes.pod.uid": string(p.UID)},
		State:        cri.ContainerState_CONTAINER_RUNNING,
	}
	sandboxStatus := &cri.PodSandboxStatus{
		Id:             "sandbox-id",
		Metadata:       &cri.PodSandboxMetadata{Name: p.Name, Namespace: p.Namespace, Uid: string(p.UID), Attempt: 0},
		RuntimeHandler: api.RuntimeClass,
		State:          cri.PodSandboxState_SANDBOX_READY,
		Network:        &cri.PodSandboxNetworkStatus{Ip: podIP},
	}
	if warm {
		w.Status.Execution = &api.Execution{PodUID: string(p.UID), ContainerID: containerID, PodIP: podIP}
	} else {
		p.Status.Phase = core.PodRunning
		p.Status.PodIP = podIP
		p.Status.ContainerStatuses = []core.ContainerStatus{{
			Name: "guest", ContainerID: "containerd://" + containerID,
			State: core.ContainerState{Running: &core.ContainerStateRunning{}},
		}}
	}
	runtime := &resumeGuestCRI{
		executionCRI: &executionCRI{containers: []*cri.Container{container}, sandbox: sandboxStatus},
		items:        []*cri.PodSandbox{{Id: "sandbox-id", Metadata: &cri.PodSandboxMetadata{Name: p.Name, Namespace: p.Namespace, Uid: string(p.UID)}, RuntimeHandler: api.RuntimeClass}},
	}
	return &Backend{Runtime: runtime}, w, p
}

func startResumeGuestServer(t *testing.T, status int, body string, redirectsTo string) (*http.Server, net.Listener, *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(guestapi.Port)))
	if err != nil {
		t.Skipf("guest API test port unavailable: %v", err)
	}
	var requests atomic.Int32
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/unquiesce" {
			http.Error(w, "wrong endpoint", http.StatusNotFound)
			return
		}
		if redirectsTo != "" {
			http.Redirect(w, r, redirectsTo, http.StatusTemporaryRedirect)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return server, listener, &requests
}

func TestResumeGuestPostsOnlyForCurrentExecution(t *testing.T) {
	_, _, requests := startResumeGuestServer(t, http.StatusNoContent, "", "")
	for _, warm := range []bool{true, false} {
		name := "cold-status"
		if warm {
			name = "confirmed-execution"
		}
		t.Run(name, func(t *testing.T) {
			b, w, p := resumeGuestFixture(t, warm, "current-container")
			if err := b.ResumeGuest(context.Background(), w, p); err != nil {
				t.Fatalf("ResumeGuest() error = %v", err)
			}
		})
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("guest requests = %d, want 2", got)
	}
}

func TestResumeGuestRejectsReplacedContainerBeforeRequest(t *testing.T) {
	_, _, requests := startResumeGuestServer(t, http.StatusNoContent, "", "")
	b, w, p := resumeGuestFixture(t, true, "old-container")
	b.Runtime.(*resumeGuestCRI).containers[0].Id = "replacement-container"
	if err := b.ResumeGuest(context.Background(), w, p); err == nil {
		t.Fatal("ResumeGuest() accepted a replaced container")
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("guest requests = %d, want 0", got)
	}
}

func TestConfirmExecutionRejectsReplacedContainer(t *testing.T) {
	b, w, p := resumeGuestFixture(t, true, "old-container")
	b.Runtime.(*resumeGuestCRI).containers[0].Id = "replacement-container"
	if _, err := b.ConfirmExecution(context.Background(), w, p); err == nil {
		t.Fatal("ConfirmExecution() accepted a replaced container")
	}
}

func TestResumeGuestHTTPFailureIsSanitizedAndRetryable(t *testing.T) {
	_, _, requests := startResumeGuestServer(t, http.StatusInternalServerError, "sensitive guest response", "")
	b, w, p := resumeGuestFixture(t, true, "current-container")
	err := b.ResumeGuest(context.Background(), w, p)
	if err == nil {
		t.Fatal("ResumeGuest() accepted HTTP 500")
	}
	if strings.Contains(err.Error(), "sensitive guest response") || strings.Contains(err.Error(), w.Spec.Container.Args[2]) {
		t.Fatalf("error leaked response/config: %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("guest requests = %d, want 1", got)
	}
}

func TestResumeGuestDoesNotFollowRedirect(t *testing.T) {
	_, _, requests := startResumeGuestServer(t, http.StatusNoContent, "", "http://127.0.0.1:1/unquiesce")
	b, w, p := resumeGuestFixture(t, true, "current-container")
	if err := b.ResumeGuest(context.Background(), w, p); err == nil {
		t.Fatal("ResumeGuest() accepted redirect")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("guest requests = %d, want 1", got)
	}
}
