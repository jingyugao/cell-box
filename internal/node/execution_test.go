package node

import (
	"context"
	"errors"
	"testing"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	runtimebackend "github.com/jingyugao/cell-box/internal/runtime"
	"google.golang.org/grpc"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type executionCRI struct {
	cri.RuntimeServiceClient
	containers []*cri.Container
	sandbox    *cri.PodSandboxStatus
}

func (c *executionCRI) ListContainers(context.Context, *cri.ListContainersRequest, ...grpc.CallOption) (*cri.ListContainersResponse, error) {
	return &cri.ListContainersResponse{Containers: c.containers}, nil
}
func (c *executionCRI) PodSandboxStatus(context.Context, *cri.PodSandboxStatusRequest, ...grpc.CallOption) (*cri.PodSandboxStatusResponse, error) {
	return &cri.PodSandboxStatusResponse{Status: c.sandbox}, nil
}
func TestWarmExecutionRequiresStartedBusinessContainer(t *testing.T) {
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "warm", Namespace: "boxes", UID: "uid"}}
	w := &api.ResumablePod{Spec: api.Spec{Container: core.Container{Name: "guest"}}}
	c := &executionCRI{sandbox: &cri.PodSandboxStatus{Id: "sandbox", Metadata: &cri.PodSandboxMetadata{Name: p.Name, Namespace: p.Namespace, Uid: string(p.UID)}, RuntimeHandler: api.RuntimeClass, State: cri.PodSandboxState_SANDBOX_READY, Network: &cri.PodSandboxNetworkStatus{Ip: "10.42.0.2"}}}
	b := &Backend{Runtime: c}
	check := func(want bool) {
		t.Helper()
		e, err := b.warmExecution(context.Background(), w, p, "sandbox")
		if err != nil || (e != nil) != want {
			t.Fatalf("execution=%+v error=%v", e, err)
		}
	}
	check(false)
	c.containers = []*cri.Container{{Id: "child", PodSandboxId: "sandbox", Metadata: &cri.ContainerMetadata{Name: "guest"}, Labels: map[string]string{"io.kubernetes.pod.uid": "uid"}, State: cri.ContainerState_CONTAINER_CREATED}}
	check(false)
	// Port readiness observes restored sockets even while CRI still reports
	// the application container as CREATED.
	e, err := b.inspectExecution(context.Background(), w, p, "sandbox", false)
	if err != nil || e == nil {
		t.Fatalf("restoring execution=%+v error=%v", e, err)
	}
	c.containers[0].State = cri.ContainerState_CONTAINER_RUNNING
	check(true)
	c.containers[0].Metadata.Attempt = 1
	if _, err := b.warmExecution(context.Background(), w, p, "sandbox"); err == nil {
		t.Fatal("restarted container accepted")
	}
	c.containers[0].Metadata.Attempt = 0
	c.sandbox.Metadata.Uid = "other"
	if _, err := b.warmExecution(context.Background(), w, p, "sandbox"); err == nil {
		t.Fatal("another sandbox accepted")
	}
}

func TestCheckpointReadsTheImageFromCRIAndWaitsForAStartInFlight(t *testing.T) {
	w := &api.ResumablePod{Spec: api.Spec{Container: core.Container{Name: "guest"}}}
	guest := &cri.Container{Id: "child", Metadata: &cri.ContainerMetadata{Name: "guest"}, ImageRef: "sha256:config", State: cri.ContainerState_CONTAINER_CREATED}
	old := &cri.Container{Id: "old", Metadata: &cri.ContainerMetadata{Name: "guest"}, ImageRef: "sha256:other", State: cri.ContainerState_CONTAINER_EXITED}
	b := &Backend{Runtime: &executionCRI{containers: []*cri.Container{old, guest}}}
	if _, err := b.runningContainerImage(context.Background(), w, "sandbox"); !errors.Is(err, runtimebackend.ErrRetryableStorage) {
		t.Fatalf("a start in flight must be retried without failing the Box: %v", err)
	}
	guest.State = cri.ContainerState_CONTAINER_RUNNING
	if image, err := b.runningContainerImage(context.Background(), w, "sandbox"); err != nil || image != "sha256:config" {
		t.Fatalf("image=%q error=%v", image, err)
	}
}
