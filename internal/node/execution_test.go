package node

import (
	"context"
	"testing"

	api "cellbox.local/cellbox/api/v1alpha1"
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
