package node

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"google.golang.org/grpc"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type pidCRI struct {
	executionCRI
	sid     string
	pid     int
	verbose int
}

func (c *pidCRI) ListPodSandbox(_ context.Context, _ *cri.ListPodSandboxRequest, _ ...grpc.CallOption) (*cri.ListPodSandboxResponse, error) {
	return &cri.ListPodSandboxResponse{Items: []*cri.PodSandbox{{Id: c.sid, Metadata: c.sandbox.Metadata, RuntimeHandler: api.RuntimeClass}}}, nil
}

func (c *pidCRI) PodSandboxStatus(_ context.Context, r *cri.PodSandboxStatusRequest, _ ...grpc.CallOption) (*cri.PodSandboxStatusResponse, error) {
	info := map[string]string{}
	if r.Verbose {
		c.verbose++
		data, _ := json.Marshal(map[string]int{"pid": c.pid})
		info["info"] = string(data)
	}
	return &cri.PodSandboxStatusResponse{Status: c.sandbox, Info: info}, nil
}

func TestPortExecutionAsksCRIForThePIDOnceAndRechecksARecycledOne(t *testing.T) {
	group, err := os.ReadFile("/proc/self/cgroup")
	if err != nil || len(group) < 3 {
		t.Skip("no cgroup information")
	}
	// The fake sandbox ID is text from this process's own cgroup.
	sid := string(group[:3])
	p := &core.Pod{ObjectMeta: meta.ObjectMeta{Name: "pod", Namespace: "boxes", UID: "uid"}}
	w := &api.ResumablePod{Spec: api.Spec{Container: core.Container{Name: "guest"}}, Status: api.Status{PodUID: "uid"}}
	c := &pidCRI{sid: sid, pid: os.Getpid()}
	c.sandbox = &cri.PodSandboxStatus{Id: sid, Metadata: &cri.PodSandboxMetadata{Name: p.Name, Namespace: p.Namespace, Uid: "uid"}, RuntimeHandler: api.RuntimeClass, State: cri.PodSandboxState_SANDBOX_READY, Network: &cri.PodSandboxNetworkStatus{Ip: "10.42.0.2"}}
	c.containers = []*cri.Container{{Id: "c", PodSandboxId: sid, Metadata: &cri.ContainerMetadata{Name: "guest"}, State: cri.ContainerState_CONTAINER_RUNNING, Labels: map[string]string{"io.kubernetes.pod.uid": "uid"}}}
	b := &Backend{Runtime: c}
	check := func(wantVerbose int) {
		t.Helper()
		execution, ns, err := b.portExecution(context.Background(), w, p)
		if err != nil || execution == nil || ns == nil {
			t.Fatalf("execution=%+v error=%v", execution, err)
		}
		ns.Close()
		if c.verbose != wantVerbose {
			t.Fatalf("verbose CRI calls = %d, want %d", c.verbose, wantVerbose)
		}
	}
	check(1)
	check(1)

	// A cached PID that no longer belongs to the sandbox is not trusted.
	b.sandboxPIDs[sid] = 1 << 30
	check(2)
}
