package node

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	core "k8s.io/api/core/v1"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type failureEvidence struct {
	CapturedAt time.Time           `json:"capturedAt"`
	OwnerUID   string              `json:"ownerUID"`
	BoxName    string              `json:"boxName"`
	PodUID     string              `json:"podUID"`
	PodName    string              `json:"podName"`
	Node       string              `json:"node"`
	Message    string              `json:"message"`
	PodStatus  *core.PodStatus     `json:"podStatus,omitempty"`
	Sandboxes  []string            `json:"sandboxIDs,omitempty"`
	Containers []containerEvidence `json:"containers,omitempty"`
	Memory     map[string]string   `json:"memory,omitempty"`
	Journals   map[string]string   `json:"journals,omitempty"`
	Errors     []string            `json:"errors,omitempty"`
}

type containerEvidence struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	ExitCode   int32  `json:"exitCode"`
	Reason     string `json:"reason"`
	Message    string `json:"message"`
	StartedAt  int64  `json:"startedAt"`
	FinishedAt int64  `json:"finishedAt"`
	LogTail    string `json:"logTail,omitempty"`
}

// CaptureFailure runs before Pod deletion and CRI/runsc cleanup. Failures of
// individual probes are evidence too; only failure to persist blocks cleanup.
// Neither Pod specs nor CRI verbose info (which can contain credentials) is saved.
func (b *Backend) CaptureFailure(ctx context.Context, w *api.ResumablePod, p *core.Pod) error {
	owner, uid := string(w.UID), w.Status.PodUID
	if uid == "" && p != nil {
		uid = string(p.UID)
	}
	if !ValidID(owner) || (uid != "" && !ValidID(uid)) {
		return fmt.Errorf("invalid failure evidence identity")
	}
	execution := uid
	if execution == "" {
		execution = fmt.Sprintf("unassigned-cycle-%d", w.Status.Cycle)
	}
	path := filepath.Join(b.Base, "diagnostics", owner, execution, "failure.json")
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	e := failureEvidence{CapturedAt: time.Now().UTC(), OwnerUID: owner, BoxName: w.Name, PodUID: uid,
		PodName: w.Status.PodName, Node: w.Spec.NodeName, Message: w.Status.Message,
		Memory: map[string]string{}, Journals: map[string]string{}}
	if p != nil {
		e.PodStatus = p.Status.DeepCopy()
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if b.Runtime != nil && uid != "" {
		res, err := b.Runtime.ListPodSandbox(ctx, &cri.ListPodSandboxRequest{Filter: &cri.PodSandboxFilter{LabelSelector: map[string]string{"io.kubernetes.pod.uid": uid}}})
		if err != nil {
			e.Errors = append(e.Errors, "list sandboxes: "+err.Error())
		} else {
			for _, s := range res.Items {
				if s.Metadata == nil || s.Metadata.Uid != uid || s.Metadata.Name != e.PodName || s.Metadata.Namespace != w.Namespace || s.RuntimeHandler != api.RuntimeClass {
					continue
				}
				e.Sandboxes = append(e.Sandboxes, s.Id)
			}
		}
		containers, err := b.Runtime.ListContainers(ctx, &cri.ListContainersRequest{Filter: &cri.ContainerFilter{LabelSelector: map[string]string{"io.kubernetes.pod.uid": uid}}})
		if err != nil {
			e.Errors = append(e.Errors, "list containers: "+err.Error())
		} else {
			for _, c := range containers.Containers {
				if c.Labels["io.kubernetes.pod.uid"] != uid {
					continue
				}
				status, err := b.Runtime.ContainerStatus(ctx, &cri.ContainerStatusRequest{ContainerId: c.Id})
				if err != nil {
					e.Errors = append(e.Errors, "container "+c.Id+": "+err.Error())
					continue
				}
				if s := status.Status; s != nil {
					item := containerEvidence{ID: s.Id, State: s.State.String(), ExitCode: s.ExitCode, Reason: s.Reason, Message: s.Message, StartedAt: s.StartedAt, FinishedAt: s.FinishedAt}
					// CRI returns a node path. Only this exact Pod's log directory
					// may be read, never an arbitrary path from runtime metadata.
					prefix := filepath.Join("/var/log/pods", w.Namespace+"_"+e.PodName+"_"+uid) + string(os.PathSeparator)
					if strings.HasPrefix(filepath.Clean(s.LogPath), prefix) {
						logPath := s.LogPath
						if b.HostMountNamespace {
							logPath = filepath.Join("/proc/1/root", logPath)
						}
						data, err := readDiagnosticTail(logPath, 64*1024)
						if err != nil {
							e.Errors = append(e.Errors, "container log "+s.Id+": "+err.Error())
						} else {
							item.LogTail = string(data)
						}
					}
					e.Containers = append(e.Containers, item)
				}
			}
		}
	}
	// The CRI record may already be gone, but the adapter authorization is kept
	// until cleanup. Retain its sandbox identity without storing configuration.
	entries, err := os.ReadDir(filepath.Join(b.Base, "requests"))
	if err != nil && !os.IsNotExist(err) {
		e.Errors = append(e.Errors, err.Error())
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		var t Ticket
		if ReadJSON(filepath.Join(b.Base, "requests", entry.Name()), &t) == nil && t.OwnerUID == owner && t.PodUID == uid {
			id := strings.TrimSuffix(entry.Name(), ".json")
			if ValidID(id) && !containsID(e.Sandboxes, id) {
				e.Sandboxes = append(e.Sandboxes, id)
			}
		}
	}
	b.captureMemory(&e)
	// K3s writes containerd's runtime errors to a file rather than its journal.
	// Discover the installed node path from the mounted CRI socket directory.
	if b.HostMountNamespace && b.Socket == "/run/k3s/containerd/containerd.sock" {
		data, err := readDiagnosticTail("/proc/1/root/var/lib/rancher/k3s/agent/containerd/containerd.log", 512*1024)
		if err != nil {
			e.Errors = append(e.Errors, "containerd log: "+err.Error())
		} else {
			e.Journals["containerd"] = string(data)
		}
	}
	for _, unit := range []string{"k3s", "kernel"} {
		args := []string{"--since=-5min", "--no-pager", "-o", "short-iso", "-n", "250"}
		if unit == "kernel" {
			args = append(args, "-k")
		} else {
			args = append(args, "-u", unit)
		}
		command := "journalctl"
		if b.HostMountNamespace {
			command = "nsenter"
			args = append([]string{"-t", "1", "-m", "--root", "--", "journalctl"}, args...)
		}
		probe, stop := context.WithTimeout(ctx, 5*time.Second)
		cmd := exec.CommandContext(probe, command, args...)
		cmd.WaitDelay = time.Second
		out := &diagnosticTail{limit: 256 * 1024}
		cmd.Stdout, cmd.Stderr = out, out
		if err := cmd.Run(); err != nil {
			e.Errors = append(e.Errors, unit+" journal: "+err.Error())
		}
		stop()
		e.Journals[unit] = string(out.data)
	}
	if err := AtomicJSON(path, e); err != nil {
		return err
	}
	log.Printf("runtime failure evidence saved box=%s pod=%s path=%s", w.Name, uid, path)
	return nil
}

func readDiagnosticTail(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, fmt.Errorf("diagnostic source is not a regular file")
	}
	if st.Size() > limit {
		if _, err := f.Seek(st.Size()-limit, io.SeekStart); err != nil {
			return nil, err
		}
	}
	return io.ReadAll(io.LimitReader(f, limit))
}

func containsID(ids []string, id string) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

type diagnosticTail struct {
	limit int
	data  []byte
}

func (t *diagnosticTail) Write(p []byte) (int, error) {
	n := len(p)
	t.data = append(t.data, p...)
	if len(t.data) > t.limit {
		t.data = append([]byte(nil), t.data[len(t.data)-t.limit:]...)
	}
	return n, nil
}

func (b *Backend) captureMemory(e *failureEvidence) {
	if len(e.Sandboxes) == 0 {
		return
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		e.Errors = append(e.Errors, "process inventory: "+err.Error())
		return
	}
	for _, entry := range entries {
		proc := filepath.Join("/proc", entry.Name())
		cmd, err := os.ReadFile(filepath.Join(proc, "cmdline"))
		if err != nil {
			continue
		}
		matched := false
		for _, arg := range strings.Split(string(cmd), "\x00") {
			if containsID(e.Sandboxes, arg) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		groups, err := os.ReadFile(filepath.Join(proc, "cgroup"))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(groups), "\n") {
			if !strings.HasPrefix(line, "0::/") {
				continue
			}
			group := strings.TrimPrefix(line, "0::")
			root := "/sys/fs/cgroup"
			if b.HostMountNamespace {
				root = "/proc/1/root/sys/fs/cgroup"
			}
			for _, name := range []string{"memory.events", "memory.events.local", "memory.current", "memory.peak", "memory.max"} {
				if data, err := os.ReadFile(filepath.Join(root, group, name)); err == nil {
					e.Memory[group+"/"+name] = string(data)
				}
			}
		}
	}
}
