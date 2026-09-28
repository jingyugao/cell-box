package resumable

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/guestapi"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NewForConfig constructs the real provider for an explicitly supplied cluster.
func NewForConfig(cfg *rest.Config) (*Provider, error) {
	if cfg == nil {
		return nil, errors.New("Kubernetes REST config is required")
	}
	scheme := runtime.NewScheme()
	if err := core.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := api.AddToScheme(scheme); err != nil {
		return nil, err
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return New(c, &SPDYTokenExec{Config: cfg, Kube: kube}), nil
}

// NewInCluster uses the Pod's service account. The account needs scoped
// ResumablePod, Pod and Service permissions plus pods/exec creation.
func NewInCluster() (*Provider, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return NewForConfig(cfg)
}

// NewForKubeconfig supports operator tools without inheriting an ambient context.
func NewForKubeconfig(path string) (*Provider, error) {
	if path == "" {
		return nil, errors.New("kubeconfig path is required")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		return nil, err
	}
	return NewForConfig(cfg)
}

type SPDYTokenExec struct {
	Config *rest.Config
	Kube   kubernetes.Interface
}

func (e *SPDYTokenExec) Token(ctx context.Context, namespace, pod, container string) (string, error) {
	if e == nil || e.Config == nil || e.Kube == nil {
		return "", errors.New("Kubernetes exec client is required")
	}
	if namespace == "" || pod == "" || container == "" {
		return "", errors.New("Pod exec target is required")
	}
	return e.tokenWithCodec(ctx, namespace, pod, container)
}

func (e *SPDYTokenExec) tokenWithCodec(ctx context.Context, namespace, pod, container string) (string, error) {
	scheme := runtime.NewScheme()
	if err := core.AddToScheme(scheme); err != nil {
		return "", err
	}
	req := e.Kube.CoreV1().RESTClient().Post().Resource("pods").Namespace(namespace).Name(pod).SubResource("exec")
	req.VersionedParams(&core.PodExecOptions{Container: container, Command: []string{guestapi.Binary, "token"}, Stdout: true, Stderr: true}, runtime.NewParameterCodec(scheme))
	exec, err := remotecommand.NewSPDYExecutor(e.Config, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("construct guest token exec: %w", err)
	}
	var stdout boundedTokenWriter
	if err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: io.Discard}); err != nil {
		// stderr may include sensitive guest content; do not return it.
		return "", fmt.Errorf("guest token exec failed: %w", err)
	}
	return strings.TrimSpace(stdout.String()), nil
}

// boundedTokenWriter prevents an untrusted guest from growing provider memory.
type boundedTokenWriter struct{ bytes.Buffer }

func (w *boundedTokenWriter) Write(p []byte) (int, error) {
	if w.Len()+len(p) > 4096 {
		return 0, errors.New("guest token response exceeds limit")
	}
	return w.Buffer.Write(p)
}
