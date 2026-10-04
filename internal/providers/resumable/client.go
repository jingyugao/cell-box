package resumable

import (
	"errors"

	api "cellbox.local/cellbox/api/v1alpha1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NewForConfig constructs the real provider for an explicitly supplied cluster.
func NewForConfig(cfg *rest.Config) (*Provider, error) {
	if cfg == nil {
		return nil, errors.New("Kubernetes REST config is required")
	}
	// This API performs several short reads per lifecycle operation. The
	// client-go 5 QPS default makes a single sandbox wait on its own limiter.
	cfg = rest.CopyConfig(cfg)
	if cfg.QPS == 0 {
		cfg.QPS = 50
	}
	if cfg.Burst == 0 {
		cfg.Burst = 100
	}
	scheme := runtime.NewScheme()
	if err := core.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := api.AddToScheme(scheme); err != nil {
		return nil, err
	}
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	return New(c), nil
}

// NewInCluster uses the Pod's service account. The account needs scoped
// ResumablePod, Pod and Service permissions.
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
