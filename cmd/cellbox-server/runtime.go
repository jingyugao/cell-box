package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/controller"
	"cellbox.local/cellbox/internal/node"
	"cellbox.local/cellbox/internal/providers/docker"
	"cellbox.local/cellbox/internal/providers/localrunsc"
	"cellbox.local/cellbox/internal/providers/resumable"
	"cellbox.local/cellbox/internal/runtimecontrol"
	"cellbox.local/cellbox/internal/service"
	"cellbox.local/cellbox/internal/telemetry"
	"cellbox.local/cellbox/internal/warmtemplates"
	core "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	controlleroptions "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metrics "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

type nodeOptions struct {
	Node, Namespace, Socket string
	LocalGuestBinary        string
	LocalPlatform           string
	HostMountNamespace      bool
	WarmPoolSize            int
}

func (o nodeOptions) validate(config service.Config) error {
	if o.WarmPoolSize < 0 || o.WarmPoolSize > 32 {
		return errors.New("warm pool size must be between 0 and 32")
	}
	if o.Node != "" && (o.Namespace == "" || o.Socket == "") {
		return errors.New("node runtime requires --namespace and --cri-socket")
	}
	localRunsc := false
	for _, p := range config.Profiles {
		if p.Provider == "local-runsc" {
			localRunsc = true
			continue
		}
		if p.Provider != "resumable-k8s-pod" {
			continue
		}
		if o.Node == "" || o.Namespace == "" || o.Socket == "" {
			return errors.New("resumable runtime requires --node, --namespace and --cri-socket")
		}
		if p.NodeName != o.Node || p.Namespace != o.Namespace {
			return fmt.Errorf("profile %q must use this server's node and namespace", p.ID)
		}
	}
	if localRunsc && o.LocalPlatform != "systrap" && o.LocalPlatform != "kvm" {
		return errors.New("--local-runsc-platform must be systrap or kvm")
	}
	return nil
}

// newRuntime wires the same client, event source and desired-config reader into both sides.
// The manager has no separate leader election: the process lease already fences
// HTTP handlers, recovery workers, lifecycle work and warm pool maintenance.
func newRuntime(kube *rest.Config, options nodeOptions, readDesired func(context.Context) ([]warmtemplates.Template, string, error)) (*resumable.Provider, manager.Manager, *node.Backend, error) {
	p, err := resumable.NewForConfig(kube)
	if err != nil {
		return nil, nil, nil, errors.New("cannot initialize Kubernetes provider")
	}
	kube = rest.CopyConfig(kube)
	if kube.QPS == 0 {
		kube.QPS = 50
	}
	if kube.Burst == 0 {
		kube.Burst = 100
	}
	// Never return while a node side effect is still running. The process lease
	// is not released on cancellation; takeover remains fenced during shutdown.
	grace := time.Duration(-1)
	mgr, err := ctrl.NewManager(kube, ctrl.Options{Scheme: p.Client.Scheme(),
		Cache:          cache.Options{DefaultNamespaces: map[string]cache.Config{options.Namespace: {}}},
		LeaderElection: false, Metrics: metrics.Options{BindAddress: "0"}, HealthProbeBindAddress: "0", GracefulShutdownTimeout: &grace})
	if err != nil {
		return nil, nil, nil, err
	}
	backend, err := node.New(options.Socket)
	if err != nil {
		return nil, nil, nil, err
	}
	failed := true
	defer func() {
		if failed {
			_ = backend.Close()
		}
	}()
	backend.HostMountNamespace = options.HostMountNamespace
	local := runtimecontrol.New(p.Client)
	p.Client, p.Local = local, local
	p.ConfirmExecutionLocal = backend.ConfirmExecution
	p.CreateVolumeLocal = func(ctx context.Context, spec boxprovider.VolumeSpec) error {
		if spec.NodeName != options.Node || spec.Namespace != options.Namespace {
			return errors.New("volume target differs from this server's node or namespace")
		}
		return backend.CreateVolume(spec.UID, spec.NodeName, spec.Agent)
	}
	p.DeleteVolumeLocal = func(ctx context.Context, spec boxprovider.VolumeSpec) error {
		if spec.NodeName != options.Node || spec.Namespace != options.Namespace {
			return errors.New("volume target differs from this server's node or namespace")
		}
		return backend.DeleteVolume(spec.UID, spec.NodeName)
	}
	p.WithStoppedVolumeLocal = func(ctx context.Context, spec boxprovider.VolumeSpec, work func(string) error) error {
		if spec.NodeName != options.Node || spec.Namespace != options.Namespace {
			return errors.New("volume target differs from this server's node or namespace")
		}
		return backend.WithStoppedVolume(ctx, spec.UID, spec.NodeName, work)
	}
	p.CacheImageLocal = func(ctx context.Context, image, nodeName, namespace string) error {
		if nodeName != options.Node || namespace != options.Namespace {
			return errors.New("image cache target differs from this server's node or namespace")
		}
		pull, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()
		return backend.CacheImage(pull, image)
	}
	r := &controller.Reconciler{Client: local, Runtime: backend, NodeName: options.Node}
	if err = mgr.Add(&runtimecontrol.Synchronizer{Client: local}); err != nil {
		return nil, nil, nil, err
	}
	r.WarmPool = &controller.WarmPool{Reconciler: r, Namespace: options.Namespace, Size: options.WarmPoolSize, ReadDesired: readDesired, PublishObservation: p.SetWarmSnapshot}
	if err = mgr.Add(r.WarmPool); err != nil {
		return nil, nil, nil, err
	}
	if err = mgr.Add(&node.GarbageCollector{Backend: backend}); err != nil {
		return nil, nil, nil, err
	}
	// CR/Pod watches recover external changes and seed work after restart. Local
	// requests use the direct source and never wait for those watch events.
	err = ctrl.NewControllerManagedBy(mgr).For(&api.ResumablePod{}).Owns(&core.Pod{}).
		WatchesRawSource(local).WithOptions(controlleroptions.Options{MaxConcurrentReconciles: 4}).Complete(r)
	if err != nil {
		return nil, nil, nil, err
	}
	failed = false
	return p, mgr, backend, nil
}

func serveUnified(parent context.Context, config service.Config, dockerBinary string, kube *rest.Config, options nodeOptions) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	providers := map[string]boxprovider.Provider{}
	var mgr manager.Manager
	var backend *node.Backend
	var app *service.Service
	if options.Node != "" {
		p, m, b, err := newRuntime(kube, options, func(ctx context.Context) ([]warmtemplates.Template, string, error) {
			if app == nil {
				return nil, "", errors.New("template store is not initialized")
			}
			desired, err := app.DesiredWarmTemplates(ctx)
			if err != nil {
				return nil, "", err
			}
			records, _, err := resumable.BuildWarmTemplates(options.Namespace, desired)
			return records, service.WarmDesiredHash(desired), err
		})
		if err != nil {
			return err
		}
		providers[p.Name()], mgr, backend = p, m, b
		defer backend.Close()

	}
	for _, profile := range config.Profiles {
		if providers[profile.Provider] != nil {
			continue
		}
		switch profile.Provider {
		case "docker":
			providers[profile.Provider] = docker.New(dockerBinary)
		case "local-runsc":
			provider, err := localrunsc.New(config.DataDir, options.LocalGuestBinary, options.LocalPlatform)
			if err != nil {
				return fmt.Errorf("cannot initialize local-runsc provider: %w", err)
			}
			providers[profile.Provider] = provider
			defer provider.Close()
		}
	}
	var err error
	app, err = service.NewContext(ctx, config, providers)
	if err != nil {
		return fmt.Errorf("cannot start Cellbox service: %w", err)
	}
	// Sharing the initialized store avoids a second import, ownership or close
	// lifetime. No runtime worker starts until this pointer is installed.
	if backend != nil {
		backend.Objects = app.RuntimeObjects()
		backend.Checkpoints = app.CheckpointStore()
		provider := providers["resumable-k8s-pod"].(*resumable.Provider)
		if err := provider.Local.SetJournal(ctx, app.RuntimeJournal()); err != nil {
			return errors.Join(err, app.Close())
		}
	}
	if provider, ok := providers["local-runsc"].(*localrunsc.Provider); ok {
		if err := provider.SetCheckpointStore(ctx, app.CheckpointStore()); err != nil {
			return errors.Join(err, app.Close())
		}
	}
	runtimeResult := make(chan error, 1)
	if mgr != nil {
		go func() { runtimeResult <- mgr.Start(ctx) }()
	}
	server := &http.Server{Addr: config.Listen, Handler: telemetry.HTTPHandler(app.Handler()), ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 1 << 20, BaseContext: func(net.Listener) context.Context { return ctx }}
	httpResult := make(chan error, 1)
	go func() { httpResult <- server.ListenAndServe() }()
	var result error
	var httpStopped, runtimeStopped bool
	select {
	case <-app.Done():
		if parent.Err() == nil {
			result = errors.New("Cellbox stopped after a durable state write failure")
		}
	case result = <-runtimeResult:
		runtimeStopped = true
		if result == nil && parent.Err() == nil {
			result = errors.New("embedded runtime stopped unexpectedly")
		}
	case result = <-httpResult:
		httpStopped = true
		if errors.Is(result, http.ErrServerClosed) {
			result = nil
		}
	}
	cancel()
	shutdown, stop := context.WithTimeout(context.Background(), 15*time.Second)
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		result = errors.Join(result, err)
	}
	stop()
	if !httpStopped {
		if err := <-httpResult; !errors.Is(err, http.ErrServerClosed) {
			result = errors.Join(result, err)
		}
	}
	// Join runtime work BEFORE app.Close closes its metadata store. The lease
	// holder waits for this entire function before its process can exit normally.
	if mgr != nil && !runtimeStopped {
		result = errors.Join(result, <-runtimeResult)
	}
	return errors.Join(result, app.Close())
}
