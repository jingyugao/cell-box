// SPDX-License-Identifier: Apache-2.0

package main

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/controller"
	"cellbox.local/cellbox/internal/imagecache"
	"cellbox.local/cellbox/internal/node"
	"cellbox.local/cellbox/internal/objectstorage"
	"cellbox.local/cellbox/internal/version"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"os"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	controlleroptions "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metrics "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	var nodeName, namespace, leaderNamespace, socket, objectStorageConfig string
	var showVersion, hostMountNamespace bool
	var warmPoolSize int
	flag.IntVar(&warmPoolSize, "warm-pool-size", 2, "maximum unassigned warm Pods per node (0 disables, one extra during rotation)")
	flag.BoolVar(&showVersion, "version", false, "print build version and exit")
	flag.BoolVar(&hostMountNamespace, "host-mount-namespace", false, "run runsc in the host mount/root namespace (requires privileged hostPID Pod)")
	flag.StringVar(&nodeName, "node", "", "Kubernetes node name (must equal kubernetes.io/hostname)")
	flag.StringVar(&namespace, "namespace", "cell-box", "managed Cellbox namespace")
	flag.StringVar(&leaderNamespace, "leader-election-namespace", "", "leader election namespace (defaults to managed namespace)")
	flag.StringVar(&socket, "cri-socket", "/run/containerd/containerd.sock", "CRI Unix socket")
	flag.StringVar(&objectStorageConfig, "object-storage-config", "", "path to S3-compatible checkpoint storage configuration JSON")
	flag.Parse()
	if showVersion {
		fmt.Println(version.String("cellbox-node-controller"))
		return nil
	}
	if warmPoolSize < 0 || warmPoolSize > 32 {
		return fmt.Errorf("warm pool size must be between 0 and 32")
	}
	if nodeName == "" {
		return fmt.Errorf("--node is required")
	}
	if leaderNamespace == "" {
		leaderNamespace = namespace
	}
	ctrl.SetLogger(zap.New())
	scheme := runtime.NewScheme()
	core.AddToScheme(scheme)
	api.AddToScheme(scheme)
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme, Cache: cache.Options{DefaultNamespaces: map[string]cache.Config{namespace: {}}}, LeaderElection: true, LeaderElectionID: "cellbox-" + nodeName, LeaderElectionNamespace: leaderNamespace, Metrics: metrics.Options{BindAddress: "0"}, HealthProbeBindAddress: "0"})
	if err != nil {
		return err
	}
	backend, err := node.New(socket)
	if err != nil {
		return err
	}
	if objectStorageConfig != "" {
		data, readErr := os.ReadFile(objectStorageConfig)
		if readErr != nil {
			return fmt.Errorf("cannot read object storage configuration: %w", readErr)
		}
		var config objectstorage.Config
		if decodeErr := json.Unmarshal(data, &config); decodeErr != nil {
			return fmt.Errorf("invalid object storage configuration")
		}
		objects, storageErr := objectstorage.New(context.Background(), config)
		if storageErr != nil {
			return storageErr
		}
		backend.Objects = objects
	}
	backend.HostMountNamespace = hostMountNamespace
	r := &controller.Reconciler{Client: mgr.GetClient(), Runtime: backend, NodeName: nodeName}
	// Uncached reads avoid stale Pod adoption/deletion during host side effects.
	direct, err := clientNew(mgr)
	if err != nil {
		return err
	}
	r.Client = direct
	pool := &controller.WarmPool{Reconciler: r, Namespace: namespace, Size: warmPoolSize}
	r.WarmPool = pool
	if err = mgr.Add(pool); err != nil {
		return err
	}
	if err = mgr.Add(&node.GarbageCollector{Backend: backend}); err != nil {
		return err
	}
	if err = ctrl.NewControllerManagedBy(mgr).For(&api.ResumablePod{}).Owns(&core.Pod{}).WithOptions(controlleroptions.Options{MaxConcurrentReconciles: 4}).Complete(r); err != nil {
		return err
	}
	if err = ctrl.NewControllerManagedBy(mgr).Named("image-cache").For(&core.ConfigMap{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(obj client.Object) bool { return obj.GetLabels()[imagecache.Label] == "true" })).
		WithOptions(controlleroptions.Options{MaxConcurrentReconciles: 2}).
		Complete(&controller.ImageCacheReconciler{Client: direct, Runtime: backend, NodeName: nodeName}); err != nil {
		return err
	}
	return mgr.Start(ctrl.SetupSignalHandler())
}
