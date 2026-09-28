// SPDX-License-Identifier: Apache-2.0

package main

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/controller"
	"cellbox.local/cellbox/internal/node"
	"cellbox.local/cellbox/internal/version"
	"flag"
	"fmt"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"os"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metrics "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	var nodeName, namespace, leaderNamespace, socket string
	var showVersion, hostMountNamespace bool
	flag.BoolVar(&showVersion, "version", false, "print build version and exit")
	flag.BoolVar(&hostMountNamespace, "host-mount-namespace", false, "run runsc in the host mount/root namespace (requires privileged hostPID Pod)")
	flag.StringVar(&nodeName, "node", "", "Kubernetes node name (must equal kubernetes.io/hostname)")
	flag.StringVar(&namespace, "namespace", "recoverable-system", "managed ResumablePod namespace")
	flag.StringVar(&leaderNamespace, "leader-election-namespace", "", "leader election namespace (defaults to managed namespace)")
	flag.StringVar(&socket, "cri-socket", "/run/k3s/containerd/containerd.sock", "CRI Unix socket")
	flag.Parse()
	if showVersion {
		fmt.Println(version.String("resumablepod-controller"))
		return nil
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
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: scheme, Cache: cache.Options{DefaultNamespaces: map[string]cache.Config{namespace: {}}}, LeaderElection: true, LeaderElectionID: "resumablepod-" + nodeName, LeaderElectionNamespace: leaderNamespace, Metrics: metrics.Options{BindAddress: "0"}, HealthProbeBindAddress: "0"})
	if err != nil {
		return err
	}
	backend, err := node.New(socket)
	if err != nil {
		return err
	}
	backend.HostMountNamespace = hostMountNamespace
	r := &controller.Reconciler{Client: mgr.GetClient(), Runtime: backend, NodeName: nodeName}
	// Uncached reads avoid stale Pod adoption/deletion during host side effects.
	direct, err := clientNew(mgr)
	if err != nil {
		return err
	}
	r.Client = direct
	if err = ctrl.NewControllerManagedBy(mgr).For(&api.ResumablePod{}).Owns(&core.Pod{}).Complete(r); err != nil {
		return err
	}
	return mgr.Start(ctrl.SetupSignalHandler())
}
