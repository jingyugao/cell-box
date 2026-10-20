package main

import (
	"context"
	"net"
	"testing"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/node"
	"cellbox.local/cellbox/internal/service"
)

func TestNodeOptionsRuntimeDefaultsAndOverrides(t *testing.T) {
	defaults := nodeOptions{}
	if defaults.runtimeClass() != api.RuntimeClass || defaults.runtimeHandler() != api.RuntimeClass || defaults.nodeBase() != node.DefaultBase || defaults.runsc() != node.DefaultRunsc || defaults.shim() != node.DefaultShim {
		t.Fatalf("unexpected defaults: class=%q handler=%q base=%q runsc=%q shim=%q", defaults.runtimeClass(), defaults.runtimeHandler(), defaults.nodeBase(), defaults.runsc(), defaults.shim())
	}
	custom := nodeOptions{RuntimeClass: "runsc-cellbox-dev", RuntimeHandler: "gvisor-bench-pause", NodeBase: "/var/lib/cellbox-dev", Runsc: "/opt/cellbox-dev/runsc", Shim: "/usr/local/bin/gvisor-cellbox-dev/4320a3de3afb/containerd-shim-runsc-v1"}
	if custom.runtimeClass() != "runsc-cellbox-dev" || custom.runtimeHandler() != "gvisor-bench-pause" || custom.nodeBase() != "/var/lib/cellbox-dev" || custom.runsc() != "/opt/cellbox-dev/runsc" || custom.shim() != custom.Shim {
		t.Fatalf("custom runtime settings were not preserved: %+v", custom)
	}
	custom.RuntimeHandler = ""
	if custom.runtimeHandler() != custom.RuntimeClass {
		t.Fatalf("empty CRI handler should default to RuntimeClass: got %q", custom.runtimeHandler())
	}
}

func TestServerRejectsProfilesAssignedToAnotherRuntime(t *testing.T) {
	options := nodeOptions{Node: "node-a", Namespace: "cell-box", Socket: "/run/containerd/containerd.sock", WarmPoolSize: 2}
	config := service.Config{Profiles: []service.Profile{{ID: "agent", Provider: "resumable-k8s-pod", NodeName: "node-a", Namespace: "cell-box"}}}
	if err := options.validate(config); err != nil {
		t.Fatal(err)
	}
	for _, wrong := range []nodeOptions{
		{Node: "node-b", Namespace: options.Namespace, Socket: options.Socket},
		{Node: options.Node, Namespace: "other", Socket: options.Socket},
		{Namespace: options.Namespace, Socket: options.Socket},
		{Node: options.Node, Namespace: options.Namespace},
		{Node: options.Node, Namespace: options.Namespace, Socket: options.Socket, WarmPoolSize: 33},
		{Node: options.Node, Namespace: options.Namespace, Socket: options.Socket, NodeBase: "/"},
		{Node: options.Node, Namespace: options.Namespace, Socket: options.Socket, Runsc: "bin/runsc"},
		{Node: options.Node, Namespace: options.Namespace, Socket: options.Socket, Shim: "/usr/local/bin/../containerd-shim-runsc-v1"},
	} {
		if err := wrong.validate(config); err == nil {
			t.Fatalf("accepted wrong local runtime: %+v", wrong)
		}
	}
}

func TestHTTPBindFailureStopsServiceAndReleasesMetadataLock(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	config := service.Config{Listen: listener.Addr().String(), DataDir: t.TempDir()}
	done := make(chan error, 1)
	go func() { done <- serveUnified(t.Context(), config, "docker", nil, nodeOptions{}) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("bind conflict was ignored")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP failure left background service running")
	}
	app, err := service.NewContext(context.Background(), config, nil)
	if err != nil {
		t.Fatalf("server retained metadata ownership after failure: %v", err)
	}
	if err = app.Close(); err != nil {
		t.Fatal(err)
	}
}
