package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/jingyugao/cell-box/internal/service"
)

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
