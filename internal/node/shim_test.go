// SPDX-License-Identifier: Apache-2.0

package node

import (
	"strings"
	"testing"
)

func TestShimIdentity(t *testing.T) {
	id := strings.Repeat("a", 64)
	socket := "/run/k3s/containerd/containerd.sock"
	cmd := []byte("shim\x00-namespace\x00k8s.io\x00-address\x00" + socket + "\x00-id\x00" + id + "\x00")
	exe := "/usr/local/bin/containerd-shim-runsc-v1"
	if !matchesShim(exe, exe, cmd, id, socket) {
		t.Fatal("exact identity rejected")
	}
	for _, bad := range []struct{ exe, expectedExe, id, socket string }{
		{"/tmp/containerd-shim-runsc-v1", exe, id, socket},
		{"/usr/local/bin/gvisor-cellbox-dev/4320a3de3afb/containerd-shim-runsc-v1", exe, id, socket},
		{exe, "/usr/local/bin/containerd-shim-runsc-v1-backup", id, socket},
		{exe, exe, strings.Repeat("b", 64), socket},
		{exe, exe, id, "/run/other.sock"},
	} {
		if matchesShim(bad.exe, bad.expectedExe, cmd, bad.id, bad.socket) {
			t.Fatal("foreign shim matched")
		}
	}
	if matchesShim(exe, exe, []byte(strings.ReplaceAll(string(cmd), "k8s.io", "default")), id, socket) {
		t.Fatal("wrong namespace matched")
	}
	custom := "/usr/local/bin/gvisor-cellbox-dev/4320a3de3afb/containerd-shim-runsc-v1"
	if !matchesShim(custom, custom, cmd, id, socket) {
		t.Fatal("configured dev shim identity rejected")
	}
	if matchesShim(custom+"-lookalike", custom, cmd, id, socket) {
		t.Fatal("lookalike executable path matched configured shim")
	}
}

func TestBackendShimDefaultsAndOverride(t *testing.T) {
	if got := (&Backend{}).shim(); got != DefaultShim {
		t.Fatalf("default shim = %q, want %q", got, DefaultShim)
	}
	const custom = "/usr/local/bin/gvisor-cellbox-dev/4320a3de3afb/containerd-shim-runsc-v1"
	if got := (&Backend{Shim: custom}).shim(); got != custom {
		t.Fatalf("configured shim = %q, want %q", got, custom)
	}
}
