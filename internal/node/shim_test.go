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
	if !matchesShim(exe, cmd, id, socket) {
		t.Fatal("exact identity rejected")
	}
	for _, bad := range []struct{ exe, id, socket string }{{"/tmp/containerd-shim-runsc-v1", id, socket}, {exe, strings.Repeat("b", 64), socket}, {exe, id, "/run/other.sock"}} {
		if matchesShim(bad.exe, cmd, bad.id, bad.socket) {
			t.Fatal("foreign shim matched")
		}
	}
	if matchesShim(exe, []byte(strings.ReplaceAll(string(cmd), "k8s.io", "default")), id, socket) {
		t.Fatal("wrong namespace matched")
	}
}
