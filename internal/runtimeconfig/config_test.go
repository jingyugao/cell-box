// SPDX-License-Identifier: Apache-2.0

package runtimeconfig

import (
	"strings"
	"testing"
)

func TestPreservesNodeConfigAndIsIdempotent(t *testing.T) {
	for _, version := range []string{"2", "3"} {
		input := []byte("# node-owned settings\nversion = " + version + "\nimports = [\"/etc/containerd/other/*.toml\"]\n[grpc]\naddress = '/custom/containerd.sock'\n[plugins.'vendor.plugin']\nsetting = 'preserve me'\n")
		config, err := Decode(input)
		if err != nil {
			t.Fatal(err)
		}
		result, err := Plan(config, input, "config", false)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(string(result), string(input)) {
			t.Fatal("installer rewrote existing node configuration")
		}
		parsed, err := Decode(result)
		if err != nil || !IsCellbox(Handler(parsed)) {
			t.Fatalf("invalid runtime handler: %v", err)
		}
		again, err := Plan(parsed, result, "config", false)
		if err != nil || string(again) != string(result) {
			t.Fatalf("repeated install changes config: %v", err)
		}
	}
}

func TestRefusesForeignHandlerAndMalformedMarkers(t *testing.T) {
	config, _ := Decode([]byte("version = 3\n"))
	for _, input := range []string{
		"version = 3\n[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runsc-recoverable]\nruntime_type = 'foreign'\n",
		"version = 3\n" + begin + "\n",
		"version = 3\n" + end + "\n" + begin,
	} {
		if _, err := Plan(config, []byte(input), "config", true); err == nil {
			t.Fatal("unsafe node configuration accepted")
		}
	}
	if _, err := Plan(map[string]any{"version": int64(4)}, nil, "config", false); err == nil {
		t.Fatal("unknown config version accepted")
	}
}

func TestK3sTemplatePreservesBaseAndLegacyDropin(t *testing.T) {
	config, _ := Decode([]byte("version = 3\n"))
	input := []byte("{{ template \"base\" . }}\n# existing custom setting\n[plugins.'other']\nvalue = 42\n")
	result, err := Plan(config, input, "template", false)
	if err != nil || !strings.HasPrefix(string(result), string(input)) {
		t.Fatalf("K3s template changed: %v", err)
	}
	again, err := Plan(config, result, "template", false)
	if err != nil || string(again) != string(result) {
		t.Fatalf("K3s template is not idempotent: %v", err)
	}
	legacy := []byte(runtimeBlock("io.containerd.cri.v1.runtime"))
	if _, err := Plan(config, legacy, "dropin", false); err == nil {
		t.Fatal("unowned legacy drop-in accepted")
	}
	result, err = Plan(config, legacy, "dropin", true)
	if err != nil || !strings.Contains(string(result), "version = 3") {
		t.Fatalf("owned legacy drop-in migration failed: %v", err)
	}
	if _, err := Plan(config, append(legacy, []byte("\n[plugins.'other']\nvalue = 42\n")...), "dropin", true); err == nil {
		t.Fatal("drop-in containing unrelated settings accepted")
	}
}
