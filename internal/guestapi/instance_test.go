package guestapi

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func encodedConfig(t *testing.T, env map[string]string) string {
	t.Helper()
	config := DefaultConfig()
	config.Env = env
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(data)
}

func TestInstanceIDFromArgs(t *testing.T) {
	marked := []string{"serve", "--config-base64", encodedConfig(t, map[string]string{InstanceIDEnv: "guest-1", "A": "b"})}
	if id, err := InstanceIDFromArgs(marked); err != nil || id != "guest-1" {
		t.Fatalf("marked: %q %v", id, err)
	}
	for name, args := range map[string][]string{
		"no configuration": {"serve"},
		"no marker":        {"serve", "--config-base64", encodedConfig(t, map[string]string{"A": "b"})},
	} {
		if id, err := InstanceIDFromArgs(args); err != nil || id != "" {
			t.Fatalf("%s: %q %v", name, id, err)
		}
	}
	for name, args := range map[string][]string{
		"missing value": {"serve", "--config-base64"},
		"bad base64":    {"serve", "--config-base64", "***"},
		"bad json":      {"serve", "--config-base64", base64.StdEncoding.EncodeToString([]byte("{"))},
	} {
		if _, err := InstanceIDFromArgs(args); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

// Stripping the marker must leave the original arguments untouched and keep
// every other setting, so only the marker differs between two Boxes' commands.
func TestWithoutInstanceID(t *testing.T) {
	original := []string{"serve", "--config-base64", encodedConfig(t, map[string]string{InstanceIDEnv: "guest-1", "A": "b"})}
	copyOfOriginal := append([]string(nil), original...)
	stripped := WithoutInstanceID(original)
	if original[2] != copyOfOriginal[2] {
		t.Fatal("input arguments were modified")
	}
	if id, _ := InstanceIDFromArgs(stripped); id != "" {
		t.Fatalf("marker survived: %q", id)
	}
	if want := encodedConfig(t, map[string]string{"A": "b"}); stripped[2] != want {
		t.Fatal("other settings changed while stripping the marker")
	}
	invalid := []string{"serve", "--config-base64", "***"}
	if got := WithoutInstanceID(invalid); got[2] != "***" {
		t.Fatal("undecodable configuration must pass through unchanged")
	}
}
