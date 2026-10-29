package guestapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// InstanceIDFromArgs reads the public routing marker from an admitted Guest
// command. Older commands without Guest configuration have no marker.
func InstanceIDFromArgs(args []string) (string, error) {
	for i, arg := range args {
		if arg != "--config-base64" {
			continue
		}
		if i+1 >= len(args) {
			return "", fmt.Errorf("missing Guest configuration")
		}
		config, err := decodeInstanceConfig(args[i+1])
		if err != nil {
			return "", err
		}
		return config.Env[InstanceIDEnv], nil
	}
	return "", nil
}

// WithoutInstanceID keeps warm runtimes interchangeable between Boxes. The
// ordinary checkpoint/spec fingerprint still includes the routing marker.
func WithoutInstanceID(args []string) []string {
	for i, arg := range args {
		if arg != "--config-base64" || i+1 >= len(args) {
			continue
		}
		config, err := decodeInstanceConfig(args[i+1])
		if err != nil || config.Env[InstanceIDEnv] == "" {
			return args
		}
		delete(config.Env, InstanceIDEnv)
		encoded, err := json.Marshal(config)
		if err != nil {
			return args
		}
		result := append([]string(nil), args...)
		result[i+1] = base64.StdEncoding.EncodeToString(encoded)
		return result
	}
	return args
}

func decodeInstanceConfig(encoded string) (Config, error) {
	var config Config
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(data) > 1<<20 {
		return config, fmt.Errorf("invalid Guest configuration encoding")
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return config, fmt.Errorf("invalid Guest configuration")
	}
	return config, nil
}
