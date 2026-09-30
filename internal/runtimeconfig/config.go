// SPDX-License-Identifier: Apache-2.0

// Package runtimeconfig adds a managed runtime section without rewriting node settings.
package runtimeconfig

import (
	"fmt"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

const begin = "# BEGIN CELLBOX RUNTIME"
const end = "# END CELLBOX RUNTIME"

func Decode(data []byte) (map[string]any, error) {
	var config map[string]any
	err := toml.Unmarshal(data, &config)
	return config, err
}

func Version(config map[string]any) (int64, error) {
	v, _ := config["version"].(int64)
	if v != 2 && v != 3 {
		return 0, fmt.Errorf("unsupported containerd config version %d (expected 2 or 3)", v)
	}
	return v, nil
}

func Handler(config map[string]any) map[string]any {
	for _, plugin := range []string{"io.containerd.grpc.v1.cri", "io.containerd.cri.v1.runtime"} {
		value := config
		for _, key := range []string{"plugins", plugin, "containerd", "runtimes", "runsc-recoverable"} {
			value, _ = value[key].(map[string]any)
		}
		if value != nil {
			return value
		}
	}
	return nil
}

func IsCellbox(handler map[string]any) bool {
	options, _ := handler["options"].(map[string]any)
	return handler["runtime_type"] == "io.containerd.runsc.v1" && options["ConfigPath"] == "/var/lib/cellbox/runsc.toml"
}

func stripManaged(data string) (string, error) {
	starts, ends := strings.Count(data, begin), strings.Count(data, end)
	if starts == 0 && ends == 0 {
		return data, nil
	}
	if starts != 1 || ends != 1 {
		return "", fmt.Errorf("malformed Cellbox runtime markers")
	}
	i, j := strings.Index(data, begin), strings.Index(data, end)
	if j < i {
		return "", fmt.Errorf("reversed Cellbox runtime markers")
	}
	return data[:i] + data[j+len(end):], nil
}

// Plan preserves the original config/template and only replaces its managed block.
// adopt is reserved for a drop-in owned by an earlier Cellbox installer.
func Plan(config map[string]any, target []byte, kind string, adopt bool) ([]byte, error) {
	version, err := Version(config)
	if err != nil {
		return nil, err
	}
	plugin := "io.containerd.cri.v1.runtime"
	if version == 2 {
		plugin = "io.containerd.grpc.v1.cri"
	}
	clean, err := stripManaged(string(target))
	if err != nil {
		return nil, err
	}
	switch kind {
	case "template":
		if strings.Contains(clean, "runsc-recoverable") {
			return nil, fmt.Errorf("unmanaged runsc-recoverable handler in K3s template")
		}
		if strings.TrimSpace(clean) == "" {
			clean = "{{ template \"base\" . }}\n"
		}
	case "config", "dropin":
		parsed, err := Decode([]byte(clean))
		if err != nil {
			return nil, err
		}
		if handler := Handler(parsed); handler != nil {
			if kind != "dropin" || !adopt || !IsCellbox(handler) {
				return nil, fmt.Errorf("refusing to overwrite an unmanaged runtime handler")
			}
			// Earlier Cellbox drop-ins contained only this handler and no version.
			expected, _ := Decode([]byte(runtimeBlock(plugin)))
			actual, _ := toml.Marshal(parsed)
			want, _ := toml.Marshal(expected)
			if string(actual) != string(want) {
				return nil, fmt.Errorf("legacy drop-in contains additional settings; refusing adoption")
			}
			clean = ""
		}
		if kind == "dropin" && strings.TrimSpace(clean) == "" {
			clean = fmt.Sprintf("version = %d\n", version)
		}
	default:
		return nil, fmt.Errorf("unknown target kind %q", kind)
	}
	result := strings.TrimRight(clean, "\n") + "\n\n" + begin + "\n" + runtimeBlock(plugin) + end + "\n"
	if kind != "template" {
		if _, err := Decode([]byte(result)); err != nil {
			return nil, fmt.Errorf("invalid generated containerd config: %w", err)
		}
	}
	return []byte(result), nil
}

func runtimeBlock(plugin string) string {
	return fmt.Sprintf(`[plugins.%q.containerd.runtimes.runsc-recoverable]
  runtime_type = "io.containerd.runsc.v1"
  pod_annotations = ["dev.gvisor.internal.recovery.ticket"]
  [plugins.%q.containerd.runtimes.runsc-recoverable.options]
    TypeUrl = "io.containerd.runsc.v1.options"
    ConfigPath = "/var/lib/cellbox/runsc.toml"
`, plugin, plugin)
}
