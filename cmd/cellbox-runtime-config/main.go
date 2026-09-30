// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"cellbox.local/cellbox/internal/runtimeconfig"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "active host containerd config (under --host)")
	target := flag.String("target", "", "file to update")
	output := flag.String("output", "", "candidate output path")
	kind := flag.String("kind", "config", "config, template, or dropin")
	host := flag.String("host", "/host", "host filesystem mount")
	adopt := flag.Bool("adopt", false, "adopt a legacy Cellbox-owned drop-in")
	versionOnly := flag.Bool("version-only", false, "print active config version")
	readyOnly := flag.Bool("ready-only", false, "print whether the loaded runtime handler needs no restart")
	flag.Parse()
	seen := map[string]bool{}
	ready := false
	var activeVersion int64
	handlers := 0
	var load func(string) (map[string]any, error)
	load = func(path string) (map[string]any, error) {
		if seen[path] {
			return nil, fmt.Errorf("repeated or cyclic containerd import: %s", path)
		}
		seen[path] = true
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		config, err := runtimeconfig.Decode(data)
		if err != nil {
			return nil, err
		}
		if path == *configPath {
			activeVersion, err = runtimeconfig.Version(config)
			if err != nil {
				return nil, err
			}
		}
		if handler := runtimeconfig.Handler(config); handler != nil && (!*adopt || !runtimeconfig.IsCellbox(handler)) {
			// Our own marked section is safe to reconcile on subsequent runs.
			if !runtimeconfig.IsCellbox(handler) || !containsManaged(data) {
				return nil, fmt.Errorf("unmanaged runsc-recoverable handler in %s", path)
			}
		}
		if runtimeconfig.Handler(config) != nil {
			handlers++
			if handlers > 1 {
				return nil, fmt.Errorf("multiple runsc-recoverable handlers in containerd config")
			}
			// Imported fragments may omit their version; inherit the root version.
			if _, ok := config["version"]; !ok {
				config["version"] = activeVersion
			}
			ready = runtimeconfig.Ready(config)
		}
		if disabled, ok := config["disabled_plugins"].([]any); ok {
			for _, item := range disabled {
				if item == "cri" || item == "io.containerd.grpc.v1.cri" || item == "io.containerd.cri.v1.runtime" {
					return nil, fmt.Errorf("CRI disabled in %s", path)
				}
			}
		}
		if imports, ok := config["imports"].([]any); ok {
			for _, item := range imports {
				pattern, ok := item.(string)
				if !ok {
					return nil, fmt.Errorf("invalid import in %s", path)
				}
				if filepath.IsAbs(pattern) {
					pattern = filepath.Join(*host, pattern)
				} else {
					pattern = filepath.Join(filepath.Dir(path), pattern)
				}
				paths, err := filepath.Glob(pattern)
				if err != nil {
					return nil, err
				}
				for _, imported := range paths {
					if _, err = load(imported); err != nil {
						return nil, err
					}
				}
			}
		}
		return config, nil
	}
	config, err := load(*configPath)
	if err != nil {
		return err
	}
	if *readyOnly {
		fmt.Println(ready)
		return nil
	}
	if *versionOnly {
		v, err := runtimeconfig.Version(config)
		if err == nil {
			fmt.Println(v)
		}
		return err
	}
	data, err := os.ReadFile(*target)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	result, err := runtimeconfig.Plan(config, data, *kind, *adopt)
	if err != nil {
		return err
	}
	return os.WriteFile(*output, result, 0600)
}

func containsManaged(data []byte) bool {
	return bytes.Contains(data, []byte("# BEGIN CELLBOX RUNTIME"))
}
