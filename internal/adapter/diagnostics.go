package adapter

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// DiagnosticArgs gives every OCI invocation its own append-only runtime logs.
// It does not depend on the shim expanding placeholders in its log_path.
func (a Adapter) DiagnosticArgs(args []string) ([]string, error) {
	if len(args) < 2 || !sandboxID.MatchString(args[len(args)-1]) {
		return args, nil // Version/help and internal commands have no OCI identity.
	}
	dir := filepath.Join(a.Base, "logs", args[len(args)-1])
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create runtime log directory: %w", err)
	}
	// These global options must precede the subcommand. Timestamped paths are
	// expanded by runsc itself, including the long-lived sentry's panic output.
	prefix := []string{
		"--debug-log=" + filepath.Join(dir, "runsc.%TIMESTAMP%.%COMMAND%.log"),
		"--panic-log=" + filepath.Join(dir, "panic.%TIMESTAMP%.%COMMAND%.log"),
	}
	// Keep caller-supplied runtime settings, but prevent later flags overriding
	// the persistent destinations. No syscall/packet tracing is enabled.
	global := true
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "create", "start", "restore", "delete", "wait", "state", "kill", "events", "checkpoint", "exec":
			global = false
		}
		if global && (args[i] == "--debug-log" || args[i] == "--panic-log") {
			i++
			continue
		}
		if global && (strings.HasPrefix(args[i], "--debug-log=") || strings.HasPrefix(args[i], "--panic-log=")) {
			continue
		}
		prefix = append(prefix, args[i])
	}
	return prefix, nil
}
