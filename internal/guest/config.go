package guest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"cellbox.local/cellbox/internal/guestapi"
)

const (
	toolRoot   = "/opt/cellbox/tools"
	debugRoot  = "/var/lib/cellbox/debug"
	toolSocket = "/run/cellbox/tool.sock"
)

var namePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)

type compiledTool struct {
	spec     guestapi.Tool
	patterns []*regexp.Regexp
}

func validateConfig(c guestapi.Config) (map[string]compiledTool, error) {
	if c.DebugHome != "" && c.DebugHome != "/home/debug" {
		return nil, errors.New("debugHome must be the admitted host mount")
	}
	if !filepath.IsAbs(c.Workspace) || filepath.Clean(c.Workspace) != c.Workspace || c.Workspace == "/" {
		return nil, errors.New("workspace must be a clean absolute path")
	}
	for _, private := range []string{debugRoot, "/home/debug", "/run/cellbox", toolRoot, "/etc", "/home/agent"} {
		if private == "/home/agent" && c.Workspace == "/home/agent/workspace" {
			continue
		}
		if c.Workspace == private || strings.HasPrefix(c.Workspace, private+"/") || strings.HasPrefix(private, c.Workspace+"/") {
			return nil, fmt.Errorf("workspace overlaps protected path %s", private)
		}
	}
	if c.Agent.UID == 0 || c.Agent.GID == 0 || (c.Debug.UID == 0) != (c.Debug.GID == 0) || c.Agent.UID == c.Debug.UID || c.Agent.GID == c.Debug.GID {
		return nil, errors.New("agent identity must be non-root and distinct from debug identity")
	}
	if len(c.Command) > 0 && (!filepath.IsAbs(c.Command[0]) || strings.ContainsRune(c.Command[0], 0)) {
		return nil, errors.New("command executable must be absolute")
	}
	for k, v := range c.Env {
		if !validEnv(k, v) {
			return nil, fmt.Errorf("invalid environment variable %q", k)
		}
	}
	out := make(map[string]compiledTool, len(c.Tools))
	for _, t := range c.Tools {
		if t.WorkspaceWrite && !t.WorkspaceRead {
			return nil, fmt.Errorf("tool %q workspaceWrite requires workspaceRead", t.ID)
		}
		if !namePattern.MatchString(t.ID) {
			return nil, fmt.Errorf("invalid tool ID %q", t.ID)
		}
		if _, ok := out[t.ID]; ok {
			return nil, fmt.Errorf("duplicate tool ID %q", t.ID)
		}
		if !strings.HasPrefix(t.Executable, toolRoot+"/") || filepath.Clean(t.Executable) != t.Executable {
			return nil, fmt.Errorf("tool %q executable must be under %s", t.ID, toolRoot)
		}
		if len(t.InputPatterns) > 32 {
			return nil, fmt.Errorf("too many arguments for tool %q", t.ID)
		}
		ct := compiledTool{spec: t}
		for _, p := range t.InputPatterns {
			if len(p) > 512 {
				return nil, fmt.Errorf("tool %q input pattern too large", t.ID)
			}
			re, err := regexp.Compile("^(?:" + p + ")$")
			if err != nil {
				return nil, fmt.Errorf("tool %q pattern: %w", t.ID, err)
			}
			ct.patterns = append(ct.patterns, re)
		}
		for k, slot := range t.CredentialEnv {
			if !validEnv(k, "x") || !namePattern.MatchString(slot) {
				return nil, fmt.Errorf("tool %q invalid credential mapping", t.ID)
			}
		}
		for _, a := range t.Args {
			if strings.ContainsRune(a, 0) {
				return nil, fmt.Errorf("tool %q contains NUL argument", t.ID)
			}
		}
		out[t.ID] = ct
	}
	return out, nil
}

func validEnv(k, v string) bool {
	return namePattern.MatchString(k) && !strings.ContainsRune(v, 0) && len(v) <= 8192
}

// checkImmutable verifies every component of a configured launcher path. Symlinks
// or writable directories would let an untrusted process replace a fixed tool.
func checkImmutable(path string, executable bool) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("path is not clean and absolute")
	}
	cur := "/"
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if err != nil {
			return err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in immutable path %s", cur)
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok || st.Uid != 0 {
			return fmt.Errorf("immutable path is not root-owned: %s", cur)
		}
		if fi.Mode().Perm()&022 != 0 {
			return fmt.Errorf("writable immutable path %s", cur)
		}
	}
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if executable && (!fi.Mode().IsRegular() || fi.Mode().Perm()&0111 == 0) {
		return fmt.Errorf("tool is not executable: %s", path)
	}
	return nil
}
