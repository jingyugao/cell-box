// Package guestapi defines the internal protocol to a Cellbox guest.
package guestapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
)

const Port = 40000
const Binary = "/opt/cellbox/bin/cellbox-container-agent"
const MaxCredentialBytes = 1 << 20
const MaxCredentialBatchBytes = 2 << 20

type CredentialBatch struct {
	Slots map[string][]byte `json:"slots"`
}

var credentialSlotPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)

func (b CredentialBatch) Validate() error {
	if len(b.Slots) == 0 || len(b.Slots) > 32 {
		return fmt.Errorf("credential batch requires 1..32 slots")
	}
	total := 0
	for slot, data := range b.Slots {
		if !credentialSlotPattern.MatchString(slot) || len(data) == 0 || len(data) > MaxCredentialBytes {
			return fmt.Errorf("invalid credential slot or size")
		}
		total += len(data)
	}
	if total > MaxCredentialBatchBytes {
		return fmt.Errorf("credential batch exceeds 2 MiB")
	}
	return nil
}

type Identity struct {
	UID uint32 `json:"uid"`
	GID uint32 `json:"gid"`
}

type Tool struct {
	ID         string   `json:"id"`
	Executable string   `json:"executable"`
	Args       []string `json:"args"`
	// Each supplied argument must match its corresponding full regular expression.
	InputPatterns []string `json:"inputPatterns,omitempty"`
	// PassThroughArgs accepts a variable number of caller arguments without
	// patterns. It cannot be combined with InputPatterns.
	PassThroughArgs bool `json:"passThroughArgs,omitempty"`
	// Values are private credential slot names, not caller-selected paths.
	CredentialEnv map[string]string `json:"credentialEnv,omitempty"`
	WorkspaceRead bool              `json:"workspaceRead,omitempty"`
	// WorkspaceWrite admits a debug tool to write the agent workspace. The
	// supervisor repairs ownership before the tool result is returned.
	WorkspaceWrite bool `json:"workspaceWrite,omitempty"`
}

type Config struct {
	Workspace  string            `json:"workspace"`
	Agent      Identity          `json:"agent"`
	Debug      Identity          `json:"debug"`
	DebugHome  string            `json:"debugHome,omitempty"`
	Command    []string          `json:"command,omitempty"`
	CommandDir string            `json:"commandDir,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	Tools      []Tool            `json:"tools,omitempty"`
}

// Decode defaults before applying supplied fields so an explicit debug UID/GID
// of zero remains root, while an omitted debug identity remains non-root.
func (c *Config) UnmarshalJSON(data []byte) error {
	type plain Config
	next := plain(DefaultConfig())
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&next); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("guest configuration must contain one JSON object")
	}
	*c = Config(next)
	return nil
}

func DefaultConfig() Config {
	return Config{Workspace: "/workspace", Agent: Identity{UID: 11000, GID: 11000}, Debug: Identity{UID: 11001, GID: 11001}}
}

type ExecRequest struct {
	Argv      []string          `json:"argv"`
	Cwd       string            `json:"cwd,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	TimeoutMS int64             `json:"timeoutMs,omitempty"`
}

type ExecResult struct {
	Stdout    string `json:"stdout"`
	Stderr    string `json:"stderr"`
	ExitCode  int    `json:"exitCode"`
	Truncated bool   `json:"truncated,omitempty"`
}

type FileEntry struct {
	Name      string `json:"name"`
	Directory bool   `json:"directory"`
	Size      int64  `json:"size"`
}

type ToolRequest struct {
	Args      []string `json:"args,omitempty"`
	TimeoutMS int64    `json:"timeoutMs,omitempty"`
}
