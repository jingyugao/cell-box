// Package guestapi defines the private authenticated protocol to a Cellbox guest.
package guestapi

const Port = 40000
const Binary = "/opt/cellbox/bin/cellbox-guest"
const TokenPath = "/run/cellbox/control-token"

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
	Workspace string            `json:"workspace"`
	Agent     Identity          `json:"agent"`
	Debug     Identity          `json:"debug"`
	DebugHome string            `json:"debugHome,omitempty"`
	Command   []string          `json:"command,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	Tools     []Tool            `json:"tools,omitempty"`
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
