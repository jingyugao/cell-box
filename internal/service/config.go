package service

import (
	"encoding/json"
	"fmt"
	"github.com/jingyugao/cell-box/internal/guestapi"
	"github.com/jingyugao/cell-box/internal/image"
	"github.com/jingyugao/cell-box/internal/objectstorage"
	"io"
	"math"
	"path"
	"regexp"
	"strings"
)

var validName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$`)
var immutableImage = regexp.MustCompile(`^(sha256:[a-f0-9]{64}|[^\s@]+@sha256:[a-f0-9]{64})$`)

func LoadConfig(r io.Reader) (Config, error) {
	var c Config
	d := json.NewDecoder(io.LimitReader(r, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return c, fmt.Errorf("configuration must contain one JSON object")
	}
	return c, c.Validate()
}
func (c *Config) Validate() error {
	if err := c.VolumeBackups.Validate(); err != nil {
		return err
	}
	if c.OperationRetentionSeconds == 0 {
		c.OperationRetentionSeconds = 86400
	}
	if c.ExecutionRetentionSeconds == 0 {
		c.ExecutionRetentionSeconds = 3600
	}
	if c.OperationRetentionSeconds < 3600 || c.OperationRetentionSeconds > 30*86400 || c.ExecutionRetentionSeconds < 300 || c.ExecutionRetentionSeconds > c.OperationRetentionSeconds {
		return fmt.Errorf("operation retention must be 1 hour..30 days; execution retention must be 5 minutes..operation retention")
	}
	if c.ObjectStorage != (objectstorage.Config{}) {
		if err := c.ObjectStorage.Validate(); err != nil {
			return err
		}
	}
	if c.Listen == "" {
		c.Listen = "127.0.0.1:8090"
	}
	if c.DataDir == "" {
		c.DataDir = "data"
	}
	if strings.ContainsAny(c.DataDir, "\x00\r\n") || path.Clean(c.DataDir) != c.DataDir || c.DataDir == "." || c.DataDir == "/" {
		return fmt.Errorf("dataDir must be a clean directory path other than . or /")
	}
	if c.StartupTimeoutSeconds == 0 {
		c.StartupTimeoutSeconds = 120
	}
	if c.StartupTimeoutSeconds < 1 || c.StartupTimeoutSeconds > 600 {
		return fmt.Errorf("startupTimeoutSeconds must be 1..600")
	}
	if c.ImageBuild != (ImageBuildConfig{}) {
		if !strings.HasPrefix(c.ImageBuild.Address, "unix:///") || !image.ValidRepository(c.ImageBuild.Repository) || c.ImageBuild.GuestBinary == "" {
			return fmt.Errorf("imageBuild requires a Unix BuildKit address, registry repository, and guest binary")
		}
	}
	if c.ClientID == "" {
		c.ClientID = "internal"
	}
	if !validName.MatchString(c.ClientID) {
		return fmt.Errorf("invalid client namespace")
	}
	profiles := map[string]bool{}
	var node, namespace string
	for i := range c.Profiles {
		p := &c.Profiles[i]
		if !validName.MatchString(p.ID) || profiles[p.ID] {
			return fmt.Errorf("invalid or duplicate profile ID")
		}
		profiles[p.ID] = true
		if p.Provider != "docker" && p.Provider != "resumable-k8s-pod" && p.Provider != "local-runsc" {
			return fmt.Errorf("profile %s has unknown provider", p.ID)
		}
		// New resumable Boxes always persist HOME. Stored immutable profiles are
		// decoded separately, so pre-upgrade Boxes retain their original layout.
		p.PersistentHome = p.Provider == "resumable-k8s-pod" || p.Provider == "local-runsc"
		if !immutableImage.MatchString(p.Image) || ((p.Provider == "resumable-k8s-pod" || p.Provider == "local-runsc") && !strings.Contains(p.Image, "@sha256:")) {
			return fmt.Errorf("profile image must be an immutable SHA256 reference; resumable and local-runsc providers require repository@sha256:digest")
		}
		for _, trusted := range p.TrustedToolImages {
			if !immutableImage.MatchString(trusted) || !strings.Contains(trusted, "@sha256:") {
				return fmt.Errorf("trustedToolImages must contain immutable repository@sha256:digest references")
			}
		}
		for _, mounted := range p.MountedToolRuntimeImages {
			if !immutableImage.MatchString(mounted) || !strings.Contains(mounted, "@sha256:") {
				return fmt.Errorf("mountedToolRuntimeImages must contain immutable repository@sha256:digest references")
			}
		}
		for _, home := range p.DebugHomeImages {
			if !immutableImage.MatchString(home) || !strings.Contains(home, "@sha256:") {
				return fmt.Errorf("debugHomeImages must contain immutable repository@sha256:digest references")
			}
		}
		if (p.Provider == "docker" || p.Provider == "local-runsc") && (p.Namespace != "" || p.NodeName != "") {
			return fmt.Errorf("%s profiles do not accept namespace or nodeName", p.Provider)
		}
		if math.IsNaN(p.CPU) || math.IsInf(p.CPU, 0) || p.CPU < 0 || p.MemoryMiB < 0 {
			return fmt.Errorf("profile resource limits must be finite and nonnegative")
		}
		if p.Provider == "local-runsc" && ((p.CPU > 0 && p.CPU < 0.01) || p.CPU > float64(math.MaxInt64)/100000 || p.MemoryMiB > math.MaxInt64/(1<<20)) {
			return fmt.Errorf("local-runsc CPU limit must be zero or at least 0.01 cores; resource limits must fit cgroup counters")
		}
		if p.Provider == "resumable-k8s-pod" && (p.Namespace == "" || p.NodeName == "") {
			return fmt.Errorf("resumable profile requires namespace and nodeName")
		}
		if p.Provider == "resumable-k8s-pod" {
			if node != "" && (p.NodeName != node || p.Namespace != namespace) {
				return fmt.Errorf("one Cellbox instance manages one node and namespace; profiles must share that location")
			}
			node, namespace = p.NodeName, p.Namespace
		}
		if p.SharedReadOnlyHostPath != "" {
			if (p.Provider != "resumable-k8s-pod" && p.Provider != "local-runsc") || !path.IsAbs(p.SharedReadOnlyHostPath) ||
				path.Clean(p.SharedReadOnlyHostPath) != p.SharedReadOnlyHostPath || p.SharedReadOnlyHostPath == "/" ||
				len(p.SharedReadOnlyHostPath) > 4096 || strings.ContainsAny(p.SharedReadOnlyHostPath, "\x00\r\n") {
				return fmt.Errorf("profile %s sharedReadOnlyHostPath requires a resumable Kubernetes or local-runsc provider and a valid absolute path", p.ID)
			}
		}
		if p.DebugReadOnlyHostPath != "" {
			if (p.Provider != "resumable-k8s-pod" && p.Provider != "local-runsc") || !path.IsAbs(p.DebugReadOnlyHostPath) ||
				path.Clean(p.DebugReadOnlyHostPath) != p.DebugReadOnlyHostPath ||
				p.DebugReadOnlyHostPath == "/" || len(p.DebugReadOnlyHostPath) > 4096 ||
				strings.ContainsAny(p.DebugReadOnlyHostPath, "\x00\r\n") {
				return fmt.Errorf("profile %s debugReadOnlyHostPath requires a resumable Kubernetes or local-runsc provider and a valid absolute path", p.ID)
			}
		}
		if p.DebugReadWriteHostPath != "" {
			if (p.Provider != "resumable-k8s-pod" && p.Provider != "local-runsc") || p.DebugReadOnlyHostPath != "" || !path.IsAbs(p.DebugReadWriteHostPath) ||
				path.Clean(p.DebugReadWriteHostPath) != p.DebugReadWriteHostPath ||
				p.DebugReadWriteHostPath == "/" || len(p.DebugReadWriteHostPath) > 4096 ||
				strings.ContainsAny(p.DebugReadWriteHostPath, "\x00\r\n") {
				return fmt.Errorf("profile %s debugReadWriteHostPath requires a resumable Kubernetes or local-runsc provider and a valid absolute path", p.ID)
			}
		}
		if p.Guest.DebugHome != "" {
			return fmt.Errorf("profile %s debugHome is managed by Cellbox", p.ID)
		}
		if p.Guest.Workspace == "" {
			p.Guest.Workspace = "/workspace"
			if p.PersistentHome {
				p.Guest.Workspace = "/home/agent/workspace"
			}
		}
		defaults := guestapi.DefaultConfig()
		if p.Guest.Agent == (guestapi.Identity{}) && p.Guest.Debug == (guestapi.Identity{}) {
			p.Guest.Agent = defaults.Agent
			p.Guest.Debug = defaults.Debug
		}
		if !path.IsAbs(p.Guest.Workspace) || path.Clean(p.Guest.Workspace) != p.Guest.Workspace || p.Guest.Workspace == "/" {
			return fmt.Errorf("workspace must be a clean absolute directory")
		}
		if p.Guest.Agent.UID == 0 || p.Guest.Agent.GID == 0 || (p.Guest.Debug.UID == 0) != (p.Guest.Debug.GID == 0) || p.Guest.Agent.UID == p.Guest.Debug.UID || p.Guest.Agent.GID == p.Guest.Debug.GID {
			return fmt.Errorf("agent must be non-root and distinct from debug; root debug requires UID/GID zero")
		}

	}
	return nil
}
