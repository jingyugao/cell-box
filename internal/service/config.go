package service

import (
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/image"
	"cellbox.local/cellbox/internal/objectstorage"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
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
	for i := range c.Clients {
		c.Clients[i].Token = os.Getenv(c.Clients[i].TokenEnv)
	}
	return c, c.Validate()
}
func (c *Config) Validate() error {
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
	if c.PublicURL != "" {
		u, e := url.Parse(c.PublicURL)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") != "" {
			return fmt.Errorf("publicUrl must be an HTTP(S) origin")
		}
	}
	if c.ServiceDomain != "" {
		if strings.ContainsAny(c.ServiceDomain, "/: @\\") || !strings.Contains(c.ServiceDomain, ".") || strings.ToLower(c.ServiceDomain) != c.ServiceDomain {
			return fmt.Errorf("serviceDomain must be a lowercase DNS name")
		}
	}
	clients := map[string]bool{}
	tokens := map[string]bool{}
	if len(c.Clients) == 0 {
		return fmt.Errorf("at least one authenticated client is required")
	}
	for _, cl := range c.Clients {
		if !validName.MatchString(cl.ID) || clients[cl.ID] {
			return fmt.Errorf("invalid or duplicate client ID")
		}
		if len(cl.Token) < 32 || tokens[cl.Token] {
			return fmt.Errorf("client tokens must be distinct and at least 32 bytes (check tokenEnv)")
		}
		clients[cl.ID] = true
		tokens[cl.Token] = true
		if cl.AuthorizeURL != "" {
			u, e := url.Parse(cl.AuthorizeURL)
			if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" {
				return fmt.Errorf("authorizeUrl must be an HTTPS URL")
			}
		}
	}
	profiles := map[string]bool{}
	for i := range c.Profiles {
		p := &c.Profiles[i]
		if !validName.MatchString(p.ID) || profiles[p.ID] {
			return fmt.Errorf("invalid or duplicate profile ID")
		}
		profiles[p.ID] = true
		if p.Provider != "docker" && p.Provider != "resumable-k8s-pod" {
			return fmt.Errorf("profile %s has unknown provider", p.ID)
		}
		if !immutableImage.MatchString(p.Image) || (p.Provider == "resumable-k8s-pod" && !strings.Contains(p.Image, "@sha256:")) {
			return fmt.Errorf("profile image must be an immutable SHA256 reference; Kubernetes requires repository@sha256:digest")
		}
		if p.CPU == 0 {
			p.CPU = 1
		}
		if p.MemoryMiB == 0 {
			p.MemoryMiB = 512
		}
		if p.Provider == "docker" && (p.Namespace != "" || p.NodeName != "") {
			return fmt.Errorf("Docker profiles do not accept namespace or nodeName")
		}
		if p.CPU < 0 || p.CPU > 1024 || p.MemoryMiB < 0 || p.MemoryMiB > 1048576 {
			return fmt.Errorf("invalid profile resource limits")
		}
		if p.Provider == "resumable-k8s-pod" && (p.Namespace == "" || p.NodeName == "") {
			return fmt.Errorf("resumable profile requires namespace and nodeName")
		}
		if p.SharedReadOnlyHostPath != "" {
			if p.Provider != "resumable-k8s-pod" || !path.IsAbs(p.SharedReadOnlyHostPath) ||
				path.Clean(p.SharedReadOnlyHostPath) != p.SharedReadOnlyHostPath || p.SharedReadOnlyHostPath == "/" ||
				len(p.SharedReadOnlyHostPath) > 4096 || strings.ContainsAny(p.SharedReadOnlyHostPath, "\x00\r\n") {
				return fmt.Errorf("profile %s has invalid sharedReadOnlyHostPath", p.ID)
			}
		}
		if p.DebugReadOnlyHostPath != "" {
			if p.Provider != "resumable-k8s-pod" || !path.IsAbs(p.DebugReadOnlyHostPath) ||
				path.Clean(p.DebugReadOnlyHostPath) != p.DebugReadOnlyHostPath ||
				p.DebugReadOnlyHostPath == "/" || len(p.DebugReadOnlyHostPath) > 4096 ||
				strings.ContainsAny(p.DebugReadOnlyHostPath, "\x00\r\n") {
				return fmt.Errorf("profile %s has invalid debugReadOnlyHostPath", p.ID)
			}
		}
		if p.DebugReadWriteHostPath != "" {
			if p.Provider != "resumable-k8s-pod" || p.DebugReadOnlyHostPath != "" || !path.IsAbs(p.DebugReadWriteHostPath) ||
				path.Clean(p.DebugReadWriteHostPath) != p.DebugReadWriteHostPath ||
				p.DebugReadWriteHostPath == "/" || len(p.DebugReadWriteHostPath) > 4096 ||
				strings.ContainsAny(p.DebugReadWriteHostPath, "\x00\r\n") {
				return fmt.Errorf("profile %s has invalid debugReadWriteHostPath", p.ID)
			}
		}
		if p.Guest.DebugHome != "" {
			return fmt.Errorf("profile %s debugHome is managed by Cellbox", p.ID)
		}
		if p.Guest.Workspace == "" {
			p.Guest.Workspace = "/workspace"
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
		if len(p.Clients) == 0 {
			return fmt.Errorf("profile %s requires explicit clients", p.ID)
		}
		for _, id := range p.Clients {
			if !clients[id] {
				return fmt.Errorf("profile %s references unknown client", p.ID)
			}
		}
	}
	return nil
}
