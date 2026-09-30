// Package docker implements a local Docker Cellbox runtime.
package docker

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/image"
)

const (
	ownerLabel = "cellbox.managed-container"
	boxLabel   = "cellbox.box-id"
	specLabel  = "cellbox.spec-key"
	ownerValue = "1"
)

var boxIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
var imageIDPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var tokenPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)

type Runner interface {
	Run(context.Context, []string) ([]byte, error)
}
type execRunner struct{ binary string }

func (r execRunner) Run(ctx context.Context, args []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, r.binary, args...)
	b, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("docker %s: %w: %s", args[0], err, strings.TrimSpace(string(b)))
	}
	return b, nil
}

type Provider struct{ runner Runner }

func (p *Provider) run(ctx context.Context, args []string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return p.runner.Run(ctx, args)
}

func New(binary string) *Provider {
	if binary == "" {
		binary = "docker"
	}
	return &Provider{runner: execRunner{binary}}
}
func NewWithRunner(r Runner) *Provider { return &Provider{runner: r} }
func (*Provider) Name() string         { return "docker" }

type imageInfo struct {
	ID     string `json:"Id"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
}
type container struct {
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	Image  string `json:"Image"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State struct {
		Status    string `json:"Status"`
		Paused    bool   `json:"Paused"`
		ExitCode  int    `json:"ExitCode"`
		StartedAt string `json:"StartedAt"`
	} `json:"State"`
	NetworkSettings struct {
		Ports map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"Ports"`
	} `json:"NetworkSettings"`
}

func (p *Provider) image(ctx context.Context, ref string) (imageInfo, error) {
	var infos []imageInfo
	b, err := p.run(ctx, []string{"image", "inspect", ref})
	if err != nil {
		return imageInfo{}, err
	}
	if err = json.Unmarshal(b, &infos); err != nil || len(infos) != 1 || !imageIDPattern.MatchString(infos[0].ID) {
		return imageInfo{}, errors.New("invalid image inspect result")
	}
	return infos[0], nil
}
func (p *Provider) container(ctx context.Context, name string) (container, error) {
	var infos []container
	b, err := p.run(ctx, []string{"container", "inspect", name})
	if err != nil {
		return container{}, err
	}
	if err = json.Unmarshal(b, &infos); err != nil || len(infos) != 1 || infos[0].ID == "" {
		return container{}, errors.New("invalid container inspect result")
	}
	return infos[0], nil
}
func missing(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "No such container") || strings.Contains(err.Error(), "No such object"))
}
func validateSpec(s boxprovider.Spec) error {
	if !boxIDPattern.MatchString(s.BoxID) {
		return errors.New("invalid box ID")
	}
	if s.Image == "" || strings.HasPrefix(s.Image, "-") {
		return errors.New("image required")
	}
	if math.IsNaN(s.CPU) || math.IsInf(s.CPU, 0) || s.CPU <= 0 || s.CPU > 1024 {
		return errors.New("CPU must be in (0, 1024]")
	}
	if s.MemoryMiB < 64 || s.MemoryMiB > 1048576 {
		return errors.New("memory must be 64 through 1048576 MiB")
	}
	if s.NodeName != "" || s.Namespace != "" {
		return errors.New("node and namespace are unsupported by Docker")
	}
	return nil
}
func nameFor(id string) string { return "cellbox-" + id }
func configArg(c guestapi.Config) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}
func fingerprint(s boxprovider.Spec, imageID string) (string, error) {
	// JSON encoding sorts map keys and gives retries a stable identity.
	b, err := json.Marshal(struct {
		BoxID, ImageID string
		Config         guestapi.Config
		CPU            float64
		MemoryMiB      int64
		Staged         bool
	}{s.BoxID, imageID, s.Config, s.CPU, s.MemoryMiB, s.Staged})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
func verify(c container, boxID, specKey string) error {
	if c.Name != "/"+nameFor(boxID) || c.Config.Labels[ownerLabel] != ownerValue || c.Config.Labels[boxLabel] != boxID {
		return errors.New("container name collision or ownership mismatch")
	}
	if specKey != "" && c.Config.Labels[specLabel] != specKey {
		return errors.New("existing container has different immutable specification")
	}
	return nil
}
func (p *Provider) Create(ctx context.Context, s boxprovider.Spec) (boxprovider.Handle, error) {
	if err := validateSpec(s); err != nil {
		return boxprovider.Handle{}, err
	}
	img, err := p.image(ctx, s.Image)
	if err != nil {
		return boxprovider.Handle{}, fmt.Errorf("inspect prepared image: %w", err)
	}
	if img.Config.Labels[image.ManagedLabel] != image.ImageVersion || img.Config.Labels[image.IdentityLabel] == "" {
		return boxprovider.Handle{}, errors.New("image is not Cellbox prepared")
	}
	key, err := fingerprint(s, img.ID)
	if err != nil {
		return boxprovider.Handle{}, err
	}
	name := nameFor(s.BoxID)
	if c, err := p.container(ctx, name); err == nil {
		if err = verify(c, s.BoxID, key); err != nil {
			return boxprovider.Handle{}, err
		}
		if c.Image != img.ID {
			return boxprovider.Handle{}, errors.New("existing container image mismatch")
		}
		if c.State.Status == "created" {
			if _, err = p.run(ctx, []string{"start", c.ID}); err != nil {
				return boxprovider.Handle{}, err
			}
		}
		return boxprovider.Handle{Provider: p.Name(), ID: c.ID, Name: name, ImageID: img.ID}, nil
	} else if !missing(err) {
		return boxprovider.Handle{}, err
	}
	encoded, err := configArg(s.Config)
	if err != nil {
		return boxprovider.Handle{}, err
	}
	args := []string{"create", "--name", name, "--label", ownerLabel + "=" + ownerValue, "--label", boxLabel + "=" + s.BoxID, "--label", specLabel + "=" + key, "--cpus", strconv.FormatFloat(s.CPU, 'f', -1, 64), "--memory", strconv.FormatInt(s.MemoryMiB, 10) + "m", "--user", "0:0", "--cap-drop", "ALL", "--cap-add", "CHOWN", "--cap-add", "FOWNER", "--cap-add", "DAC_OVERRIDE", "--cap-add", "SETUID", "--cap-add", "SETGID", "--security-opt", "no-new-privileges", "--publish", fmt.Sprintf("127.0.0.1::%d", guestapi.Port), "--entrypoint", guestapi.Binary, img.ID, "serve", "--config-base64", encoded}
	if s.Staged {
		args = append(args, "--staged")
	}
	if _, err = p.run(ctx, args); err != nil {
		// A concurrent retry may have won creation. Inspect and accept only exact ownership.
		if c, e := p.container(ctx, name); e == nil && verify(c, s.BoxID, key) == nil && c.Image == img.ID {
			if c.State.Status == "created" {
				if _, e = p.run(ctx, []string{"start", c.ID}); e != nil {
					return boxprovider.Handle{}, e
				}
			}
			return boxprovider.Handle{Provider: p.Name(), ID: c.ID, Name: name, ImageID: img.ID}, nil
		}
		return boxprovider.Handle{}, err
	}
	c, err := p.container(ctx, name)
	if err != nil {
		return boxprovider.Handle{}, err
	}
	if err = verify(c, s.BoxID, key); err != nil {
		return boxprovider.Handle{}, err
	}
	if c.Image != img.ID {
		return boxprovider.Handle{}, errors.New("created container image mismatch")
	}
	if _, err = p.run(ctx, []string{"start", c.ID}); err != nil {
		return boxprovider.Handle{}, err
	}
	return boxprovider.Handle{Provider: p.Name(), ID: c.ID, Name: name, ImageID: img.ID}, nil
}
func (p *Provider) owned(ctx context.Context, h boxprovider.Handle) (container, error) {
	if h.Provider != "docker" || !strings.HasPrefix(h.Name, "cellbox-") || !boxIDPattern.MatchString(strings.TrimPrefix(h.Name, "cellbox-")) || h.ID == "" {
		return container{}, errors.New("invalid Docker handle")
	}
	c, err := p.container(ctx, h.Name)
	if missing(err) {
		return container{}, boxprovider.ErrNotFound
	}
	if err != nil {
		return container{}, err
	}
	if err = verify(c, strings.TrimPrefix(h.Name, "cellbox-"), ""); err != nil {
		return container{}, err
	}
	if c.ID != h.ID || h.ImageID != "" && c.Image != h.ImageID {
		return container{}, errors.New("Docker handle identity mismatch")
	}
	return c, nil
}
func (p *Provider) Inspect(ctx context.Context, h boxprovider.Handle) (boxprovider.Observation, error) {
	c, err := p.owned(ctx, h)
	if errors.Is(err, boxprovider.ErrNotFound) {
		return boxprovider.Observation{Phase: "deleted"}, nil
	}
	if err != nil {
		return boxprovider.Observation{}, err
	}
	phase := "failed"
	switch c.State.Status {
	case "running":
		if c.State.Paused {
			phase = "frozen"
		} else {
			phase = "running"
		}
	case "paused":
		phase = "frozen"
	case "created", "restarting":
		phase = "creating"
	case "exited", "dead":
		phase = "failed"
	}
	executionID, err := executionIdentity(c)
	if err != nil {
		return boxprovider.Observation{}, err
	}
	return boxprovider.Observation{Phase: phase, ExecutionID: executionID}, nil
}
func executionIdentity(c container) (string, error) {
	if c.State.StartedAt == "" {
		if c.State.Status == "running" || c.State.Status == "paused" {
			return "", errors.New("running container lacks start identity")
		}
		return "", nil
	}
	started, err := time.Parse(time.RFC3339Nano, c.State.StartedAt)
	if err != nil {
		return "", errors.New("invalid container start identity")
	}
	if started.IsZero() {
		if c.State.Status == "running" || c.State.Status == "paused" {
			return "", errors.New("running container has zero start identity")
		}
		return "", nil
	}
	return c.ID + "/" + started.UTC().Format(time.RFC3339Nano), nil
}
func (p *Provider) Action(ctx context.Context, h boxprovider.Handle, action string) error {
	if action == "suspend" || action == "resume" {
		return boxprovider.ErrUnsupported
	}
	if action != "freeze" && action != "unfreeze" {
		return boxprovider.ErrUnsupported
	}
	c, err := p.owned(ctx, h)
	if err != nil {
		return err
	}
	if action == "freeze" {
		if c.State.Paused {
			return nil
		}
		if c.State.Status != "running" {
			return errors.New("container is not running")
		}
		_, err = p.run(ctx, []string{"pause", c.ID})
		return err
	}
	if !c.State.Paused {
		return nil
	}
	_, err = p.run(ctx, []string{"unpause", c.ID})
	return err
}
func (p *Provider) Destroy(ctx context.Context, h boxprovider.Handle) error {
	c, err := p.owned(ctx, h)
	if errors.Is(err, boxprovider.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = p.run(ctx, []string{"rm", "--force", c.ID})
	if missing(err) {
		return nil
	}
	return err
}
func (p *Provider) Guest(ctx context.Context, h boxprovider.Handle) (boxprovider.Connection, error) {
	c, err := p.owned(ctx, h)
	if err != nil {
		return boxprovider.Connection{}, err
	}
	if c.State.Status != "running" || c.State.Paused {
		return boxprovider.Connection{}, errors.New("guest container is not running")
	}
	bindings := c.NetworkSettings.Ports[strconv.Itoa(guestapi.Port)+"/tcp"]
	if len(bindings) != 1 || bindings[0].HostIP != "127.0.0.1" {
		return boxprovider.Connection{}, errors.New("guest port lacks a unique IPv4 loopback binding")
	}
	port, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil || port < 1 || port > 65535 {
		return boxprovider.Connection{}, errors.New("invalid guest host port")
	}
	// Token is returned to the caller only. Never include command output in errors.
	out, err := p.run(ctx, []string{"exec", "--user", "0", c.ID, guestapi.Binary, "token"})
	if err != nil {
		if ctx.Err() != nil {
			return boxprovider.Connection{}, ctx.Err()
		}
		return boxprovider.Connection{}, errors.New("guest token retrieval failed")
	}
	token := strings.TrimSpace(string(out))
	if !tokenPattern.MatchString(token) {
		return boxprovider.Connection{}, errors.New("invalid guest token")
	}
	return boxprovider.Connection{URL: "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), Token: token}, nil
}
