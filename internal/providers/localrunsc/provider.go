// Package localrunsc runs OCI sandboxes directly, without a container daemon.
package localrunsc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/inventory"
	"cellbox.local/cellbox/internal/objectstorage"
	"golang.org/x/sys/unix"
)

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$`)
var executionID = regexp.MustCompile(`^[a-f0-9]{32}$`)
var registryImage = regexp.MustCompile(`^[^\s@]+@sha256:[a-f0-9]{64}$`)

// Provider serializes node side effects and fences other servers using its directory.
type Provider struct {
	mu                sync.Mutex
	base, guestBinary string
	guestPortProtocol int
	platform          string
	lock              *os.File
	changed           chan struct{}
	checkpoints       inventory.CheckpointStore
	objects           objectstorage.Objects
	bootID            string
	bootChanged       bool
	insecureRegistry  string
}

type record struct {
	Handle               boxprovider.Handle `json:"handle"`
	Spec                 boxprovider.Spec   `json:"spec"`
	Fingerprint          string             `json:"fingerprint"`
	Network              network            `json:"network"`
	Ready                bool               `json:"ready"`
	Phase                string             `json:"phase,omitempty"`
	Generation           uint64             `json:"generation,omitempty"`
	TransitionID         string             `json:"transitionId,omitempty"`
	ObservedTransitionID string             `json:"observedTransitionId,omitempty"`
	Snapshot             string             `json:"snapshot,omitempty"`
	Platform             string             `json:"platform,omitempty"`
	RunscVersion         string             `json:"runscVersion,omitempty"`
	Message              string             `json:"message,omitempty"`
	InventoryStage       string             `json:"inventoryStage,omitempty"`
	BootID               string             `json:"bootId,omitempty"`
	GuestPortProtocol    int                `json:"guestPortProtocol,omitempty"`
}

func New(dataDir, guestBinary, platform string) (*Provider, error) {
	if platform == "" {
		platform = "systrap"
	}
	if platform != "systrap" && platform != "kvm" {
		return nil, errors.New("local platform must be systrap or kvm")
	}
	if os.Geteuid() != 0 {
		return nil, errors.New("local-runsc requires root for namespaces, networking and guest ownership")
	}
	for _, binary := range []string{"runsc", "skopeo", "umoci", "ip", "iptables"} {
		if _, err := exec.LookPath(binary); err != nil {
			return nil, fmt.Errorf("local-runsc prerequisite %s: %w", binary, err)
		}
	}
	base, err := filepath.Abs(filepath.Join(dataDir, "local-runsc"))
	if err != nil {
		return nil, err
	}
	if guestBinary == "" {
		exe, err := os.Executable()
		if err != nil {
			return nil, err
		}
		guestBinary = filepath.Join(filepath.Dir(exe), "cellbox-container-agent")
	}
	guestBinary, err = filepath.Abs(guestBinary)
	if err != nil {
		return nil, err
	}
	if info, err := os.Stat(guestBinary); err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return nil, errors.New("local-runsc requires a Linux guest executable; build cellbox-container-agent or set --local-guest-binary")
	}
	guestProtocol, err := guestapi.BinaryProtocolVersion(guestBinary)
	if err != nil {
		return nil, fmt.Errorf("inspect local Guest capabilities: %w", err)
	}
	guestPortProtocol := 0
	if guestProtocol == guestapi.ProtocolVersion {
		guestPortProtocol = 2
	}
	for _, dir := range []string{base, filepath.Join(base, "boxes"), filepath.Join(base, "images"), filepath.Join(base, "runsc")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
		info, err := os.Lstat(dir)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
			return nil, fmt.Errorf("local runtime directory must be private: %s", dir)
		}
	}
	lock, err := os.OpenFile(filepath.Join(base, "server.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, errors.New("another local-runsc server owns this data directory")
	}
	p := &Provider{base: base, guestBinary: guestBinary, guestPortProtocol: guestPortProtocol, platform: platform, lock: lock, changed: make(chan struct{})}
	if err := p.prepareBoot(); err != nil {
		lock.Close()
		return nil, err
	}
	if err := p.recoverInterrupted(); err != nil {
		lock.Close()
		return nil, err
	}
	return p, nil
}

func (*Provider) Name() string { return "local-runsc" }

// Close releases the server fence; running sandboxes survive server restarts.
func (p *Provider) Close() error { return p.lock.Close() }

func command(ctx context.Context, binary string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, binary, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s %s", binary, strings.Join(args, " "), err, strings.TrimSpace(string(out)), strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// SetInsecureRegistry uses the same explicit HTTP registry exception as the
// shared image importer. It is configured before any runtime work starts.
func (p *Provider) SetInsecureRegistry(host string) { p.insecureRegistry = host }

func (p *Provider) run(ctx context.Context, args ...string) ([]byte, error) {
	runArgs := append([]string{"--root=" + filepath.Join(p.base, "runsc"), "--network=sandbox", "--platform=" + p.platform, "--debug-log=" + filepath.Join(p.base, "runsc.log"), "--debug-command=create,start,boot,gofer"}, args...)
	if len(args) > 0 && (args[0] == "create" || args[0] == "restore") {
		// Sentry inherits OCI stdio and outlives create. A capture pipe would
		// keep exec.Wait blocked until the whole sandbox exits.
		log, err := os.OpenFile(filepath.Join(p.base, "runsc", args[len(args)-1]+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		defer log.Close()
		cmd := exec.CommandContext(ctx, "runsc", runArgs...)
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("runsc %s failed (see %s): %w", args[0], log.Name(), err)
		}
		return nil, nil
	}
	return command(ctx, "runsc", runArgs...)
}

func (p *Provider) dir(name string) string { return filepath.Join(p.base, "boxes", name) }
func (p *Provider) save(r record) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	dir := p.dir(r.Handle.Name)
	f, err := os.OpenFile(filepath.Join(dir, "record.tmp"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	syncErr := f.Sync()
	if err := errors.Join(writeErr, syncErr, f.Close()); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(dir, "record.tmp"), filepath.Join(dir, "record.json")); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return err
	}
	p.notify()
	return nil
}
func (p *Provider) load(name string) (record, error) {
	var r record
	if !identifier.MatchString(name) {
		return r, errors.New("invalid local sandbox name")
	}
	b, err := os.ReadFile(filepath.Join(p.dir(name), "record.json"))
	if os.IsNotExist(err) {
		return r, boxprovider.ErrNotFound
	}
	if err != nil {
		return r, err
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return r, err
	}
	if r.Handle.Provider != p.Name() || r.Handle.Name != name || !executionID.MatchString(r.Handle.ID) || r.Network.Name != "cbl"+r.Handle.ID[:12] {
		return r, errors.New("invalid local sandbox ownership record")
	}
	if r.Generation == 0 {
		r.Generation = 1
	}
	if r.Phase == "" && r.Ready {
		r.Phase = "running"
	}
	return r, nil
}

func (p *Provider) owned(h boxprovider.Handle) (record, error) {
	if h.Provider != p.Name() || h.ID == "" {
		return record{}, errors.New("invalid local sandbox handle")
	}
	r, err := p.load(h.Name)
	if err == nil && r.Handle != h {
		return r, errors.New("local sandbox execution identity changed")
	}
	return r, err
}

func validate(s boxprovider.Spec) error {
	if !identifier.MatchString(s.BoxID) || !registryImage.MatchString(s.Image) {
		return errors.New("local-runsc requires a valid box ID and repository@sha256:digest image")
	}
	if s.NodeName != "" || s.Namespace != "" {
		return boxprovider.ErrUnsupported
	}
	for _, host := range []string{s.SharedReadOnlyHostPath, s.DebugReadOnlyHostPath, s.DebugReadWriteHostPath} {
		if host != "" && (!filepath.IsAbs(host) || filepath.Clean(host) != host || host == "/" || strings.ContainsAny(host, "\x00\r\n")) {
			return errors.New("invalid local host mount path")
		}
	}
	if s.DebugReadOnlyHostPath != "" && s.DebugReadWriteHostPath != "" {
		return errors.New("local debug HOME cannot have both read-only and writable host mounts")
	}
	if math.IsNaN(s.CPU) || math.IsInf(s.CPU, 0) || s.CPU < 0 || s.CPU > float64(math.MaxInt64)/100000 || s.MemoryMiB < 0 || s.MemoryMiB > math.MaxInt64/(1<<20) {
		return errors.New("invalid local resource limits")
	}
	if s.CPU > 0 && s.CPU < 0.01 {
		return errors.New("local CPU limit must be at least 0.01 cores or zero for unlimited")
	}
	if s.PersistentHome && s.HomeVolumeUID == "" {
		return errors.New("local persistent HOME requires an independent volume")
	}
	for _, mount := range s.Mounts {
		if !identifier.MatchString(mount.UID) || !filepath.IsAbs(mount.MountPath) || filepath.Clean(mount.MountPath) != mount.MountPath || mount.MountPath == "/" {
			return errors.New("invalid local volume mount")
		}
	}
	return nil
}

func (p *Provider) Create(ctx context.Context, s boxprovider.Spec) (boxprovider.Handle, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var zero boxprovider.Handle
	if err := validate(s); err != nil {
		return zero, err
	}
	data, err := json.Marshal(s)
	if err != nil {
		return zero, err
	}
	hash := sha256.Sum256(data)
	fingerprint := hex.EncodeToString(hash[:])
	if r, err := p.load(s.BoxID); err == nil {
		if r.Fingerprint != fingerprint {
			return zero, errors.New("existing local sandbox specification differs")
		}
		if r.Ready {
			return r.Handle, nil
		}
		if err := p.cleanup(ctx, r); err != nil {
			return zero, err
		}
	} else if !errors.Is(err, boxprovider.ErrNotFound) {
		return zero, err
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return zero, err
	}
	h := boxprovider.Handle{Provider: p.Name(), ID: hex.EncodeToString(id), Name: s.BoxID, ImageID: s.Image}
	reserved, err := p.records()
	if err != nil {
		return zero, err
	}
	networks := make([]network, 0, len(reserved))
	for _, existing := range reserved {
		networks = append(networks, existing.Network)
	}
	n, err := allocateNetwork(ctx, h.ID, networks...)
	if err != nil {
		return zero, err
	}
	r := record{Handle: h, Spec: s, Fingerprint: fingerprint, Network: n, Phase: "creating", Generation: 1, Platform: p.platform, BootID: p.bootID, GuestPortProtocol: p.guestPortProtocol}
	if s.Staged {
		r.InventoryStage = "staged"
		var item inventory.Record
		var box struct {
			Phase string `json:"phase"`
		}
		if json.Unmarshal(s.Inventory, &item) == nil && json.Unmarshal(item.Box, &box) == nil && box.Phase == "restoring" {
			r.InventoryStage = "restoring"
		}
	}
	if err := os.MkdirAll(p.dir(h.Name), 0700); err != nil {
		return zero, err
	}
	if err := p.save(r); err != nil {
		return zero, err
	}
	fail := func(cause error) (boxprovider.Handle, error) {
		slog.Error("local sandbox creation failed", "box", s.BoxID, "error", cause)
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		return zero, errors.Join(cause, p.cleanup(cleanupCtx, r))
	}
	if err := p.prepareImage(ctx, r); err != nil {
		return fail(err)
	}
	if err := n.create(ctx); err != nil {
		return fail(err)
	}
	if err := p.configure(ctx, r); err != nil {
		return fail(err)
	}
	if _, err := p.run(ctx, "create", "--bundle="+filepath.Join(p.dir(h.Name), "bundle"), h.ID); err != nil {
		return fail(err)
	}
	if _, err := p.run(ctx, "start", h.ID); err != nil {
		return fail(err)
	}
	r.Ready = true
	r.Phase = "running"
	if err := p.save(r); err != nil {
		return fail(err)
	}
	return h, nil
}

type runtimeState struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func (p *Provider) state(ctx context.Context, id string) (runtimeState, error) {
	var state runtimeState
	// list distinguishes an absent sandbox from permission, transport and corrupt state errors.
	b, err := p.run(ctx, "list", "--format=json")
	if err != nil {
		return state, err
	}
	var states []runtimeState
	if err := json.Unmarshal(b, &states); err != nil {
		return state, err
	}
	for _, s := range states {
		if s.ID == id {
			return s, nil
		}
	}
	return state, boxprovider.ErrNotFound
}

func (p *Provider) Inspect(ctx context.Context, h boxprovider.Handle) (boxprovider.Observation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.inspect(ctx, h)
}

func (p *Provider) Guest(ctx context.Context, h boxprovider.Handle) (boxprovider.Connection, error) {
	return p.GuestForExecution(ctx, h, "")
}

func (p *Provider) GuestForExecution(ctx context.Context, h boxprovider.Handle, execution string) (boxprovider.Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, err := p.owned(h)
	if err != nil {
		return boxprovider.Connection{}, err
	}
	if execution != "" && execution != r.execution() {
		return boxprovider.Connection{}, boxprovider.ErrStaleExecution
	}
	s, err := p.state(ctx, h.ID)
	if err != nil {
		return boxprovider.Connection{}, err
	}
	if !r.Ready || r.Phase != "running" || s.Status != "running" {
		return boxprovider.Connection{}, boxprovider.ErrNotReady
	}
	return boxprovider.Connection{URL: fmt.Sprintf("http://%s:%d", r.Network.Guest, guestapi.Port)}, nil
}

func (p *Provider) Destroy(ctx context.Context, h boxprovider.Handle) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, err := p.owned(h)
	if errors.Is(err, boxprovider.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	err = p.cleanup(ctx, r)
	if err != nil {
		slog.Error("local sandbox cleanup failed", "box", h.Name, "error", err)
	}
	return err
}

func (p *Provider) cleanup(ctx context.Context, r record) error {
	if _, err := p.state(ctx, r.Handle.ID); err == nil {
		if _, err := p.run(ctx, "delete", "--force", r.Handle.ID); err != nil {
			return err
		}
	} else if !errors.Is(err, boxprovider.ErrNotFound) {
		return err
	}
	if err := r.Network.remove(ctx); err != nil {
		return err
	}
	if p.checkpoints != nil {
		if err := p.checkpoints.DeleteCheckpoint(ctx, r.Handle.ID, ""); err != nil {
			return err
		}
	}
	if err := os.RemoveAll(p.dir(r.Handle.Name)); err != nil {
		return err
	}
	p.notify()
	return nil
}

func (p *Provider) volumeBusy(uid string) error {
	entries, err := os.ReadDir(filepath.Join(p.base, "boxes"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		r, err := p.load(entry.Name())
		if err != nil {
			return err
		}
		if r.Spec.HomeVolumeUID == uid {
			return errors.New("volume is still attached to a local sandbox")
		}
		for _, m := range r.Spec.Mounts {
			if m.UID == uid {
				return errors.New("volume is still attached to a local sandbox")
			}
		}
	}
	return nil
}

func (p *Provider) CreateVolume(ctx context.Context, s boxprovider.VolumeSpec) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.NodeName != "" || s.Namespace != "" || !identifier.MatchString(s.UID) {
		return errors.New("invalid local volume")
	}
	if _, err := homevolume.Prepare(p.base, s.UID, "local", homevolume.VolumeSpecHash, true); err != nil {
		return err
	}
	path, err := homevolume.Path(p.base, s.UID)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Chown(int(s.Agent.UID), int(s.Agent.GID)); err != nil {
		return err
	}
	return f.Chmod(0700)
}

func (p *Provider) DeleteVolume(ctx context.Context, s boxprovider.VolumeSpec) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.volumeBusy(s.UID); err != nil {
		return err
	}
	return homevolume.RemoveVolume(p.base, s.UID, "local")
}

func (p *Provider) WithStoppedVolume(ctx context.Context, s boxprovider.VolumeSpec, work func(string) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.withStoppedVolume(ctx, s.UID, work)
}

// The provider mutex fences create/resume against the entire stopped-volume
// operation. The existing HOME lock coordinates backup, archive and deletion.
func (p *Provider) withStoppedVolume(ctx context.Context, uid string, work func(string) error) error {
	records, err := p.records()
	if err != nil {
		return err
	}
	for _, r := range records {
		if !r.volumes()[uid] {
			continue
		}
		if r.Phase != "suspended" && r.Phase != "failed" {
			return boxprovider.ErrNotReady
		}
		if _, err := p.state(ctx, r.Handle.ID); !errors.Is(err, boxprovider.ErrNotFound) {
			if err != nil {
				return err
			}
			return boxprovider.ErrNotReady
		}
	}
	err = homevolume.WithVolume(ctx, p.base, uid, "local", work)
	if errors.Is(err, homevolume.ErrStaleVolume) {
		return boxprovider.ErrStaleVolume
	}
	return err
}

func guestConfig(c guestapi.Config) (string, error) {
	b, err := json.Marshal(c)
	return base64.StdEncoding.EncodeToString(b), err
}
