package localrunsc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jingyugao/cell-box/internal/boxprovider"
	"github.com/jingyugao/cell-box/internal/guestapi"
	"github.com/jingyugao/cell-box/internal/homevolume"
	"github.com/jingyugao/cell-box/internal/inventory"
	"golang.org/x/sys/unix"
)

var (
	_ boxprovider.TransitionProvider            = (*Provider)(nil)
	_ boxprovider.ResumeRequestProvider         = (*Provider)(nil)
	_ boxprovider.CompletionObservationProvider = (*Provider)(nil)
	_ boxprovider.FencedGuestProvider           = (*Provider)(nil)
)

// Called with mu held, except during initialization before the provider is shared.
func (p *Provider) notify() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (r record) execution() string {
	if r.Generation <= 1 {
		return r.Handle.ID
	}
	return r.Handle.ID + "-" + strconv.FormatUint(r.Generation, 10)
}

func (p *Provider) recoverInterrupted() error {
	entries, err := os.ReadDir(filepath.Join(p.base, "boxes"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		r, err := p.load(entry.Name())
		if errors.Is(err, boxprovider.ErrNotFound) {
			continue // Creation may have stopped before the first ownership record.
		}
		if err != nil {
			return err
		}
		if r.Phase == "suspending" || r.Phase == "resuming" {
			// Replaying a restore could rewind memory against an already changed disk.
			r.Phase, r.Message = "failed", "local lifecycle interrupted; discard this sandbox instead of replaying its checkpoint"
			if err := p.save(r); err != nil {
				return err
			}
		} else if r.Phase == "running" && (p.bootChanged || (r.BootID != "" && r.BootID != p.bootID)) {
			r.Phase, r.Message = "failed", "host restarted without a suspended checkpoint; automatic cold restart is disabled"
			if err := p.save(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// SetCheckpointStore shares the service ledger and reconciles interrupted inventory
// updates. Payloads and writable files remain owned by this provider on this node.
func (p *Provider) SetCheckpointStore(ctx context.Context, store inventory.CheckpointStore) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.checkpoints = store
	entries, err := os.ReadDir(filepath.Join(p.base, "boxes"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		r, err := p.load(entry.Name())
		if errors.Is(err, boxprovider.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if r.Phase == "suspended" {
			err = p.publishCheckpoint(ctx, r)
		} else {
			err = store.DeleteCheckpoint(ctx, r.Handle.ID, "")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Provider) publishCheckpoint(ctx context.Context, r record) error {
	if p.checkpoints == nil {
		return errors.New("local checkpoint inventory is not configured")
	}
	item, err := r.inventory(r.observation())
	if err != nil {
		return err
	}
	return p.checkpoints.PutCheckpoint(ctx, item)
}

func (p *Provider) inspect(ctx context.Context, h boxprovider.Handle) (boxprovider.Observation, error) {
	r, err := p.owned(h)
	if err != nil {
		return boxprovider.Observation{}, err
	}
	if r.Phase == "failed" {
		return r.observation(), nil
	}
	s, err := p.state(ctx, h.ID)
	if err != nil && !errors.Is(err, boxprovider.ErrNotFound) {
		return boxprovider.Observation{}, err
	}
	return r.observeState(s, err == nil), nil
}

func (p *Provider) WatchObservations(ctx context.Context, h boxprovider.Handle) (<-chan boxprovider.Observation, error) {
	p.mu.Lock()
	_, err := p.owned(h)
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	stream := make(chan boxprovider.Observation)
	go func() {
		defer close(stream)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			p.mu.Lock()
			o, err := p.inspect(ctx, h)
			changed := p.changed
			p.mu.Unlock()
			if errors.Is(err, boxprovider.ErrNotFound) {
				o, err = boxprovider.Observation{Phase: "deleted"}, nil
			}
			if err != nil {
				return // The service reconnects after transient inspection failures.
			}
			// Heartbeats also repair a service-side admission that failed before
			// changing runtime state, after its operation has been cleared.
			select {
			case stream <- o:
			case <-ctx.Done():
				return
			}
			if o.Phase == "deleted" {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-changed:
			case <-ticker.C:
			}
		}
	}()
	return stream, nil
}

func (p *Provider) Action(context.Context, boxprovider.Handle, string) error {
	// The service uses conditional transition tokens for checkpoint operations.
	return boxprovider.ErrUnsupported
}

func (p *Provider) RequestResume(ctx context.Context, h boxprovider.Handle, previous, next string, ports ...int) error {
	if len(ports) == 0 {
		ports = []int{guestapi.Port}
	}
	return p.actionTransition(ctx, h, "resume", previous, next, ports)
}

func (p *Provider) ActionTransition(ctx context.Context, h boxprovider.Handle, action, previous, next string) (result error) {
	return p.actionTransition(ctx, h, action, previous, next, nil)
}

func (p *Provider) actionTransition(ctx context.Context, h boxprovider.Handle, action, previous, next string, ports []int) (result error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer func() {
		if result != nil {
			slog.Error("local sandbox transition failed", "box", h.Name, "action", action, "error", result)
		}
	}()
	r, err := p.owned(h)
	if err != nil {
		return err
	}
	if !identifier.MatchString(next) || next == previous || r.TransitionID != previous {
		return boxprovider.ErrStaleTransition
	}
	if action == "resume" {
		ports, err = boxprovider.NormalizeWaitPorts(ports)
		if err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	switch action {
	case "suspend":
		return p.suspend(ctx, r, next)
	case "resume":
		return p.resume(ctx, r, next, ports)
	default:
		return boxprovider.ErrUnsupported
	}
}

func (p *Provider) failTransition(r record, cause error) error {
	r.Phase, r.Message = "failed", cause.Error()
	return errors.Join(cause, p.save(r))
}

func (p *Provider) deleteExecution(ctx context.Context, id string) error {
	if _, err := p.state(ctx, id); errors.Is(err, boxprovider.ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	if _, err := p.run(ctx, "delete", "--force", id); err != nil {
		return err
	}
	if _, err := p.state(ctx, id); !errors.Is(err, boxprovider.ErrNotFound) {
		return errors.Join(errors.New("runsc execution still exists after deletion"), err)
	}
	return nil
}

func (p *Provider) suspend(ctx context.Context, r record, transition string) error {
	s, err := p.state(ctx, r.Handle.ID)
	if err != nil {
		return err
	}
	if r.Phase != "running" || s.Status != "running" || !r.Ready {
		return boxprovider.ErrNotReady
	}
	if r.Platform != "" && r.Platform != p.platform {
		return errors.New("checkpoint requires the sandbox's original execution platform")
	}
	if p.checkpoints == nil {
		return errors.New("local checkpoint inventory is not configured")
	}
	version, err := command(ctx, "runsc", "--version")
	if err != nil {
		return err
	}
	if err := r.Network.rememberMACs(ctx); err != nil {
		return err
	}
	r.Phase, r.TransitionID, r.Snapshot = "suspending", transition, transition
	r.Platform, r.RunscVersion = p.platform, strings.TrimSpace(string(version))
	if err := p.save(r); err != nil {
		return err
	}
	path := p.snapshotPath(r)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return p.failTransition(r, err)
	}
	if _, err := p.run(ctx, "checkpoint", "--compression=none", "--image-path="+path, r.Handle.ID); err != nil {
		return p.failTransition(r, err)
	}
	if err := p.validateSnapshot(r); err != nil {
		return p.failTransition(r, err)
	}
	if err := p.deleteExecution(ctx, r.Handle.ID); err != nil {
		return p.failTransition(r, err)
	}
	if err := p.syncCheckpoint(r); err != nil {
		return p.failTransition(r, err)
	}
	r.Phase, r.ObservedTransitionID = "suspended", transition
	if err := p.publishCheckpoint(ctx, r); err != nil {
		return p.failTransition(r, err)
	}
	return p.save(r)
}

func (p *Provider) snapshotPath(r record) string {
	return filepath.Join(p.dir(r.Handle.Name), "checkpoints", r.Snapshot)
}

func (r record) volumes() map[string]bool {
	volumes := map[string]bool{}
	if r.Spec.HomeVolumeUID != "" {
		volumes[r.Spec.HomeVolumeUID] = true
	}
	for _, m := range r.Spec.Mounts {
		volumes[m.UID] = true
	}
	return volumes
}

func (p *Provider) syncCheckpoint(r record) error {
	// Checkpoints reference the retained writable files. Flush their backing
	// filesystems too, before publishing a durable suspended record.
	paths := []string{p.snapshotPath(r), filepath.Join(p.dir(r.Handle.Name), "bundle", "rootfs")}
	for uid := range r.volumes() {
		path, err := homevolume.Path(p.base, uid)
		if err != nil {
			return err
		}
		paths = append(paths, path)
	}
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		err = errors.Join(unix.Syncfs(int(f.Fd())), f.Close())
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Provider) validateSnapshot(r record) error {
	if !identifier.MatchString(r.Snapshot) {
		return errors.New("invalid local checkpoint identity")
	}
	for _, name := range []string{"checkpoint.img", "pages.img", "pages_meta.img"} {
		info, err := os.Lstat(filepath.Join(p.snapshotPath(r), name))
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("invalid local checkpoint payload")
		}
	}
	return nil
}

func (p *Provider) resume(ctx context.Context, r record, transition string, ports []int) error {
	if r.Phase != "suspended" || r.Generation == math.MaxUint64 {
		return boxprovider.ErrNotReady
	}
	if r.GuestPortProtocol < 1 && (len(ports) != 1 || ports[0] != guestapi.Port) {
		return fmt.Errorf("%w: legacy Guest checkpoint cannot inspect custom wait ports", boxprovider.ErrUnsupported)
	}
	if err := p.validateSnapshot(r); err != nil {
		return err
	}
	version, err := command(ctx, "runsc", "--version")
	if err != nil {
		return err
	}
	if r.Platform != p.platform || r.RunscVersion != strings.TrimSpace(string(version)) {
		return errors.New("checkpoint requires the same runsc version and execution platform")
	}
	for uid := range r.volumes() {
		if _, err := homevolume.Verify(p.base, uid, "local", homevolume.VolumeSpecHash, ""); err != nil {
			return fmt.Errorf("%w: %v", boxprovider.ErrStaleVolume, err)
		}
	}
	// Commit the consumed execution cycle before invoking restore. Once runsc
	// starts, retrying this snapshot against a writable disk is unsafe.
	if _, err := p.state(ctx, r.Handle.ID); !errors.Is(err, boxprovider.ErrNotFound) {
		if err != nil {
			return err
		}
		return errors.New("suspended sandbox still has a runtime execution")
	}
	if err := r.Network.ensure(ctx); err != nil {
		return err
	}
	r.Generation++
	r.BootID = p.bootID
	r.Phase, r.TransitionID = "resuming", transition
	if err := p.save(r); err != nil {
		return err
	}
	if p.checkpoints == nil {
		return p.failTransition(r, errors.New("local checkpoint inventory is not configured"))
	}
	if err := p.checkpoints.DeleteCheckpoint(ctx, r.Handle.ID, r.Snapshot); err != nil {
		return p.failTransition(r, err)
	}
	if err := p.deleteExecution(ctx, r.Handle.ID); err != nil {
		return p.failTransition(r, err)
	}
	if err := r.Network.configureGuest(ctx); err != nil {
		return p.failTransition(r, err)
	}
	if _, err := p.run(ctx, "restore", "--detach", "--bundle="+filepath.Join(p.dir(r.Handle.Name), "bundle"), "--image-path="+p.snapshotPath(r), r.Handle.ID); err != nil {
		return p.failTransition(r, err)
	}
	if err := waitPortsListening(ctx, r.Network.Guest, ports); err != nil {
		return p.failTransition(r, err)
	}
	s, err := p.state(ctx, r.Handle.ID)
	if err != nil || s.Status != "running" {
		return p.failTransition(r, errors.Join(errors.New("restored runsc execution is not running"), err))
	}
	consumed := p.snapshotPath(r)
	r.Phase, r.ObservedTransitionID, r.Snapshot, r.Message = "running", transition, "", ""
	if err := p.save(r); err != nil {
		return err
	}
	if err := os.RemoveAll(consumed); err != nil {
		slog.Warn("cannot remove consumed local checkpoint", "box", r.Handle.Name, "error", err)
	}
	return nil
}

func waitPortsListening(ctx context.Context, guest string, ports []int) error {
	values := make(url.Values, 1)
	portStrings := make([]string, len(ports))
	for i, port := range ports {
		if port < 1 || port > 65535 {
			return fmt.Errorf("invalid wait port %d", port)
		}
		portStrings[i] = strconv.Itoa(port)
	}
	values.Set("ports", strings.Join(portStrings, ","))
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, ResponseHeaderTimeout: time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s:%d/v1/ports/listening?%s", guest, guestapi.Port, values.Encode()), nil)
		if err != nil {
			return err
		}
		response, err := client.Do(req)
		if err == nil {
			status := response.StatusCode
			if status == http.StatusNotFound {
				_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
				_ = response.Body.Close()
				if len(ports) != 1 || ports[0] != guestapi.Port {
					return fmt.Errorf("%w: restored Guest cannot inspect custom ports", boxprovider.ErrUnsupported)
				}
				return unquiesceLegacyGuest(ctx, client, guest)
			}
			if status != http.StatusOK {
				_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
				_ = response.Body.Close()
				return fmt.Errorf("guest port inspection: HTTP %d", status)
			}
			var state listeningPorts
			decodeErr := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&state)
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if decodeErr != nil {
				return fmt.Errorf("decode guest port inspection: %w", decodeErr)
			}
			listening := make(map[int]bool, len(state.Listening))
			for _, port := range state.Listening {
				listening[port] = true
			}
			complete := len(state.Missing) == 0
			for _, port := range ports {
				complete = complete && listening[port]
			}
			if complete {
				return nil
			}
		} else if ctx.Err() != nil {
			return fmt.Errorf("wait for guest ports %v: %w", ports, ctx.Err())
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for guest ports %v: %w", ports, ctx.Err())
		case <-ticker.C:
		}
	}
}

func unquiesceLegacyGuest(ctx context.Context, client *http.Client, guest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s:%d/v1/unquiesce", guest, guestapi.Port), nil)
	if err != nil {
		return err
	}
	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("reopen legacy Guest admission: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("reopen legacy Guest admission: HTTP %d", response.StatusCode)
	}
	return nil
}

type listeningPorts struct {
	Listening []int `json:"listening"`
	Missing   []int `json:"missing"`
}
