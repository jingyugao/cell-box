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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
	"cellbox.local/cellbox/internal/guestapi"
	"cellbox.local/cellbox/internal/homevolume"
	"cellbox.local/cellbox/internal/inventory"
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
	var item inventory.Record
	if err := json.Unmarshal(r.Spec.Inventory, &item); err != nil {
		return err
	}
	var box map[string]json.RawMessage
	if err := json.Unmarshal(item.Box, &box); err != nil {
		return err
	}
	if item.ClientID == "" || box == nil {
		return errors.New("invalid local checkpoint inventory")
	}
	box["phase"], _ = json.Marshal("suspended")
	box["generation"], _ = json.Marshal(r.Generation)
	box["imageId"], _ = json.Marshal(r.Handle.ImageID)
	delete(box, "executionId")
	delete(box, "operationId")
	delete(box, "error")
	item.Box, _ = json.Marshal(box)
	item.RuntimeID, item.Snapshot, item.NodeName = r.Handle.ID, r.Snapshot, "local"
	return p.checkpoints.PutCheckpoint(ctx, item)
}

func (p *Provider) inspect(ctx context.Context, h boxprovider.Handle) (boxprovider.Observation, error) {
	r, err := p.owned(h)
	if err != nil {
		return boxprovider.Observation{}, err
	}
	o := boxprovider.Observation{Phase: r.Phase, Generation: r.Generation, TransitionID: r.TransitionID,
		ObservedTransitionID: r.ObservedTransitionID, Message: r.Message}
	if o.Phase == "" {
		o.Phase = "creating"
	}
	if o.Phase == "failed" {
		return o, nil
	}
	s, err := p.state(ctx, h.ID)
	if o.Phase == "suspended" && errors.Is(err, boxprovider.ErrNotFound) {
		return o, nil
	}
	if err != nil && !errors.Is(err, boxprovider.ErrNotFound) {
		return o, err
	}
	if o.Phase == "running" && err == nil && s.Status == "running" {
		o.ExecutionID, o.GuestReady = r.execution(), r.Ready
	} else if o.Phase == "running" || o.Phase == "suspended" {
		o.Phase, o.Message = "failed", "local runsc state disagrees with the committed lifecycle"
	}
	return o, nil
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
		var previous boxprovider.Observation
		first := true
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
			if first || o != previous {
				select {
				case stream <- o:
				case <-ctx.Done():
					return
				}
				previous, first = o, false
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

func (p *Provider) RequestResume(ctx context.Context, h boxprovider.Handle, previous, next string) error {
	return p.ActionTransition(ctx, h, "resume", previous, next)
}

func (p *Provider) ActionTransition(ctx context.Context, h boxprovider.Handle, action, previous, next string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, err := p.owned(h)
	if err != nil {
		return err
	}
	if !identifier.MatchString(next) || next == previous || r.TransitionID != previous {
		return boxprovider.ErrStaleTransition
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	switch action {
	case "suspend":
		err := p.suspend(ctx, r, next)
		if err != nil {
			// The caller quiesced the guest before admission. A rejection before
			// the durable suspending intent must leave that running guest usable.
			current, loadErr := p.owned(h)
			if loadErr == nil && current.Phase == "running" && current.TransitionID == previous {
				undoCtx, undoCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				undoErr := unquiesce(undoCtx, current.Network.Guest)
				undoCancel()
				if undoErr != nil {
					return p.failTransition(current, errors.Join(err, undoErr))
				}
			}
		}
		return err
	case "resume":
		return p.resume(ctx, r, next)
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

func (p *Provider) resume(ctx context.Context, r record, transition string) error {
	if r.Phase != "suspended" || r.Generation == math.MaxUint64 {
		return boxprovider.ErrNotReady
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
	r.Generation++
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
	if err := unquiesce(ctx, r.Network.Guest); err != nil {
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

func unquiesce(ctx context.Context, guest string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, ResponseHeaderTimeout: time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request := func(method, path string) error {
		req, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("http://%s:%d%s", guest, guestapi.Port, path), nil)
		if err != nil {
			return err
		}
		response, err := client.Do(req)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusNoContent {
			return fmt.Errorf("restored guest %s: HTTP %d", path, response.StatusCode)
		}
		return nil
	}
	for {
		if err := request(http.MethodGet, "/healthz"); err == nil {
			if err := request(http.MethodPost, "/v1/unquiesce"); err != nil {
				return err
			}
			return request(http.MethodGet, "/healthz")
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("restored guest readiness: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
