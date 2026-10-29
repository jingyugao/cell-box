package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/fsnotify/fsnotify"
	api "github.com/jingyugao/cell-box/api/v1alpha1"
	backend "github.com/jingyugao/cell-box/internal/runtime"
	"github.com/jingyugao/cell-box/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sys/unix"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// WarmLease is an execution lease, not a running or ready business container.
// Its deadline stays below the kubelet runtime RPC timeout. Expired Pod UIDs
// are never rearmed; the pool replaces the entire Pod.
type WarmLease struct {
	UID, Name, Namespace, SpecHash, SID, Phase string
	Deadline                                   time.Time
	Ticket                                     Ticket
	DeferredRestore                            bool `json:"deferredRestore,omitempty"`
}

func warmUpdate(base, uid string, create bool, fn func(*WarmLease) error) (WarmLease, error) {
	return warmUpdateContext(context.Background(), base, uid, create, fn)
}

func warmUpdateContext(_ context.Context, base, uid string, create bool, fn func(*WarmLease) error) (l WarmLease, resultErr error) {
	if !ValidID(uid) {
		return l, fmt.Errorf("invalid warm Pod UID")
	}
	dir := filepath.Join(base, "warm")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return l, err
	}
	f, err := os.OpenFile(filepath.Join(dir, uid+".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return l, err
	}
	defer f.Close()
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		return l, err
	}
	defer unix.Flock(int(f.Fd()), unix.LOCK_UN)
	path := filepath.Join(dir, uid+".json")
	if err = ReadJSON(path, &l); err != nil && !(create && errors.Is(err, os.ErrNotExist)) {
		return l, err
	}
	before := l
	if err = fn(&l); err != nil {
		return l, err
	}
	if !reflect.DeepEqual(before, l) {
		err = AtomicJSON(path, l)
	}
	return l, err
}

func (b *Backend) RegisterWarm(ctx context.Context, p *core.Pod, hash string) error {
	_, err := warmUpdateContext(ctx, b.Base, string(p.UID), true, func(l *WarmLease) error {
		if l.UID != "" {
			if l.UID == string(p.UID) && l.Name == p.Name && l.Namespace == p.Namespace && l.SpecHash == hash {
				return nil
			}
			return fmt.Errorf("warm Pod identity changed")
		}
		*l = WarmLease{UID: string(p.UID), Name: p.Name, Namespace: p.Namespace, SpecHash: hash, Phase: "registered"}
		return nil
	})
	return err
}

func (b *Backend) WarmStatus(ctx context.Context, p *core.Pod) (WarmLease, error) {
	return warmUpdateContext(ctx, b.Base, string(p.UID), false, func(l *WarmLease) error {
		if l.Name != p.Name || l.Namespace != p.Namespace {
			return fmt.Errorf("warm Pod identity mismatch")
		}
		return nil
	})
}

func BindWarm(base, uid, name, namespace, sid string) (WarmLease, error) {
	return warmUpdate(base, uid, false, func(l *WarmLease) error {
		if l.UID != uid || l.Name != name || l.Namespace != namespace || l.Phase != "registered" || l.SID != "" {
			return fmt.Errorf("warm sandbox already used or identity mismatch")
		}
		l.SID = sid
		l.Phase = "created"
		return AtomicJSON(filepath.Join(base, "requests", sid+".json"), Ticket{PodUID: uid, PodName: name, Namespace: namespace})
	})
}

// MarkDeferredRestore records that the warm sandbox root was started without
// binding its checkpoint. The child start path consumes the checkpoint after
// the controller assigns this lease, before its Gofer is created.
func MarkDeferredRestore(base, uid, name, namespace, sid string) error {
	_, err := warmUpdate(base, uid, false, func(l *WarmLease) error {
		if l.UID != uid || l.Name != name || l.Namespace != namespace || l.SID != sid || l.Phase != "created" {
			return fmt.Errorf("warm sandbox identity or phase changed")
		}
		l.DeferredRestore = true
		return nil
	})
	return err
}

// DeferredRestore reports whether this sandbox's warm root was started with
// its checkpoint intentionally unbound. A missing lease means this is not a
// warm sandbox; a present lease must match the supplied CRI identity.
func DeferredRestore(base, uid, name, namespace, sid string) (deferred bool, err error) {
	_, err = warmUpdate(base, uid, false, func(l *WarmLease) error {
		if l.UID != uid || l.Name != name || l.Namespace != namespace || l.SID != sid {
			return fmt.Errorf("warm sandbox identity mismatch")
		}
		deferred = l.DeferredRestore
		return nil
	})
	return deferred, err
}

func WaitWarm(base, uid, sid string) (Ticket, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return Ticket{}, err
	}
	defer watcher.Close()
	// Watch the directory before publishing waiting: authorization replaces
	// the lease atomically, so a watch on the old file inode would miss it.
	if err = watcher.Add(filepath.Join(base, "warm")); err != nil {
		return Ticket{}, err
	}
	initial, err := warmUpdate(base, uid, false, func(l *WarmLease) error {
		if l.SID != sid || l.Phase != "created" {
			return fmt.Errorf("warm start already attempted")
		}
		l.Phase = "waiting"
		l.Deadline = time.Now().Add(60 * time.Second)
		return nil
	})
	if err != nil {
		return Ticket{}, err
	}
	deadline := time.NewTimer(time.Until(initial.Deadline))
	defer deadline.Stop()
	for {
		l, err := warmUpdate(base, uid, false, func(l *WarmLease) error {
			if l.Phase == "waiting" && !time.Now().Before(l.Deadline) {
				l.Phase = "expired"
			}
			if l.Phase == "claimed" {
				var live Ticket
				if err := ReadJSON(filepath.Join(base, "tickets", uid+".json"), &live); err != nil {
					return err
				}
				if !reflect.DeepEqual(live, l.Ticket) {
					return fmt.Errorf("warm assignment revoked")
				}
				l.Phase = "restoring"
			}
			return nil
		})
		if err != nil {
			return Ticket{}, err
		}
		switch l.Phase {
		case "restoring":
			return l.Ticket, nil
		case "waiting":
			select {
			case <-deadline.C:
			case event, ok := <-watcher.Events:
				if !ok {
					return Ticket{}, fmt.Errorf("warm authorization watcher closed")
				}
				if event.Name != filepath.Join(base, "warm", uid+".json") {
					continue
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return Ticket{}, fmt.Errorf("warm authorization watcher closed")
				}
				if err != fsnotify.ErrEventOverflow {
					return Ticket{}, err
				}
			}
		default:
			return Ticket{}, fmt.Errorf("warm slot %s", l.Phase)
		}
	}
}

func (b *Backend) ActivateWarm(ctx context.Context, w *api.ResumablePod, p *core.Pod) (active bool, resultErr error) {
	ctx, span := telemetry.Start(ctx, "node.warm.activate", attribute.String("warm.phase", "claim"))
	defer func() { telemetry.End(span, resultErr) }()
	l, err := b.WarmStatus(ctx, p)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if l.Phase == "claimed" || l.Phase == "restoring" {
		mounts, err := b.prepareVolumeMounts(w, w.Status.Cycle == 1 && w.Status.Snapshot == "")
		if err != nil {
			return true, err
		}
		t := warmTicket(w, p, mounts)
		if !reflect.DeepEqual(l.Ticket, t) {
			return true, fmt.Errorf("warm slot assigned to another execution")
		}
		return true, nil
	}
	if l.Phase == "expired" || (l.Phase == "waiting" && !time.Now().Before(l.Deadline)) {
		return true, backend.ErrWarmExpired
	}
	if l.SpecHash != api.WarmSpecFingerprint(w) || w.Status.Snapshot == "" {
		return true, fmt.Errorf("incompatible warm restore")
	}
	_, mountSpan := telemetry.Start(ctx, "node.warm.activate.mounts")
	mounts, err := b.prepareVolumeMounts(w, w.Status.Cycle == 1 && w.Status.Snapshot == "")
	telemetry.End(mountSpan, err)
	if err != nil {
		return true, err
	}
	t := warmTicket(w, p, mounts)
	snapshotCtx, snapshotSpan := telemetry.Start(ctx, "node.warm.activate.snapshot")
	err = b.prepareSnapshot(snapshotCtx, w)
	telemetry.End(snapshotSpan, err)
	if err != nil {
		return true, err
	}
	leaseCtx, leaseSpan := telemetry.Start(ctx, "node.warm.activate.ticket_lease")
	defer func() { telemetry.End(leaseSpan, resultErr) }()
	_, err = warmUpdateContext(leaseCtx, b.Base, string(p.UID), false, func(l *WarmLease) error {
		if l.Phase != "waiting" || !time.Now().Before(l.Deadline) {
			return backend.ErrWarmExpired
		}
		if err := AtomicJSON(filepath.Join(b.Base, "tickets", string(p.UID)+".json"), t); err != nil {
			return err
		}
		l.Phase = "claimed"
		l.Ticket = t
		return nil
	})
	return true, err
}

func warmTicket(w *api.ResumablePod, p *core.Pod, mounts []VolumeMountAuthorization) Ticket {
	t := Ticket{OwnerUID: string(w.UID), NodeName: w.Spec.NodeName, HomeVolumeUID: w.Spec.HomeVolumeUID, PodUID: string(p.UID), Namespace: p.Namespace, PodName: p.Name, Snapshot: w.Status.Snapshot, SpecHash: w.Status.SpecHash,
		GuestArgv: append(append([]string(nil), w.Spec.Container.Command...), w.Spec.Container.Args...)}
	if w.Spec.Mounts != nil {
		t.Mounts = mounts
	} else if len(mounts) > 0 {
		t.HomeID = mounts[0].HomeID
	}
	return t
}

func RetireWarm(base, uid string) error {
	if !ValidID(uid) {
		return fmt.Errorf("invalid warm Pod UID")
	}
	if _, err := os.Stat(filepath.Join(base, "warm", uid+".json")); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	_, err := warmUpdate(base, uid, false, func(l *WarmLease) error { l.Phase = "retired"; return nil })
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// ChildRecord avoids launching runsc state merely to rediscover container type.
type ChildRecord struct {
	Ticket          Ticket
	SandboxID       string
	DeferredRestore bool   `json:"deferredRestore,omitempty"`
	BundleDir       string `json:"bundleDir,omitempty"`
}

func (b *Backend) removeChildren(uid string) error {
	dir := filepath.Join(b.Base, "children")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".json" {
			continue
		}
		var c ChildRecord
		if err = ReadJSON(filepath.Join(dir, e.Name()), &c); err != nil {
			return err
		}
		if c.Ticket.PodUID == uid {
			if err = os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
			if err = os.Remove(filepath.Join(dir, e.Name()+".started")); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

func (b *Backend) WarmState(ctx context.Context, p *core.Pod) (string, time.Time, error) {
	l, err := b.WarmStatus(ctx, p)
	return l.Phase, l.Deadline, err
}

// TryRetireWarm atomically retires an idle warm lease. A claimed or restoring
// lease is owned by an execution and must not be removed by pool maintenance.
// The caller remains responsible for deleting the Pod and cleaning its runtime.
func (b *Backend) TryRetireWarm(ctx context.Context, p *core.Pod) (bool, error) {
	if p == nil || !ValidID(string(p.UID)) {
		return false, fmt.Errorf("invalid warm Pod UID")
	}
	claimed := false
	_, err := warmUpdateContext(ctx, b.Base, string(p.UID), false, func(l *WarmLease) error {
		if l.Name != p.Name || l.Namespace != p.Namespace {
			return fmt.Errorf("warm Pod identity mismatch")
		}
		if l.Phase == "claimed" || l.Phase == "restoring" {
			claimed = true
			return nil
		}
		l.Phase = "retired"
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		// A gated Pod can be removed before RegisterWarm creates its lease.
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return !claimed, nil
}

func (b *Backend) ReapWarm(ctx context.Context, namespace string, existing map[string]bool) error {
	return b.ReapWarmWithRetirement(ctx, namespace, existing, b.TryRetireWarm)
}

// ReapWarmWithRetirement uses the controller's claim fence before touching an
// orphan. A missing API Pod alone does not prove that a local resume is idle.
func (b *Backend) ReapWarmWithRetirement(ctx context.Context, namespace string, existing map[string]bool, retire func(context.Context, *core.Pod) (bool, error)) error {
	entries, err := os.ReadDir(filepath.Join(b.Base, "warm"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var failures error
	for _, entry := range entries {
		if ctx.Err() != nil {
			return errors.Join(failures, ctx.Err())
		}
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(b.Base, "warm", entry.Name())
		var l WarmLease
		if err = ReadJSON(path, &l); err != nil {
			failures = errors.Join(failures, fmt.Errorf("read warm lease %s: %w", entry.Name(), err))
			continue
		}
		if l.Namespace != namespace || existing[l.UID] {
			continue
		}
		if !ValidID(l.UID) || entry.Name() != l.UID+".json" || l.Name == "" {
			failures = errors.Join(failures, fmt.Errorf("invalid warm lease identity: %s", entry.Name()))
			continue
		}
		p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: types.UID(l.UID), Name: l.Name, Namespace: l.Namespace}}
		retired, retireErr := retire(ctx, p)
		if retireErr != nil {
			failures = errors.Join(failures, fmt.Errorf("retire warm Pod %s: %w", l.Name, retireErr))
			continue
		}
		if !retired {
			continue
		}
		cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err = b.Cleanup(cleanupCtx, p)
		cancel()
		if err != nil {
			failures = errors.Join(failures, fmt.Errorf("reap warm Pod %s: %w", l.Name, err))
			continue
		}
		if err = os.Remove(path); err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		if err = os.Remove(filepath.Join(b.Base, "warm", l.UID+".lock")); err != nil && !errors.Is(err, os.ErrNotExist) {
			failures = errors.Join(failures, err)
		}
	}
	return failures
}
