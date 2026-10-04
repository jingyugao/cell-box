package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	backend "cellbox.local/cellbox/internal/runtime"
	"golang.org/x/sys/unix"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// WarmLease is an execution lease, not a running or ready business container.
// Its deadline stays below the kubelet RunPodSandbox timeout. Expired Pod UIDs
// are never rearmed; the pool replaces the entire Pod.
type WarmLease struct {
	UID, Name, Namespace, SpecHash, SID, Phase string
	Deadline                                   time.Time
	Ticket                                     Ticket
}

func warmUpdate(base, uid string, create bool, fn func(*WarmLease) error) (WarmLease, error) {
	var l WarmLease
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
	if before != l {
		err = AtomicJSON(path, l)
	}
	return l, err
}

func (b *Backend) RegisterWarm(_ context.Context, p *core.Pod, hash string) error {
	_, err := warmUpdate(b.Base, string(p.UID), true, func(l *WarmLease) error {
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

func (b *Backend) WarmStatus(_ context.Context, p *core.Pod) (WarmLease, error) {
	return warmUpdate(b.Base, string(p.UID), false, func(l *WarmLease) error {
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

func WaitWarm(base, uid, sid string) (Ticket, error) {
	_, err := warmUpdate(base, uid, false, func(l *WarmLease) error {
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
				if live != l.Ticket {
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
			time.Sleep(10 * time.Millisecond)
		default:
			return Ticket{}, fmt.Errorf("warm slot %s", l.Phase)
		}
	}
}

func (b *Backend) ActivateWarm(ctx context.Context, w *api.ResumablePod, p *core.Pod) (bool, error) {
	l, err := b.WarmStatus(ctx, p)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	t := Ticket{OwnerUID: string(w.UID), PodUID: string(p.UID), Namespace: p.Namespace, PodName: p.Name, Snapshot: w.Status.Snapshot, SpecHash: w.Status.SpecHash}
	if l.Phase == "claimed" || l.Phase == "restoring" {
		if l.Ticket != t {
			return true, fmt.Errorf("warm slot assigned to another execution")
		}
		return true, nil
	}
	if l.Phase == "expired" || (l.Phase == "waiting" && !time.Now().Before(l.Deadline)) {
		return true, backend.ErrWarmExpired
	}
	if l.SpecHash != w.Status.SpecHash || t.Snapshot == "" {
		return true, fmt.Errorf("incompatible warm restore")
	}
	if err = b.prepareSnapshot(ctx, w); err != nil {
		return true, err
	}
	_, err = warmUpdate(b.Base, string(p.UID), false, func(l *WarmLease) error {
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
	Ticket    Ticket
	SandboxID string
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
func (b *Backend) ReapWarm(ctx context.Context, namespace string, existing map[string]bool) error {
	entries, err := os.ReadDir(filepath.Join(b.Base, "warm"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(b.Base, "warm", entry.Name())
		var l WarmLease
		if err = ReadJSON(path, &l); err != nil {
			return err
		}
		if l.Namespace != namespace || existing[l.UID] {
			continue
		}
		p := &core.Pod{ObjectMeta: meta.ObjectMeta{UID: types.UID(l.UID), Name: l.Name, Namespace: l.Namespace}}
		if err = b.Cleanup(ctx, p); err != nil {
			return err
		}
		if err = os.Remove(path); err != nil {
			return err
		}
		if err = os.Remove(filepath.Join(b.Base, "warm", l.UID+".lock")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
