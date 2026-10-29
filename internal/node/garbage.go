package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/inventory"
	"github.com/jingyugao/cell-box/internal/telemetry"
	"golang.org/x/sync/errgroup"
)

type snapshotGarbage struct{ Owner, Snapshot string }

func (b *Backend) snapshotConsumed(owner, snapshot string) bool {
	base := filepath.Join(b.Base, "garbage", owner, snapshot)
	for _, suffix := range []string{".intent", ".json", ".done"} {
		if _, err := os.Stat(base + suffix); err == nil {
			return true
		}
	}
	if ValidID(owner) && ValidID(snapshot) {
		if _, err := os.Stat(filepath.Join(b.Base, "workloads", owner, "consumed", snapshot)); err == nil {
			return true
		}
	}
	return false
}

// ConsumeSnapshot permanently fences a sandbox checkpoint before runsc restore
// is launched. The marker is independent of any mounted volume and is removed
// with the owning Box's workloads directory.
func ConsumeSnapshot(base, owner, snapshot string) error {
	if !ValidID(owner) || !ValidID(snapshot) {
		return fmt.Errorf("invalid snapshot consumption identity")
	}
	dir := filepath.Join(base, "workloads", owner, "consumed")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	path := filepath.Join(dir, snapshot)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if os.IsExist(err) {
		return fmt.Errorf("snapshot already consumed")
	}
	if err != nil {
		return err
	}
	_, writeErr := fmt.Fprintf(f, "%s\n%s\n", owner, snapshot)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	return SyncDir(dir)
}

// InvalidateSnapshot removes replay eligibility synchronously. The durable
// local marker also prevents an interrupted CR transition republishing it.
// Only exact-version physical deletion is left to the background collector.
func (b *Backend) InvalidateSnapshot(ctx context.Context, w *api.ResumablePod) (resultErr error) {
	ctx, span := telemetry.Start(ctx, "node.snapshot.invalidate")
	defer func() { telemetry.End(span, resultErr) }()
	owner, snapshot := string(w.UID), w.Status.Snapshot
	if !ValidID(owner) || !ValidID(snapshot) {
		return fmt.Errorf("invalid snapshot invalidation identity")
	}
	base := filepath.Join(b.Base, "garbage", owner, snapshot)
	for _, suffix := range []string{".json", ".done"} {
		if _, err := os.Stat(base + suffix); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	path := base + ".intent"
	var job snapshotGarbage
	if err := ReadJSON(path, &job); os.IsNotExist(err) {
		job = snapshotGarbage{Owner: owner, Snapshot: snapshot}
		if err := AtomicJSON(path, job); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if job.Owner != owner || job.Snapshot != snapshot {
		return fmt.Errorf("invalid snapshot invalidation record")
	}
	if b.Checkpoints != nil {
		if err := b.Checkpoints.DeleteCheckpoint(ctx, owner, snapshot); err != nil && !errors.Is(err, inventory.ErrNotFound) {
			return err
		}
	}
	if err := os.Rename(path, strings.TrimSuffix(path, ".intent")+".json"); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else if err := SyncDir(filepath.Dir(path)); err != nil {
		return err
	}
	return nil
}

// CollectGarbage removes only local checkpoint files after replay eligibility
// has been invalidated in the persistent inventory store.
func (b *Backend) CollectGarbage(ctx context.Context) error {
	root := filepath.Join(b.Base, "garbage")
	owners, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	for _, owner := range owners {
		if !owner.IsDir() || !ValidID(owner.Name()) {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(root, owner.Name()))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			path := filepath.Join(root, owner.Name(), entry.Name())
			group.Go(func() error {
				var job snapshotGarbage
				if err := ReadJSON(path, &job); err != nil {
					if os.IsNotExist(err) {
						return nil
					}
					return err
				}
				if !ValidID(job.Owner) || !ValidID(job.Snapshot) || job.Owner != owner.Name() || entry.Name() != job.Snapshot+".json" {
					return fmt.Errorf("invalid snapshot cleanup record")
				}
				cache, err := SnapshotPath(b.Base, job.Owner, job.Snapshot)
				if err != nil {
					return err
				}
				if err = os.RemoveAll(cache); err != nil {
					return err
				}
				if err = os.Remove(cache + ".verified.json"); err != nil && !os.IsNotExist(err) {
					return err
				}
				if err = os.Rename(path, strings.TrimSuffix(path, ".json")+".done"); err != nil {
					if os.IsNotExist(err) {
						return nil
					}
					return err
				}
				return SyncDir(filepath.Dir(path))
			})
		}
	}
	return group.Wait()
}

type GarbageCollector struct{ Backend *Backend }

func (*GarbageCollector) NeedLeaderElection() bool { return true }
func (g *GarbageCollector) Start(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := g.Backend.CollectGarbage(ctx); err != nil && ctx.Err() == nil {
			fmt.Printf("checkpoint cleanup retry: %v\n", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
