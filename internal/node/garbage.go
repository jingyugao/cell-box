package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	"cellbox.local/cellbox/internal/inventory"
	"cellbox.local/cellbox/internal/objectstorage"
	"golang.org/x/sync/errgroup"
)

type snapshotGarbage struct{ Owner, Snapshot, IndexETag string }

func (b *Backend) snapshotConsumed(owner, snapshot string) bool {
	base := filepath.Join(b.Base, "garbage", owner, snapshot)
	for _, suffix := range []string{".intent", ".json", ".done"} {
		if _, err := os.Stat(base + suffix); err == nil {
			return true
		}
	}
	return false
}

// InvalidateSnapshot removes replay eligibility synchronously. The durable
// local marker also prevents an interrupted CR transition republishing it.
// Only exact-version physical deletion is left to the background collector.
func (b *Backend) InvalidateSnapshot(ctx context.Context, w *api.ResumablePod) error {
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
		if b.Objects != nil {
			b.inventoryMu.Lock()
			data, version := []byte(b.inventoryState[owner]), b.inventoryVersions[owner]
			b.inventoryMu.Unlock()
			if version == "" {
				var err error
				data, version, err = readInventoryObject(ctx, b.Objects, "checkpoints/"+owner+"/metadata.json")
				if err != nil && !errors.Is(err, objectstorage.ErrNotFound) {
					return retryableStorage(err)
				}
			}
			if version != "" {
				var record inventory.Record
				if json.Unmarshal(data, &record) != nil || record.RuntimeID != owner || record.Snapshot != snapshot {
					return fmt.Errorf("refusing to invalidate another checkpoint's inventory")
				}
				job.IndexETag = version
			}
		}
		if err := AtomicJSON(path, job); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if job.Owner != owner || job.Snapshot != snapshot {
		return fmt.Errorf("invalid snapshot invalidation record")
	}
	if b.Objects != nil && job.IndexETag != "" {
		if err := b.Objects.DeleteIfMatch(ctx, "checkpoints/"+owner+"/metadata.json", job.IndexETag); err != nil && !errors.Is(err, objectstorage.ErrNotFound) {
			return retryableStorage(err)
		}
	}
	b.clearInventoryState(owner)
	if err := os.Rename(path, strings.TrimSuffix(path, ".intent")+".json"); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else if err := SyncDir(filepath.Dir(path)); err != nil {
		return err
	}
	return nil
}

// CollectGarbage never removes an owner prefix or inventory index: a newer
// checkpoint may already exist when an old cleanup job is retried.
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
				if b.Objects != nil {
					key, _ := checkpointObjectKey(job.Owner, job.Snapshot)
					if err := b.Objects.Delete(ctx, key); err != nil && !errors.Is(err, objectstorage.ErrNotFound) {
						return err
					}
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
				b.durableMu.Lock()
				delete(b.durableSnapshots, job.Owner+"/"+job.Snapshot)
				b.durableMu.Unlock()
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
