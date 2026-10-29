// Package runtimecontrol connects API and lifecycle workers inside one process.
// Warm resumes commit locally before asynchronous Kubernetes publication.
package runtimecontrol

import (
	"context"
	"hash/fnv"
	"sync"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Snapshot struct {
	Resource *api.ResumablePod
	Deleted  bool
}

// Client persists first, then directly queues reconciliation and publishes the
// committed snapshot. The same instance MUST be used by API and reconciler.
// Sharded locks order reads, writes and notifications for each resource without
// retaining a lock for every sandbox ever created. Runtime work itself uses the
// controller's keyed queue, so different sandboxes can run concurrently.
type Client struct {
	client.Client
	locks    [64]sync.Mutex
	mu       sync.Mutex
	queue    workqueue.TypedRateLimitingInterface[reconcile.Request]
	pending  map[types.NamespacedName]struct{}
	subs     map[types.NamespacedName]map[chan Snapshot]snapshotVersion
	journal  *Journal
	syncMu   sync.Mutex
	syncWake chan struct{}
}

type snapshotVersion struct {
	uid                  types.UID
	revision             string
	deleted, initialized bool
}

func New(c client.Client) *Client {
	return &Client{Client: c, pending: map[types.NamespacedName]struct{}{}, subs: map[types.NamespacedName]map[chan Snapshot]snapshotVersion{}, syncWake: make(chan struct{}, 1)}
}

// Start implements controller-runtime's Source. Calls go straight to its keyed,
// deduplicating queue; there is no intermediary channel that can overflow.
func (c *Client) Start(_ context.Context, q workqueue.TypedRateLimitingInterface[reconcile.Request]) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queue = q
	for key := range c.pending {
		q.Add(reconcile.Request{NamespacedName: key})
	}
	c.pending = nil
	return nil
}

func (c *Client) Notify(key types.NamespacedName) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.queue != nil {
		c.queue.Add(reconcile.Request{NamespacedName: key})
		return
	}
	c.pending[key] = struct{}{}
}

// Subscribe registers before the initial read. Both the read and all local
// writes publish under the same per-key lock: no subscribe/read gap, no late
// initial snapshot overwriting a newer completion, no resourceVersion ordering
// assumptions. Slow consumers receive the newest complete snapshot.
func (c *Client) Subscribe(ctx context.Context, key types.NamespacedName) (<-chan Snapshot, func(), error) {
	ch := make(chan Snapshot, 1)
	c.mu.Lock()
	if c.subs[key] == nil {
		c.subs[key] = map[chan Snapshot]snapshotVersion{}
	}
	c.subs[key][ch] = snapshotVersion{}
	c.mu.Unlock()
	stop := func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.subs[key], ch)
		if len(c.subs[key]) == 0 {
			delete(c.subs, key)
		}
	}
	w := &api.ResumablePod{}
	if err := c.initialSnapshot(ctx, key, w); err != nil {
		stop()
		return nil, nil, err
	}
	return ch, stop, nil
}

func (c *Client) lock(key types.NamespacedName) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key.Namespace + "/" + key.Name))
	m := &c.locks[h.Sum32()%uint32(len(c.locks))]
	m.Lock()
	return m.Unlock
}

func (c *Client) publish(key types.NamespacedName, w *api.ResumablePod, deleted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	version := snapshotVersion{deleted: deleted, initialized: true}
	if w != nil {
		version.uid = w.UID
		version.revision = w.ResourceVersion
	}
	for ch, previous := range c.subs[key] {
		if previous == version && (version.revision != "" || deleted) {
			continue
		}
		c.subs[key][ch] = version
		var copy *api.ResumablePod
		if w != nil {
			copy = w.DeepCopy()
		}
		select {
		case <-ch:
		default:
		}
		ch <- Snapshot{Resource: copy, Deleted: deleted}
	}
}

func (c *Client) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	w, ok := obj.(*api.ResumablePod)
	if !ok {
		err := c.Client.Get(ctx, key, obj, opts...)
		if err == nil {
			if pod, ok := obj.(*core.Pod); ok {
				return c.projectPod(ctx, pod)
			}
		}
		return err
	}
	defer c.lock(key)()
	if found, err := c.localGet(ctx, key, obj); found || err != nil {
		return err
	}
	err := c.Client.Get(ctx, key, obj, opts...)
	if err == nil {
		// A journal can be installed during initial observer startup.
		if found, err := c.localGet(ctx, key, obj); found || err != nil {
			return err
		}
		if err := c.remember(ctx, w); err != nil {
			return err
		}
		c.publish(key, w, false)
	} else if apierrors.IsNotFound(err) {
		if journal := c.getJournal(); journal != nil {
			record, loadErr := journal.Load(ctx, key)
			if loadErr != nil {
				return loadErr
			}
			if record != nil && !record.Pending {
				if deleteErr := journal.Delete(ctx, key, record.Revision); deleteErr != nil {
					return deleteErr
				}
			}
		}
		c.publish(key, nil, true)
	}
	return err
}

func (c *Client) committed(w *api.ResumablePod, previousVersion string) {
	key := client.ObjectKeyFromObject(w)
	// Removing the final finalizer deletes the CR in the same API transaction.
	deleted := w.DeletionTimestamp != nil && len(w.Finalizers) == 0
	c.publish(key, w, deleted)
	if w.ResourceVersion != previousVersion || deleted {
		c.Notify(key)
	}
}

func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	w, ok := obj.(*api.ResumablePod)
	if !ok {
		return c.Client.Create(ctx, obj, opts...)
	}
	defer c.lock(client.ObjectKeyFromObject(w))()
	err := c.Client.Create(ctx, obj, opts...)
	if err == nil {
		if err := c.remember(ctx, w); err != nil {
			return err
		}
		c.committed(w, "")
	}
	return err
}

func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if handled, err := c.localUpdate(ctx, obj); handled || err != nil {
		return err
	}
	w, ok := obj.(*api.ResumablePod)
	if !ok {
		return c.Client.Update(ctx, obj, opts...)
	}
	defer c.lock(client.ObjectKeyFromObject(w))()
	if record, err := c.LocalRecord(ctx, client.ObjectKeyFromObject(w)); record != nil || err != nil {
		if err != nil {
			return err
		}
		return ErrJournalConflict
	}
	before := w.ResourceVersion
	err := c.Client.Update(ctx, obj, opts...)
	if err == nil {
		if err := c.remember(ctx, w); err != nil {
			return err
		}
		c.committed(w, before)
	}
	return err
}

func (c *Client) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if handled, err := c.localPatch(ctx, obj, patch); handled || err != nil {
		return err
	}
	w, ok := obj.(*api.ResumablePod)
	if !ok {
		return c.Client.Patch(ctx, obj, patch, opts...)
	}
	defer c.lock(client.ObjectKeyFromObject(w))()
	if record, err := c.LocalRecord(ctx, client.ObjectKeyFromObject(w)); record != nil || err != nil {
		if err != nil {
			return err
		}
		return ErrJournalConflict
	}
	before := w.ResourceVersion
	err := c.Client.Patch(ctx, obj, patch, opts...)
	if err == nil {
		if err := c.remember(ctx, w); err != nil {
			return err
		}
		c.committed(w, before)
	}
	return err
}

func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	w, ok := obj.(*api.ResumablePod)
	if !ok {
		return c.Client.Delete(ctx, obj, opts...)
	}
	key := client.ObjectKeyFromObject(w)
	defer c.lock(key)()
	err := c.Client.Delete(ctx, obj, opts...)
	// Delete need not return the finalizer-held resource. Reconciliation reads
	// its committed state and publishes deletion only after cleanup completes.
	if err == nil || apierrors.IsNotFound(err) {
		c.Notify(key)
	}
	return err
}

func (c *Client) Status() client.SubResourceWriter {
	return &statusWriter{SubResourceWriter: c.Client.Status(), parent: c}
}

type statusWriter struct {
	client.SubResourceWriter
	parent *Client
}

func (s *statusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if handled, err := s.parent.localStatus(ctx, obj); handled || err != nil {
		return err
	}
	w, ok := obj.(*api.ResumablePod)
	if !ok {
		return s.SubResourceWriter.Update(ctx, obj, opts...)
	}
	defer s.parent.lock(client.ObjectKeyFromObject(w))()
	if record, err := s.parent.LocalRecord(ctx, client.ObjectKeyFromObject(w)); record != nil || err != nil {
		if err != nil {
			return err
		}
		return ErrJournalConflict
	}
	before := w.ResourceVersion
	err := s.SubResourceWriter.Update(ctx, obj, opts...)
	if err == nil {
		if err := s.parent.remember(ctx, w); err != nil {
			return err
		}
		s.parent.committed(w, before)
	}
	return err
}

func (s *statusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	w, ok := obj.(*api.ResumablePod)
	if !ok {
		return s.SubResourceWriter.Patch(ctx, obj, patch, opts...)
	}
	defer s.parent.lock(client.ObjectKeyFromObject(w))()
	if record, err := s.parent.LocalRecord(ctx, client.ObjectKeyFromObject(w)); record != nil || err != nil {
		if err != nil {
			return err
		}
		return ErrJournalConflict
	}
	before := w.ResourceVersion
	err := s.SubResourceWriter.Patch(ctx, obj, patch, opts...)
	if err == nil {
		if err := s.parent.remember(ctx, w); err != nil {
			return err
		}
		s.parent.committed(w, before)
	}
	return err
}
