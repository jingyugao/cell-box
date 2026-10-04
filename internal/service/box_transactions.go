package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// Refcounts include waiters, so deleting an idle lock cannot split writers.
type keyedLocks struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}
type keyedLock struct {
	sync.Mutex
	refs int
}

func (l *keyedLocks) lock(key string) func() {
	l.mu.Lock()
	if l.locks == nil {
		l.locks = map[string]*keyedLock{}
	}
	v := l.locks[key]
	if v == nil {
		v = &keyedLock{}
		l.locks[key] = v
	}
	v.refs++
	l.mu.Unlock()
	v.Lock()
	return func() {
		v.Unlock()
		l.mu.Lock()
		v.refs--
		if v.refs == 0 {
			delete(l.locks, key)
		}
		l.mu.Unlock()
	}
}

func (s *Store) lockBoxWriter(id string) func() {
	if s.records == nil {
		s.writeMu.Lock()
		return s.writeMu.Unlock
	}
	s.writeMu.RLock()
	unlock := s.boxWrites.lock(id)
	return func() { unlock(); s.writeMu.RUnlock() }
}

func boxState(st State, id string) State {
	out := newState()
	if b, ok := st.Boxes[id]; ok {
		out.Boxes[id] = b
	}
	for key, op := range st.Operations {
		if op.Operation.TargetID == id {
			out.Operations[key] = op
		}
	}
	for key, value := range st.Keys {
		if _, ok := out.Operations[value.OperationID]; ok {
			out.Keys[key] = value
		}
	}
	for key, value := range st.Executions {
		if value.BoxID == id {
			out.Executions[key] = value
		}
	}
	for key, value := range st.Leases {
		if value.BoxID == id {
			out.Leases[key] = value
		}
	}
	return out
}

func replaceRecords[T any](dst, before, after map[string]T) {
	for key := range before {
		delete(dst, key)
	}
	for key, value := range after {
		dst[key] = value
	}
}

// UpdateBox commits the box, its operation and its idempotency key in one CAS.
// Cross-resource writers hold writeMu exclusively; different boxes can commit
// in parallel. This entry point is for existing-box lifecycle operations only.
func (s *Store) UpdateBox(id string, fn func(*State) error) error {
	if s.records == nil {
		return s.Update(fn)
	}
	unlock := s.lockBoxWriter(id)
	defer unlock()
	s.mu.Lock()
	if s.failure != nil {
		err := s.failure
		s.mu.Unlock()
		return err
	}
	before := boxState(s.state, id)
	expired := map[string]keyRecord{}
	for key, value := range s.state.Keys {
		if value.Expired {
			expired[key] = value
		}
	}
	if _, ok := before.Boxes[id]; !ok {
		s.mu.Unlock()
		return apiError("NOT_FOUND", "Box not found")
	}
	next, err := cloneState(before)
	key := recordKey("boxes", id)
	etag, previous, parent := s.records.etags[key], s.records.data[key], s.records.headRevision
	s.mu.Unlock()
	if err != nil {
		return err
	}
	for key, value := range expired {
		next.Keys[key] = value
	}
	if err = fn(&next); err != nil {
		return err
	}
	for key, value := range expired {
		if next.Keys[key] != value {
			return errors.New("box transaction changed an expired idempotency fence")
		}
		delete(next.Keys, key)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	projected, err := s.projectRecords(ctx, &next)
	if err != nil {
		return err
	}
	data, ok := projected[key]
	if !ok || len(projected) != 1 || etag == "" {
		return errors.New("box transaction crossed resource boundaries")
	}
	if !bytes.Equal(previous, data) {
		var durable State
		if err = json.Unmarshal(data, &durable); err != nil {
			return err
		}
		encoded, err := json.Marshal(resourceRecords{Revision: randomID("rev-"), ParentRevision: parent, State: &durable})
		if err != nil {
			return err
		}
		etag, err = writeObject(ctx, s.objects, key, encoded, etag)
		if err != nil {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.failRemote()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure != nil {
		return s.failure
	}
	replaceRecords(s.state.Boxes, before.Boxes, next.Boxes)
	replaceRecords(s.state.Operations, before.Operations, next.Operations)
	replaceRecords(s.state.Keys, before.Keys, next.Keys)
	replaceRecords(s.state.Executions, before.Executions, next.Executions)
	replaceRecords(s.state.Leases, before.Leases, next.Leases)
	s.records.etags[key], s.records.data[key] = etag, data
	s.notifyLocked()
	return nil
}

// Taking over the global head fences cross-resource writers. Rotate every live
// box revision too, before serving requests, to fence old box-local writers.
func (s *Store) claimBoxRecords(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	for key, data := range s.records.data {
		if !strings.HasPrefix(key, recordPrefix+"/boxes/") {
			continue
		}
		group.Go(func() error {
			var st State
			if err := json.Unmarshal(data, &st); err != nil {
				return err
			}
			encoded, err := json.Marshal(resourceRecords{Revision: randomID("rev-"), ParentRevision: s.records.headRevision, State: &st})
			if err != nil {
				return err
			}
			s.mu.Lock()
			etag := s.records.etags[key]
			s.mu.Unlock()
			etag, err = writeObject(ctx, s.objects, key, encoded, etag)
			if err != nil {
				return err
			}
			s.mu.Lock()
			s.records.etags[key] = etag
			s.mu.Unlock()
			return nil
		})
	}
	return group.Wait()
}
