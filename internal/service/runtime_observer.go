package service

import (
	"context"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
)

const runtimeResyncInterval = 30 * time.Second

func (s *Service) startRuntimeObservers() {
	s.observerMu.Lock()
	if s.observers == nil {
		s.observers = make(map[string]bool)
	}
	s.observerMu.Unlock()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		sync := time.NewTicker(5 * time.Second)
		defer sync.Stop()
		for {
			s.ensureRuntimeObservers()
			select {
			case <-s.ctx.Done():
				return
			case <-sync.C:
			}
		}
	}()
}

func (s *Service) ensureRuntimeObservers() {
	if s.ctx.Err() != nil {
		return
	}
	boxes, err := s.store.RuntimeBoxRecords()
	if err != nil {
		return
	}
	for _, b := range boxes {
		if _, ok := s.providers[b.Profile.Provider].(boxprovider.CompletionObservationProvider); ok && (b.Upgrade == nil || b.Upgrade.RuntimeCommitted) {
			s.ensureRuntimeFeed(b)
			continue
		}
		s.observerMu.Lock()
		if s.observers[b.Box.ID] || s.ctx.Err() != nil {
			s.observerMu.Unlock()
			continue
		}
		s.observers[b.Box.ID] = true
		s.wg.Add(1)
		s.observerMu.Unlock()
		go s.observeBoxLoop(b.Box.ID)
	}
}

func (s *Service) observeBoxLoop(id string) {
	defer s.wg.Done()
	defer func() {
		s.observerMu.Lock()
		delete(s.observers, id)
		s.observerMu.Unlock()
	}()
	resync := time.NewTicker(runtimeResyncInterval)
	defer resync.Stop()
	var changes <-chan struct{}
	var stopWatch context.CancelFunc
	var watchedHandle boxprovider.Handle
	defer func() {
		if stopWatch != nil {
			stopWatch()
		}
	}()
	backoff := 250 * time.Millisecond
	upgradeBackoff := time.Second
	for s.ctx.Err() == nil {
		b, err := s.rawBox(id)
		if err != nil || b.Handle.ID == "" || b.Box.Phase == "deleted" {
			return
		}
		if _, ok := s.providers[b.Profile.Provider].(boxprovider.CompletionObservationProvider); ok && (b.Upgrade == nil || b.Upgrade.RuntimeCommitted) {
			s.ensureRuntimeFeed(b)
			return
		}
		if b.Upgrade != nil && !b.Upgrade.RuntimeCommitted {
			nonce := b.Upgrade.Nonce
			ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
			_, _ = s.commitDiskUpgrade(ctx, b)
			cancel()
			b, err = s.rawBox(id)
			if err != nil {
				return
			}
			if b.Upgrade != nil && b.Upgrade.Nonce == nonce {
				// A failed or not-yet-observed upgrade is still owned by this
				// operation. Back off instead of hammering the provider/API.
				timer := time.NewTimer(upgradeBackoff)
				select {
				case <-s.ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
				upgradeBackoff = min(upgradeBackoff*2, 30*time.Second)
				continue
			}
			upgradeBackoff = time.Second
			// Upgrade may replace the runtime handle. Never continue inspecting or
			// watching the stale snapshot that carried the intent.
			continue
		}
		if stopWatch != nil && watchedHandle != b.Handle {
			stopWatch()
			stopWatch, changes = nil, nil
		}
		provider, hasProvider := s.providers[b.Profile.Provider]
		if !hasProvider {
			return
		}
		if changes == nil {
			changeProvider, ok := provider.(boxprovider.ChangeProvider)
			if ok {
				watchCtx, watchCancel := context.WithCancel(s.ctx)
				stream, watchErr := changeProvider.WatchChanges(watchCtx, b.Handle)
				if watchErr == nil && stream != nil {
					changes, stopWatch, watchedHandle = stream, watchCancel, b.Handle
					backoff = 250 * time.Millisecond
				} else {
					watchCancel()
					backoff = min(backoff*2, 5*time.Second)
				}
			}
		}
		// Subscribe before observing so changes in the watch/inspect gap are
		// delivered as notifications instead of waiting for the periodic resync.
		ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
		observed, observeErr := s.observe(ctx, b)
		cancel()
		if observeErr != nil && s.ctx.Err() != nil {
			return
		}
		if observeErr == nil && observed.Phase == "staged" {
			s.releaseFailedUpgrade(id)
		}
		if changes == nil {
			wait := backoff
			if _, ok := provider.(boxprovider.ChangeProvider); !ok {
				wait = runtimeResyncInterval
			}
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(wait):
			}
			continue
		}
		select {
		case <-s.ctx.Done():
			return
		case _, ok := <-changes:
			if !ok {
				changes = nil
				if stopWatch != nil {
					stopWatch()
					stopWatch = nil
				}
			}
		case <-resync.C:
		}
	}
}

// A failed upgrade may have an unknown provider outcome: the CR patch can have
// succeeded even when the API response was lost. Preserve the exact nonce as
// the Box owner until an observer verifies the replacement is staged, then
// release the Box while retaining the failed operation as history.
func (s *Service) releaseFailedUpgrade(id string) {
	b, err := s.rawBox(id)
	if err != nil || b.Upgrade == nil || !b.Upgrade.RuntimeCommitted || b.Upgrade.Nonce == "" || b.Box.OperationID != b.Upgrade.Nonce {
		return
	}
	op, err := s.store.Operation(b.ClientID, b.Upgrade.Nonce)
	if err != nil || op.Status != "failed" {
		return
	}
	_ = s.store.UpdateBoxContext(s.ctx, id, func(st *State) error {
		current, ok := st.Boxes[id]
		if !ok || current.Upgrade == nil || current.Upgrade.Nonce != b.Upgrade.Nonce ||
			!current.Upgrade.RuntimeCommitted || current.Box.OperationID != b.Upgrade.Nonce ||
			current.Box.Phase != "staged" {
			return nil
		}
		current.Box.OperationID = ""
		current.UpgradeImageID = ""
		current.Upgrade = nil
		current.Box.Version++
		st.Boxes[id] = current
		return nil
	})
}
