package service

import (
	"context"
	"errors"
	"time"

	"cellbox.local/cellbox/internal/boxprovider"
)

// One stream per Box serves both background projection and lifecycle waiters.
// Intermediate resume observations stay in memory; the operation commits the
// final execution together with its terminal result.
type runtimeFeed struct {
	provider    boxprovider.CompletionObservationProvider
	handle      boxprovider.Handle
	ready       chan struct{}
	changed     chan struct{}
	observation boxprovider.Observation
	observed    bool
}

func (s *Service) ensureRuntimeFeed(b boxRecord) *runtimeFeed {
	s.observerMu.Lock()
	defer s.observerMu.Unlock()
	if s.runtimeFeeds == nil {
		s.runtimeFeeds = make(map[string]*runtimeFeed)
	}
	if feed := s.runtimeFeeds[b.Box.ID]; feed != nil && feed.handle == b.Handle {
		return feed
	}
	feed := &runtimeFeed{handle: b.Handle, ready: make(chan struct{}), changed: make(chan struct{})}
	feed.provider = s.providers[b.Profile.Provider].(boxprovider.CompletionObservationProvider)
	s.runtimeFeeds[b.Box.ID] = feed
	s.wg.Add(1)
	go s.runRuntimeFeed(b.Box.ID, feed)
	return feed
}

func (s *Service) feedSnapshot(feed *runtimeFeed) (boxprovider.Observation, bool, <-chan struct{}) {
	s.observerMu.Lock()
	defer s.observerMu.Unlock()
	return feed.observation, feed.observed, feed.changed
}

func (s *Service) publishRuntimeFeed(id string, feed *runtimeFeed, observation boxprovider.Observation) bool {
	s.observerMu.Lock()
	defer s.observerMu.Unlock()
	if s.runtimeFeeds[id] != feed {
		return false
	}
	feed.observation = observation
	if !feed.observed {
		feed.observed = true
		close(feed.ready)
	}
	close(feed.changed)
	feed.changed = make(chan struct{})
	return true
}

func (s *Service) runRuntimeFeed(id string, feed *runtimeFeed) {
	defer s.wg.Done()
	defer func() {
		s.observerMu.Lock()
		defer s.observerMu.Unlock()
		if s.runtimeFeeds[id] == feed {
			delete(s.runtimeFeeds, id)
		}
	}()
	provider := feed.provider
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		b, err := s.rawBox(id)
		if err != nil || b.Handle != feed.handle || b.Box.Phase == "deleted" {
			return
		}
		stream, err := provider.WatchObservations(ctx, feed.handle)
		if err != nil {
			if errors.Is(err, boxprovider.ErrNotFound) {
				observation := boxprovider.Observation{Phase: "deleted"}
				s.publishRuntimeFeed(id, feed, observation)
				_, _ = s.applyRuntimeObservation(ctx, b, observation, true)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 5*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
		for {
			select {
			case <-ctx.Done():
				return
			case observation, ok := <-stream:
				if !ok {
					goto reconnect
				}
				b, err = s.rawBox(id)
				if err != nil || b.Handle != feed.handle || b.Box.Phase == "deleted" {
					return
				}
				if !s.publishRuntimeFeed(id, feed, observation) {
					return
				}
				if b.Box.OperationID != "" {
					op, opErr := s.store.Operation(b.ClientID, b.Box.OperationID)
					if opErr == nil && resumeOperationKind(op.Kind) && (op.Status == "running" || op.Status == "queued") {
						continue
					}
				}
				projected, projectionErr := s.applyRuntimeObservation(ctx, b, observation, observation.Phase == "deleted")
				if projectionErr == nil && projected.Phase == "staged" {
					s.releaseFailedUpgrade(id)
				}
			}
		}
	reconnect:
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

func (s *Service) cachedRuntimeObservation(ctx context.Context, b boxRecord) (boxprovider.Observation, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.config.StartupTimeoutSeconds)*time.Second)
	defer cancel()
	feed := s.ensureRuntimeFeed(b)
	select {
	case <-ctx.Done():
		return boxprovider.Observation{}, ctx.Err()
	case <-s.ctx.Done():
		return boxprovider.Observation{}, s.ctx.Err()
	case <-feed.ready:
	}
	observation, _, _ := s.feedSnapshot(feed)
	return observation, nil
}

func (s *Service) waitNodeResume(ctx context.Context, b boxRecord, transitionID string) (boxprovider.Observation, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(s.config.StartupTimeoutSeconds)*time.Second)
	defer cancel()
	feed := s.ensureRuntimeFeed(b)
	for {
		observation, observed, changed := s.feedSnapshot(feed)
		current, err := s.rawBox(b.Box.ID)
		if err != nil {
			return boxprovider.Observation{}, err
		}
		if current.Box.OperationID != b.Box.OperationID || current.Handle != b.Handle {
			return boxprovider.Observation{}, boxprovider.ErrStaleTransition
		}
		if observed && observation.Phase == "deleted" {
			return boxprovider.Observation{}, boxprovider.ErrNotFound
		}
		if observed && observation.TransitionID == transitionID && observation.ObservedTransitionID == transitionID {
			if observation.Phase == "failed" || observation.Phase == "deleted" {
				return boxprovider.Observation{}, apiError("READINESS_FAILED", "Runtime entered failed state")
			}
			if observation.Phase == "running" && observation.GuestReady && observation.ExecutionID != "" {
				if b.ResumeExecutionID != "" && b.ResumeExecutionID != observation.ExecutionID {
					return boxprovider.Observation{}, boxprovider.ErrStaleExecution
				}
				return observation, nil
			}
		}
		select {
		case <-ctx.Done():
			return boxprovider.Observation{}, ctx.Err()
		case <-s.ctx.Done():
			return boxprovider.Observation{}, s.ctx.Err()
		case <-changed:
		}
	}
}
