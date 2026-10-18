package service

import (
	"cellbox.local/cellbox/internal/boxprovider"
	"time"
)

func (s *Service) startRuntimeObservers() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			s.ensureRuntimeObservers()
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}
func (s *Service) ensureRuntimeObservers() {
	if s.ctx.Err() != nil {
		return
	}
	records, err := s.store.RuntimeBoxRecords()
	if err != nil {
		return
	}
	for _, b := range records {
		if _, ok := s.providers[b.Profile.Provider].(boxprovider.CompletionObservationProvider); ok {
			s.ensureRuntimeFeed(b)
		}
	}
}
