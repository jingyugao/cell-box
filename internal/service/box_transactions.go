package service

import (
	"sync"
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
