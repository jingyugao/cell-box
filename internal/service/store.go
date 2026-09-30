package service

import (
	"encoding/json"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	mu    sync.Mutex
	state State
	dir   string
	lock  *os.File
}

func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("data directory must be a real directory")
	}
	if err = os.Chmod(dir, 0700); err != nil {
		return nil, err
	}
	fd, err := unix.Open(filepath.Join(dir, "service.lock"), unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "service.lock")
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another Cellbox service owns this data directory: %w", err)
	}
	s := &Store{dir: dir, lock: f, state: newState()}
	name := filepath.Join(dir, "state.json")
	if info, e := os.Lstat(name); e == nil && !info.Mode().IsRegular() {
		s.Close()
		return nil, fmt.Errorf("state file must be a regular file")
	}
	b, err := os.ReadFile(name)
	if err != nil && !os.IsNotExist(err) {
		s.Close()
		return nil, err
	}
	if err == nil {
		if err = json.Unmarshal(b, &s.state); err != nil {
			s.Close()
			return nil, fmt.Errorf("invalid state file: %w", err)
		}
		if s.state.Schema != 2 {
			s.Close()
			return nil, fmt.Errorf("unsupported state schema")
		}
		if s.state.Boxes == nil || s.state.Operations == nil || s.state.Keys == nil || s.state.Executions == nil || s.state.Leases == nil || s.state.Routes == nil || s.state.Grants == nil || s.state.Access == nil || s.state.Sessions == nil || s.state.Archives == nil {
			s.Close()
			return nil, fmt.Errorf("state contains null collections")
		}
	}
	return s, nil
}

var errBoxObservationConflict = fmt.Errorf("box changed during observation")

func cloneBoxRecord(record boxRecord) (boxRecord, error) {
	b, err := json.Marshal(record)
	if err != nil {
		return boxRecord{}, err
	}
	var copy boxRecord
	err = json.Unmarshal(b, &copy)
	return copy, err
}

// BoxRecords returns isolated copies of only the requested records. An empty
// ID list means all records owned by client.
func (s *Store) BoxRecords(client string, ids []string) ([]boxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(ids) == 0 {
		out := make([]boxRecord, 0)
		for _, record := range s.state.Boxes {
			if record.ClientID == client {
				copy, err := cloneBoxRecord(record)
				if err != nil {
					return nil, err
				}
				out = append(out, copy)
			}
		}
		return out, nil
	}
	out := make([]boxRecord, 0, len(ids))
	for _, id := range ids {
		record, ok := s.state.Boxes[id]
		if !ok || record.ClientID != client {
			return nil, apiError("NOT_FOUND", "Box not found")
		}
		copy, err := cloneBoxRecord(record)
		if err != nil {
			return nil, err
		}
		out = append(out, copy)
	}
	return out, nil
}

func (s *Store) BoxRecord(id string) (boxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.state.Boxes[id]
	if !ok {
		return boxRecord{}, apiError("NOT_FOUND", "Box not found")
	}
	return cloneBoxRecord(record)
}

func sameObservationBase(a, b boxRecord) bool {
	return a.Box.Version == b.Box.Version && a.Box.Generation == b.Box.Generation &&
		a.Box.Phase == b.Box.Phase && a.Box.OperationID == b.Box.OperationID &&
		a.Handle == b.Handle && a.ExecutionID == b.ExecutionID &&
		a.Staged == b.Staged && a.RestoreArchiveID == b.RestoreArchiveID &&
		a.RestoreComplete == b.RestoreComplete
}

// UpdateObservedBox performs the compare even when update reports no changes.
// It persists only when the observation changed the box.
func (s *Store) UpdateObservedBox(base boxRecord, update func(*boxRecord) (bool, error)) (boxRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.state.Boxes[base.Box.ID]
	if !ok || !sameObservationBase(base, current) {
		return boxRecord{}, errBoxObservationConflict
	}
	next, err := cloneBoxRecord(current)
	if err != nil {
		return boxRecord{}, err
	}
	changed, err := update(&next)
	if err != nil {
		return boxRecord{}, err
	}
	if !changed {
		return cloneBoxRecord(current)
	}
	state, err := cloneState(s.state)
	if err != nil {
		return boxRecord{}, err
	}
	state.Boxes[base.Box.ID] = next
	if err = s.persistLocked(state); err != nil {
		return boxRecord{}, err
	}
	return cloneBoxRecord(next)
}
func (s *Store) Close() error {
	if s.lock == nil {
		return nil
	}
	return s.lock.Close()
}
func (s *Store) View(fn func(State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	var copy State
	if err = json.Unmarshal(b, &copy); err != nil {
		return err
	}
	return fn(copy)
}
func (s *Store) Update(fn func(*State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	var next State
	if err = json.Unmarshal(b, &next); err != nil {
		return err
	}
	if err = fn(&next); err != nil {
		return err
	}
	return s.persistLocked(next)
}

func cloneState(state State) (State, error) {
	b, err := json.Marshal(state)
	if err != nil {
		return State{}, err
	}
	var next State
	err = json.Unmarshal(b, &next)
	return next, err
}

func (s *Store) persistLocked(next State) error {
	b, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.dir, ".state-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(s.dir, "state.json")); err != nil {
		return err
	}
	// Rename already committed; keep memory aligned even if directory fsync fails.
	s.state = next
	d, err := os.Open(s.dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Cellbox: state committed but directory sync unavailable")
		return nil
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		fmt.Fprintln(os.Stderr, "Cellbox: state committed but directory sync failed; crash durability is uncertain")
	}
	return nil
}
