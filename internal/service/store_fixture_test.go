package service

import (
	"context"
	"encoding/json"
	"reflect"
)

// State is a test fixture snapshot, never a production Store cache.
type State struct {
	Boxes      map[string]boxRecord
	Operations map[string]operationRecord
	Executions map[string]executionRecord
	Archives   map[string]archiveRecord
}

func newState() State {
	return State{
		Boxes:      map[string]boxRecord{},
		Operations: map[string]operationRecord{},
		Executions: map[string]executionRecord{},
		Archives:   map[string]archiveRecord{},
	}
}

func loadTestState(ctx context.Context, reader sqlReader) (State, error) {
	state := newState()
	boxes, err := listRecords[boxRecord](ctx, reader, "boxes", "")
	if err != nil {
		return state, err
	}
	for _, record := range boxes {
		state.Boxes[record.Box.ID] = record
	}
	operations, err := listRecords[operationRecord](ctx, reader, "operations", "")
	if err != nil {
		return state, err
	}
	for _, record := range operations {
		state.Operations[record.Operation.ID] = record
	}
	executions, err := listRecords[executionRecord](ctx, reader, "executions", "")
	if err != nil {
		return state, err
	}
	for _, record := range executions {
		state.Executions[record.ID] = record
	}
	archives, err := listRecords[archiveRecord](ctx, reader, "archives", "")
	if err != nil {
		return state, err
	}
	for _, record := range archives {
		state.Archives[record.Archive.ID] = record
	}
	return state, nil
}

// View materializes only the test's small fixture subset from SQLite.
func (s *Store) View(fn func(State) error) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	state, err := loadTestState(ctx, tx)
	if err != nil {
		return err
	}
	return fn(state)
}

// Update is retained for older test fixtures. Each map edit is still applied
// as a SQLite transaction through the production record writers.
func (s *Store) Update(fn func(*State) error) error {
	return s.Write(context.Background(), func(tx *storeTx) error {
		before, err := loadTestState(tx.ctx, tx.tx)
		if err != nil {
			return err
		}
		data, err := json.Marshal(before)
		if err != nil {
			return err
		}
		var after State
		if err := json.Unmarshal(data, &after); err != nil {
			return err
		}
		if err := fn(&after); err != nil {
			return err
		}
		if err := syncTestRecords(tx, "boxes", before.Boxes, after.Boxes, tx.PutBox); err != nil {
			return err
		}
		if err := syncTestRecords(tx, "operations", before.Operations, after.Operations, tx.PutOperation); err != nil {
			return err
		}
		if err := syncTestRecords(tx, "executions", before.Executions, after.Executions, tx.PutExecution); err != nil {
			return err
		}
		return syncTestRecords(tx, "archives", before.Archives, after.Archives, tx.PutArchive)
	})
}

func syncTestRecords[T any](tx *storeTx, kind string, before, after map[string]T, put func(T) error) error {
	for key := range before {
		if _, exists := after[key]; !exists {
			if err := tx.Delete(kind, key); err != nil {
				return err
			}
			if kind == "executions" {
				if err := tx.Delete("execution_results", key); err != nil {
					return err
				}
			}
		}
	}
	for key, record := range after {
		if previous, exists := before[key]; exists && reflect.DeepEqual(previous, record) {
			continue
		}
		if err := put(record); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) UpdateBox(id string, fn func(*boxRecord) error) error {
	return s.UpdateBoxContext(context.Background(), id, fn)
}

func (s *Store) UpdateBoxContext(ctx context.Context, id string, fn func(*boxRecord) error) error {
	return s.Write(ctx, func(tx *storeTx) error {
		box, err := tx.Box("", id)
		if err != nil {
			return err
		}
		if err := fn(&box); err != nil {
			return err
		}
		return tx.PutBox(box)
	})
}
