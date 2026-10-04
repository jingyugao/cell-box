package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"cellbox.local/cellbox/internal/objectstorage"
)

func TestCompressedMetadataRetainsCASAndAmbiguousWriteRecovery(t *testing.T) {
	ctx := context.Background()
	objects := &ledgerObjects{lostResponse: true}
	data := bytes.Repeat([]byte(`{"operation":"resume","status":"succeeded"}`), 1000)
	etag, err := writeObject(ctx, objects, stateObjectKey, data, "")
	if err != nil || etag == "" || len(objects.data[stateObjectKey]) >= len(data)/4 {
		t.Fatalf("compressed ambiguous commit was not recovered: %v", err)
	}
	actual, actualETag, err := readObject(ctx, objects, stateObjectKey)
	if err != nil || !bytes.Equal(actual, data) || actualETag != etag {
		t.Fatal("compressed metadata or its wire ETag changed on read")
	}
	updated := append(bytes.Clone(data), ' ')
	next, err := writeObject(ctx, objects, stateObjectKey, updated, etag)
	if err != nil || next == etag {
		t.Fatalf("conditional replacement failed: %v", err)
	}
	if _, err := writeObject(ctx, objects, stateObjectKey, data, etag); !errors.Is(err, objectstorage.ErrConflict) {
		t.Fatalf("stale writer accepted: %v", err)
	}
	objects.data[stateObjectKey] = objects.data[stateObjectKey][:len(objects.data[stateObjectKey])-1]
	if _, _, err := readObject(ctx, objects, stateObjectKey); err == nil {
		t.Fatal("truncated compressed metadata accepted")
	}
}

func TestCompressedBoxHistorySurvivesHeadReplay(t *testing.T) {
	objects := &ledgerObjects{}
	s := openRecords(t, objects)
	updateRecords(t, s, func(st *State) {
		st.Boxes["a"] = coreBox("a")
		for i := range 500 {
			id := fmt.Sprintf("op-%032x", i)
			st.Operations[id] = operationRecord{ClientID: "owner", Operation: Operation{ID: id, TargetID: "a", Status: "succeeded"}}
			st.Keys[id] = keyRecord{OperationID: id, Hash: "input"}
		}
	})
	wire := objects.data[recordKey("boxes", "a")]
	if len(wire) < 2 || wire[0] != 0x1f || wire[1] != 0x8b {
		t.Fatal("large box history was not compressed")
	}
	if err := s.UpdateBox("a", setBoxOwner("a", "updated")); err != nil {
		t.Fatal(err)
	}
	reopened := openRecords(t, objects)
	if reopened.state.Boxes["a"].Box.OwnerKey != "updated" || len(reopened.state.Operations) != 500 || len(reopened.state.Keys) != 500 {
		t.Fatal("compressed head replay lost the box commit or idempotency history")
	}
	if err := reopened.UpdateBox("a", setBoxOwner("a", "reopened")); err != nil {
		t.Fatal(err)
	}
	if openRecords(t, objects).state.Boxes["a"].Box.OwnerKey != "reopened" {
		t.Fatal("reopened box CAS was not durable")
	}
}
