package service

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

type gatedBoxObjects struct {
	*ledgerObjects
	entered chan string
	release chan struct{}
}

func (o *gatedBoxObjects) Put(ctx context.Context, key string, data io.ReadSeeker, size int64, condition string) (string, error) {
	if o.entered != nil && strings.HasPrefix(key, recordPrefix+"/boxes/") {
		o.entered <- key
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-o.release:
		}
	}
	return o.ledgerObjects.Put(ctx, key, data, size, condition)
}
func setBoxOwner(id, owner string) func(*State) error {
	return func(st *State) error { b := st.Boxes[id]; b.Box.OwnerKey = owner; st.Boxes[id] = b; return nil }
}

func TestBoxCommitsAreParallelSingleObjectAndSurviveHeadReplay(t *testing.T) {
	objects := &gatedBoxObjects{ledgerObjects: &ledgerObjects{}}
	s := openRecords(t, objects)
	updateRecords(t, s, func(st *State) { st.Boxes["a"] = coreBox("a"); st.Boxes["b"] = coreBox("b") })
	head := bytes.Clone(objects.data[stateObjectKey])
	before := objects.revision
	objects.entered, objects.release = make(chan string, 2), make(chan struct{})
	done := make(chan error, 2)
	for _, id := range []string{"a", "b"} {
		go func() { done <- s.UpdateBox(id, setBoxOwner(id, "updated-"+id)) }()
	}
	for range 2 {
		select {
		case <-objects.entered:
		case <-time.After(time.Second):
			close(objects.release)
			t.Fatal("different boxes serialized their object writes")
		}
	}
	close(objects.release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if objects.revision-before != 2 || !bytes.Equal(head, objects.data[stateObjectKey]) {
		t.Fatal("box commits rewrote the global head or wrote multiple objects")
	}
	reopened := openRecords(t, objects.ledgerObjects)
	for _, id := range []string{"a", "b"} {
		if reopened.state.Boxes[id].Box.OwnerKey != "updated-"+id {
			t.Fatal("head replay rolled back a box commit")
		}
	}
}

func TestNewStoreFencesAnInflightOldBoxWriter(t *testing.T) {
	objects := &gatedBoxObjects{ledgerObjects: &ledgerObjects{}}
	old := openRecords(t, objects)
	updateRecords(t, old, func(st *State) { st.Boxes["a"] = coreBox("a") })
	objects.entered, objects.release = make(chan string, 1), make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- old.UpdateBox("a", setBoxOwner("a", "stale")) }()
	<-objects.entered
	current := openRecords(t, objects.ledgerObjects)
	close(objects.release)
	if err := <-done; err == nil {
		t.Fatal("old API writer passed a new leader's box fence")
	}
	if err := current.UpdateBox("a", setBoxOwner("a", "current")); err != nil {
		t.Fatal(err)
	}
	if openRecords(t, objects.ledgerObjects).state.Boxes["a"].Box.OwnerKey != "current" {
		t.Fatal("new leader commit lost")
	}
}

func TestBoxCommitRetainsExpiredIdempotencyFences(t *testing.T) {
	objects := &ledgerObjects{}
	s := openRecords(t, objects)
	key := "owner:resume:a:old"
	updateRecords(t, s, func(st *State) { st.Boxes["a"] = coreBox("a"); st.Keys[key] = keyRecord{Expired: true} })
	if err := s.UpdateBox("a", func(st *State) error {
		if !st.Keys[key].Expired {
			t.Fatal("expired key not visible to box transaction")
		}
		return setBoxOwner("a", "updated")(st)
	}); err != nil {
		t.Fatal(err)
	}
	if !openRecords(t, objects).state.Keys[key].Expired {
		t.Fatal("box commit lost durable expired key")
	}
}

func TestOperationWaitWakesOnCommitAndHonorsOwnership(t *testing.T) {
	f := newCoreFixture(t)
	if err := f.service.store.Update(func(st *State) error {
		st.Operations["op-wait"] = operationRecord{ClientID: "client-a", Operation: Operation{ID: "op-wait", Status: "running", Version: 1}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan Operation, 1)
	go func() { op, _ := f.service.waitOperation(ctx, "client-a", "op-wait", 10*time.Second); done <- op }()
	if _, err := f.service.waitOperation(ctx, "client-b", "op-wait", 10*time.Second); err == nil {
		t.Fatal("cross-client operation wait accepted")
	}
	if err := f.service.store.Update(func(st *State) error {
		op := st.Operations["op-wait"]
		op.Operation.Status = "succeeded"
		op.Operation.Version++
		st.Operations["op-wait"] = op
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case op := <-done:
		if op.Status != "succeeded" {
			t.Fatalf("completion notification lost: %+v", op)
		}
	case <-ctx.Done():
		t.Fatal("operation waiter did not wake")
	}
}
