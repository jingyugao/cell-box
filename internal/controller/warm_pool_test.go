// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/warmtemplates"
	core "k8s.io/api/core/v1"
)

type poolRuntime struct {
	*fakeRuntime
	deadlines map[string]time.Time
}

func (*poolRuntime) RegisterWarm(context.Context, *core.Pod, string) error { return nil }
func (f *poolRuntime) WarmState(_ context.Context, p *core.Pod) (string, time.Time, error) {
	return "waiting", f.deadlines[string(p.UID)], nil
}
func (*poolRuntime) ActivateWarm(context.Context, *api.ResumablePod, *core.Pod) (bool, error) {
	return true, nil
}
func (*poolRuntime) ReapWarm(context.Context, string, map[string]bool) error { return nil }

func TestWarmPoolConfigurationErrorStopsBeforeChangingPool(t *testing.T) {
	want := errors.New("sqlite unavailable")
	published := false
	pool := &WarmPool{
		ReadDesired: func(context.Context) ([]warmtemplates.Template, string, error) {
			return nil, "", want
		},
		PublishObservation: func(warmtemplates.Snapshot) { published = true },
	}
	if err := pool.reconcile(context.Background()); !errors.Is(err, want) {
		t.Fatalf("reconcile error = %v, want %v", err, want)
	}
	if published {
		t.Fatal("configuration error published an empty/zero observation")
	}
}

func TestWarmPoolPublishesAcknowledgedEmptySnapshot(t *testing.T) {
	r, f, w := fixture(t)
	var got warmtemplates.Snapshot
	pool := &WarmPool{
		Reconciler: r, Namespace: w.Namespace, Size: 0,
		ReadDesired: func(context.Context) ([]warmtemplates.Template, string, error) {
			hash, err := warmtemplates.HashTemplates(nil)
			return nil, hash, err
		},
		PublishObservation: func(snapshot warmtemplates.Snapshot) { got = snapshot },
	}
	r.Runtime = &poolRuntime{fakeRuntime: f, deadlines: map[string]time.Time{}}
	if err := pool.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got.NodeName != r.NodeName || got.DesiredHash == "" || got.ObservedAt.IsZero() || len(got.Pools) != 0 {
		t.Fatalf("empty pool observation was not acknowledged: %+v", got)
	}
}

func TestAllocateWarmSlotsUsesFairNodeBudget(t *testing.T) {
	if got := allocateWarmSlots(map[string]int{"a": 3, "b": 3}, 4); got["a"] != 2 || got["b"] != 2 {
		t.Fatalf("equal demands should share budget equally: %v", got)
	}
	if got := allocateWarmSlots(map[string]int{"a": 1, "b": 5}, 4); got["a"] != 1 || got["b"] != 3 {
		t.Fatalf("unused demand should flow to the other template: %v", got)
	}
	if got := allocateWarmSlots(map[string]int{"a": 4, "b": 4, "c": 4}, 2); got["a"]+got["b"]+got["c"] != 2 {
		t.Fatalf("allocation exceeded node budget: %v", got)
	}
}
