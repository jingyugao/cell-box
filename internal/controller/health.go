package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	"github.com/jingyugao/cell-box/internal/node"
	"github.com/jingyugao/cell-box/internal/runtimecontrol"
	core "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type healthRuntime interface {
	CheckHealth(context.Context, *api.ResumablePod, *core.Pod) error
}

// HealthChecker checks running and restoring executions independently of the
// low-latency resume waiter. Failures and missed LISTEN events converge through
// the same controller and locally committed status feed.
type HealthChecker struct {
	Reconciler *Reconciler
	Namespace  string
}

type healthFailure struct {
	execution string
	count     int
}

const maxConcurrentHealthChecks = 8

type healthResult struct {
	resource *api.ResumablePod
	err      error
}

func (*HealthChecker) NeedLeaderElection() bool { return true }

func (h *HealthChecker) Start(ctx context.Context) error {
	defer h.Reconciler.CancelPortWaits()
	failures := make(map[types.NamespacedName]healthFailure)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		h.check(ctx, failures)
	}
}

func (h *HealthChecker) check(ctx context.Context, failures map[types.NamespacedName]healthFailure) {
	r := h.Reconciler
	backend, ok := r.Runtime.(healthRuntime)
	if !ok {
		return
	}
	local, ok := r.Client.(*runtimecontrol.Client)
	if !ok {
		return
	}
	resources, err := local.HealthResources(ctx, h.Namespace)
	if err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "cannot enumerate runtime health")
		return
	}
	seen := make(map[types.NamespacedName]bool)
	var pending []*api.ResumablePod
	for _, w := range resources {
		key := client.ObjectKeyFromObject(w)
		if w.Spec.NodeName != r.NodeName || w.DeletionTimestamp != nil || w.Spec.DesiredState != "Running" || (w.Status.Phase != "Running" && w.Status.Phase != "Restoring") {
			continue
		}
		seen[key] = true
		pending = append(pending, w)
	}
	if len(pending) > 0 {
		workers := min(maxConcurrentHealthChecks, len(pending))
		jobs := make(chan *api.ResumablePod)
		results := make(chan healthResult, len(pending))
		var workerGroup sync.WaitGroup
		workerGroup.Add(workers)
		for range workers {
			go func() {
				defer workerGroup.Done()
				for w := range jobs {
					results <- h.checkResource(ctx, backend, w)
				}
			}()
		}
		go func() {
			defer close(jobs)
			for _, w := range pending {
				select {
				case jobs <- w:
				case <-ctx.Done():
					return
				}
			}
		}()
		go func() {
			workerGroup.Wait()
			close(results)
		}()
		for result := range results {
			h.recordHealthResult(ctx, local, failures, result)
		}
	}
	for key := range failures {
		if !seen[key] {
			delete(failures, key)
		}
	}
}

func (h *HealthChecker) checkResource(ctx context.Context, backend healthRuntime, w *api.ResumablePod) healthResult {
	r := h.Reconciler
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	p, err := r.pod(checkCtx, w)
	if err == nil && p != nil {
		err = backend.CheckHealth(checkCtx, w, p)
	}
	if err == nil && p == nil {
		err = node.ErrPortsNotListening
	}
	return healthResult{resource: w, err: err}
}

func (h *HealthChecker) recordHealthResult(ctx context.Context, local *runtimecontrol.Client, failures map[types.NamespacedName]healthFailure, result healthResult) {
	r := h.Reconciler
	w, err := result.resource, result.err
	key := client.ObjectKeyFromObject(w)
	startupTimeout := time.Duration(w.Spec.StartupTimeoutSeconds) * time.Second
	if startupTimeout <= 0 {
		startupTimeout = 90 * time.Second
	}
	startupExpired := time.Since(w.Status.Since.Time) >= startupTimeout
	if err == nil {
		delete(failures, key)
		if w.Status.Phase == "Restoring" {
			local.Notify(key)
		}
		return
	}
	if w.Status.Phase == "Restoring" && !startupExpired && !errors.Is(err, node.ErrExecutionLost) {
		// Startup grace is not part of the consecutive failure count. In
		// particular, failures recorded here must not carry into Running.
		delete(failures, key)
		if !errors.Is(err, node.ErrPortsNotListening) {
			ctrl.LoggerFrom(ctx).Error(err, "cannot inspect execution health", "box", w.Name)
		}
		return
	}
	if !errors.Is(err, node.ErrExecutionLost) && !errors.Is(err, node.ErrPortsNotListening) {
		// An unavailable CRI endpoint is not evidence that the application died.
		ctrl.LoggerFrom(ctx).Error(err, "cannot inspect execution health", "box", w.Name)
		if w.Status.Phase != "Restoring" {
			delete(failures, key)
			return
		}
		err = fmt.Errorf("startup timeout: cannot confirm listening ports: %w", err)
	}
	identity := fmt.Sprintf("%s/%s/%d/%s/%s", w.UID, w.Spec.TransitionID, w.Status.Cycle, w.Status.PodUID, w.Status.Phase)
	previous := failures[key]
	if previous.execution != identity {
		previous = healthFailure{execution: identity}
	}
	previous.count++
	failures[key] = previous
	if !errors.Is(err, node.ErrExecutionLost) {
		if previous.count < 3 {
			return
		}
	}
	// Optimistic status publication fences concurrent suspend/resume/deletion.
	// Preserve the execution for inspection; health checks do not destroy it.
	w.Status.Phase, w.Status.Message = "Failed", err.Error()
	w.Status.ObservedGeneration, w.Status.ObservedTransitionID = w.Generation, w.Spec.TransitionID
	w.Status.Since = meta.Now()
	apimeta.SetStatusCondition(&w.Status.Conditions, meta.Condition{Type: "Ready", Status: meta.ConditionFalse, Reason: "Failed", Message: err.Error(), ObservedGeneration: w.Generation})
	if updateErr := r.Status().Update(ctx, w); updateErr != nil {
		ctrl.LoggerFrom(ctx).Error(updateErr, "cannot publish execution health", "box", w.Name)
	}
}
