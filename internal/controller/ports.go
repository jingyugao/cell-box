package controller

import (
	"context"
	"log/slog"
	"sync"
	"time"

	api "cellbox.local/cellbox/api/v1alpha1"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type listeningRuntime interface {
	WaitPortsListening(context.Context, *api.ResumablePod, *core.Pod) (*api.Execution, error)
	CheckPortsListening(context.Context, *api.ResumablePod, *core.Pod) (*api.Execution, error)
}

type portWait struct {
	transition string
	pod        types.UID
	cancel     context.CancelFunc
}

type portWaits struct {
	mu      sync.Mutex
	pending map[types.NamespacedName]*portWait
}

func (r *Reconciler) stopPortWait(key types.NamespacedName) {
	r.ports.mu.Lock()
	defer r.ports.mu.Unlock()
	if waiting := r.ports.pending[key]; waiting != nil {
		waiting.cancel()
		delete(r.ports.pending, key)
	}
}

// waitListening starts at most one event waiter per execution. Reconciliation
// never sleeps waiting for CRI or polls for a listener. Health checks can enqueue
// the same resource if a notification was lost.
func (r *Reconciler) waitListening(ctx context.Context, w *api.ResumablePod, p *core.Pod) (ctrl.Result, error) {
	backend := r.Runtime.(listeningRuntime)
	execution, err := backend.CheckPortsListening(ctx, w, p)
	if err != nil {
		return ctrl.Result{}, err
	}
	key := client.ObjectKeyFromObject(w)
	if execution != nil {
		r.stopPortWait(key)
		w.Status.Execution = execution
		return r.started(ctx, w, p)
	}
	r.ports.mu.Lock()
	defer r.ports.mu.Unlock()
	if r.ports.pending == nil {
		r.ports.pending = make(map[types.NamespacedName]*portWait)
	}
	if old := r.ports.pending[key]; old != nil {
		if old.transition == w.Spec.TransitionID && old.pod == p.UID {
			return ctrl.Result{}, nil
		}
		old.cancel()
	}
	timeout := time.Duration(w.Spec.StartupTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	waiting := &portWait{transition: w.Spec.TransitionID, pod: p.UID, cancel: cancel}
	r.ports.pending[key] = waiting
	workload, pod := w.DeepCopy(), p.DeepCopy()
	go func() {
		defer cancel()
		execution, err := backend.WaitPortsListening(waitCtx, workload, pod)
		r.ports.mu.Lock()
		current := r.ports.pending[key] == waiting
		if current {
			delete(r.ports.pending, key)
		}
		r.ports.mu.Unlock()
		if !current || waitCtx.Err() != nil {
			return
		}
		if err != nil {
			slog.Warn("port event wait unavailable; health checks will reconcile", "box", workload.Name, "error", err)
			return
		}
		if execution != nil {
			if notifier, ok := r.Client.(interface{ Notify(types.NamespacedName) }); ok {
				notifier.Notify(key)
			}
		}
	}()
	return ctrl.Result{}, nil
}

// CancelPortWaits ends all subscriptions when the controller shuts down.
func (r *Reconciler) CancelPortWaits() {
	r.ports.mu.Lock()
	defer r.ports.mu.Unlock()
	for key, waiting := range r.ports.pending {
		waiting.cancel()
		delete(r.ports.pending, key)
	}
}
