package controller

import (
	"context"
	"time"

	api "github.com/jingyugao/cell-box/api/v1alpha1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

const recoveryWindow = 10 * time.Minute
const maxRecoveryAttempts = 3

func (r *Reconciler) canRecoverExecution(w *api.ResumablePod) bool {
	if !r.RestartOnExecutionLoss || w.DeletionTimestamp != nil || w.Spec.DesiredState != "Running" ||
		w.Status.Snapshot != "" || len(workloadMounts(w)) == 0 {
		return false
	}
	// Staged archive restores have a separate commit protocol.
	for _, arg := range w.Spec.Container.Args {
		if arg == "--staged" {
			return false
		}
	}
	return w.Status.RecoveryAttempts < maxRecoveryAttempts || time.Since(w.Status.LastRecoveryTime.Time) >= recoveryWindow
}

func (r *Reconciler) recoverExecution(ctx context.Context, w *api.ResumablePod, reason string) (ctrl.Result, error) {
	if time.Since(w.Status.LastRecoveryTime.Time) >= recoveryWindow {
		w.Status.RecoveryAttempts = 0
	}
	w.Status.RecoveryAttempts++
	w.Status.LastRecoveryTime = meta.Now()
	// Keep the old Pod UID until cleanup has confirmed release. begin then
	// durably allocates a new cycle and fences every old execution connection.
	return r.phase(ctx, w, "Recovering", reason+"; cold restarting with persistent volumes; process memory lost")
}
