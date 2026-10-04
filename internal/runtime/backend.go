// SPDX-License-Identifier: Apache-2.0

// Package runtime is the boundary between lifecycle reconciliation and node work.
// v0.1 uses an in-process gVisor backend. A future node-agent transport must
// preserve the same idempotency and ownership guarantees before replacing it.
package runtime

import (
	api "cellbox.local/cellbox/api/v1alpha1"
	"context"
	"errors"
	core "k8s.io/api/core/v1"
)

// ErrRetryableStorage marks an object-storage outage that must not make a
// checkpoint unrecoverable or fail an otherwise valid restore permanently.
var ErrRetryableStorage = errors.New("retryable checkpoint storage failure")

// ErrWarmExpired permits ordinary restore from the unchanged checkpoint.
var ErrWarmExpired = errors.New("warm slot expired before assignment")

// Backend performs operations only on the workload and Pod identities supplied
// by the controller. Implementations must reject ambiguous checkpoint replay.
// Cleanup must be idempotent, and must not report success with live owned tasks.
type Backend interface {
	Checkpoint(context.Context, *api.ResumablePod, *core.Pod) error
	Prepare(context.Context, *api.ResumablePod, *core.Pod) error
	Cleanup(context.Context, *core.Pod) error
	Forget(context.Context, *api.ResumablePod) error
}
