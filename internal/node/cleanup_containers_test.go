// SPDX-License-Identifier: Apache-2.0

package node

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRemovalPendingRetriesOnlyContainersStillStarting(t *testing.T) {
	// containerd reports a warm slot whose StartContainer RPC has not unwound yet
	// with code Unknown, not FailedPrecondition.
	starting := status.Error(codes.Unknown, `failed to set removing state for container "abc": container is in starting state, can't be removed`)
	for name, err := range map[string]error{
		"starting":            starting,
		"failed precondition": status.Error(codes.FailedPrecondition, "container is running"),
	} {
		if !removalPending(err) {
			t.Fatalf("%s: removal should be retried: %v", name, err)
		}
	}
	for name, err := range map[string]error{
		"other unknown": status.Error(codes.Unknown, "failed to remove container: input/output error"),
		"unavailable":   status.Error(codes.Unavailable, "containerd is down"),
		"plain":         errors.New("boom"),
	} {
		if removalPending(err) {
			t.Fatalf("%s: unrelated failure must not be retried: %v", name, err)
		}
	}
}
