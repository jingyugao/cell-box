// SPDX-License-Identifier: Apache-2.0
package node

import (
	"cellbox.local/cellbox/internal/objectstorage"
	runtimebackend "cellbox.local/cellbox/internal/runtime"
	"errors"
	"fmt"
)

func retryableStorage(err error) error {
	if errors.Is(err, objectstorage.ErrUnavailable) {
		return fmt.Errorf("%w: %w", runtimebackend.ErrRetryableStorage, err)
	}
	return err
}
