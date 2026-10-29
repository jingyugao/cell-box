// SPDX-License-Identifier: Apache-2.0
package node

import (
	"errors"
	"fmt"
	"github.com/jingyugao/cell-box/internal/objectstorage"
	runtimebackend "github.com/jingyugao/cell-box/internal/runtime"
)

func retryableStorage(err error) error {
	if errors.Is(err, objectstorage.ErrUnavailable) {
		return fmt.Errorf("%w: %w", runtimebackend.ErrRetryableStorage, err)
	}
	return err
}
