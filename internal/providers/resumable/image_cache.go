// SPDX-License-Identifier: Apache-2.0

package resumable

import (
	"context"
	"errors"
)

// CacheImage runs only through the embedded node backend. The separate-process
// ConfigMap request protocol is intentionally unsupported.
func (p *Provider) CacheImage(ctx context.Context, image, node, namespace string) error {
	if !validImage.MatchString(image) || node == "" || namespace == "" {
		return errors.New("image caching requires an immutable image, node and namespace")
	}
	if p == nil || p.CacheImageLocal == nil {
		return errors.New("image caching requires the embedded node runtime")
	}
	return p.CacheImageLocal(ctx, image, node, namespace)
}
