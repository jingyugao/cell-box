package cellbox

import (
	"context"
	"net/http"
)

// GetTemplateRuntime returns the last background observation of revision usage
// and warm-pool status. Inspect Stale and ObservedAt before relying on counts.
func (c *Client) GetTemplateRuntime(ctx context.Context, templateID string) (*TemplateRuntimeStatus, error) {
	path, err := resourcePath("/v1/templates/", templateID)
	if err != nil {
		return nil, err
	}
	var status TemplateRuntimeStatus
	err = c.do(ctx, http.MethodGet, path+"/runtime", "", nil, &status)
	return &status, err
}

// GetRevisionWarmPool returns the last background warm-pool observation for a
// revision. Resources are configured requests/limits, not measured usage.
func (c *Client) GetRevisionWarmPool(ctx context.Context, revisionID string) (*RevisionWarmStatus, error) {
	path, err := resourcePath("/v1/template-revisions/", revisionID)
	if err != nil {
		return nil, err
	}
	var status RevisionWarmStatus
	err = c.do(ctx, http.MethodGet, path+"/warm-pool", "", nil, &status)
	return &status, err
}
