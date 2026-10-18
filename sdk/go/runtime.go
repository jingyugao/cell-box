package cellbox

import (
	"context"
	"net/http"
)

// GetTemplateRuntime reads sandbox usage for this request and includes the
// provider's latest warm-pool observation. Kubernetes usage comes from a
// resource inventory query; inventory failures yield an unknown count.
func (c *Client) GetTemplateRuntime(ctx context.Context, templateID string) (*TemplateRuntimeStatus, error) {
	path, err := resourcePath("/v1/templates/", templateID)
	if err != nil {
		return nil, err
	}
	var status TemplateRuntimeStatus
	err = c.do(ctx, http.MethodGet, path+"/runtime", nil, &status)
	return &status, err
}

// GetRevisionWarmPool reads desired revision configuration from SQLite and
// returns the provider's latest warm-pool observation. Resources are configured
// requests/limits, not measured usage.
func (c *Client) GetRevisionWarmPool(ctx context.Context, revisionID string) (*RevisionWarmStatus, error) {
	path, err := resourcePath("/v1/template-revisions/", revisionID)
	if err != nil {
		return nil, err
	}
	var status RevisionWarmStatus
	err = c.do(ctx, http.MethodGet, path+"/warm-pool", nil, &status)
	return &status, err
}
