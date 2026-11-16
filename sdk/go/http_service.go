package cellbox

import (
	"context"
	"net/http"
	"time"
)

// HTTPService exposes one business TCP port to Traefik. The application must
// listen on the Pod address, and all traffic must use the Cellbox middleware.
type HTTPService struct {
	Port               int `json:"port"`
	IdleTimeoutSeconds int `json:"idleTimeoutSeconds"` // Zero uses 300 seconds.
}

type HTTPRequest struct {
	ID        string    `json:"id"`
	StartedAt time.Time `json:"startedAt"`
	Admitted  bool      `json:"admitted"`
}

type HTTPServiceStatus struct {
	Configuration  HTTPService   `json:"configuration"`
	ServiceName    string        `json:"serviceName"`
	Namespace      string        `json:"namespace"`
	LastActivityAt time.Time     `json:"lastActivityAt"`
	Requests       []HTTPRequest `json:"requests"`
}

func (c *Client) GetHTTPService(ctx context.Context, boxID string) (*HTTPServiceStatus, error) {
	path, err := resourcePath("/v1/boxes/", boxID)
	if err != nil {
		return nil, err
	}
	var status HTTPServiceStatus
	err = c.do(ctx, http.MethodGet, path+"/http-service", nil, &status)
	return &status, err
}
