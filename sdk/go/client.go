package cellbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config configures a client. Empty BaseURL, ClientID and Token read
// CELLBOX_API_URL, CELLBOX_CLIENT_ID and CELLBOX_CLIENT_TOKEN respectively.
type Config struct {
	// BaseURL is the server root (optionally with a reverse-proxy path prefix).
	BaseURL string
	// ClientID is a data namespace, not an authentication credential.
	ClientID string
	// Token is optional Bearer authentication for deployments behind a gateway.
	Token string
	// HTTPClient may customize TLS/proxies. It is copied; redirects are disabled
	// to avoid leaking credentials or redirecting a mutation to another endpoint.
	// The default has a 30-second per-request timeout; use ctx for overall limits.
	HTTPClient *http.Client
}

// Client is safe for concurrent use. Configuration is captured at construction.
type Client struct {
	baseURL  string
	clientID string
	token    string
	http     *http.Client
}

func NewClient(config Config) (*Client, error) {
	if config.BaseURL == "" {
		config.BaseURL = os.Getenv("CELLBOX_API_URL")
	}
	if config.ClientID == "" {
		config.ClientID = os.Getenv("CELLBOX_CLIENT_ID")
	}
	if config.Token == "" {
		config.Token = os.Getenv("CELLBOX_CLIENT_TOKEN")
	}
	u, err := url.Parse(config.BaseURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("cellbox: BaseURL must be an http(s) server URL without credentials, query or fragment")
	}
	hc := http.Client{Timeout: 30 * time.Second}
	if config.HTTPClient != nil {
		hc = *config.HTTPClient
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{baseURL: strings.TrimRight(u.String(), "/"), clientID: config.ClientID, token: config.Token, http: &hc}, nil
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("cellbox: encode request: %w", err)
		}
	}
	var reader io.Reader
	contentType := ""
	if body != nil {
		reader = bytes.NewReader(data)
		contentType = "application/json"
	}
	res, err := c.request(ctx, method, path, reader, contentType)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	// Two bounded 1 MiB command streams can expand sixfold in JSON escaping.
	const maxResponse = 16 << 20
	data, err = io.ReadAll(io.LimitReader(res.Body, maxResponse+1))
	if err != nil {
		return fmt.Errorf("cellbox: read response: %w", err)
	}
	if len(data) > maxResponse {
		return fmt.Errorf("cellbox: response exceeds %d bytes", maxResponse)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("cellbox: decode response: %w", err)
		}
	}
	return nil
}

// request applies the shared Cellbox headers and transport rules for JSON and
// raw file endpoints. The caller owns the body on success; errors close it.
func (c *Client) request(ctx context.Context, method, path string, body io.Reader, contentType string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.clientID != "" {
		req.Header.Set("X-Cellbox-Client-ID", c.clientID)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cellbox: %s request: %w", method, err)
	}
	if res.StatusCode >= 200 && res.StatusCode < 300 {
		return res, nil
	}
	defer res.Body.Close()
	var envelope struct {
		Error *APIError `json:"error"`
	}
	if json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&envelope) == nil && envelope.Error != nil {
		envelope.Error.StatusCode = res.StatusCode
		return nil, envelope.Error
	}
	return nil, &APIError{StatusCode: res.StatusCode, Code: "HTTP_ERROR", Message: http.StatusText(res.StatusCode)}
}

func (c *Client) submit(ctx context.Context, path string, body any) (*Operation, error) {
	var op Operation
	if err := c.do(ctx, http.MethodPost, path, body, &op); err != nil {
		return nil, &RequestError{Err: err}
	}
	if op.ID == "" {
		return nil, &RequestError{Err: fmt.Errorf("cellbox: response missing operation ID")}
	}
	return &op, nil
}

func resourcePath(prefix, id string) (string, error) {
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, "/\\\x00\r\n") {
		return "", fmt.Errorf("cellbox: invalid resource ID")
	}
	return prefix + url.PathEscape(id), nil
}

func validateListOptions(count, limit, offset int) error {
	if count > 1 {
		return fmt.Errorf("cellbox: at most one list options value is allowed")
	}
	if limit < 0 || limit > 500 {
		return fmt.Errorf("cellbox: list limit must be 0..500")
	}
	if offset < 0 {
		return fmt.Errorf("cellbox: list offset must be nonnegative")
	}
	return nil
}

func (c *Client) ListProfiles(ctx context.Context) ([]Profile, error) {
	var profiles []Profile
	err := c.do(ctx, http.MethodGet, "/v1/profiles", nil, &profiles)
	return profiles, err
}

// CreateTemplate creates a template family. Revisions are created separately.
func (c *Client) CreateTemplate(ctx context.Context, opts CreateTemplateOptions) (*Template, error) {
	if strings.TrimSpace(opts.Name) == "" {
		return nil, fmt.Errorf("cellbox: template name is required")
	}
	if opts.DefaultWarmSlots != nil && (*opts.DefaultWarmSlots < 0 || *opts.DefaultWarmSlots > 1024) {
		return nil, fmt.Errorf("cellbox: default warm slots must be 0..1024")
	}
	var template Template
	if err := c.do(ctx, http.MethodPost, "/v1/templates", opts, &template); err != nil {
		return nil, err
	}
	return &template, nil
}

// ListTemplates lists template families in the client's namespace.
func (c *Client) ListTemplates(ctx context.Context, opts ...TemplateListOptions) ([]Template, error) {
	var option TemplateListOptions
	if len(opts) > 0 {
		option = opts[0]
	}
	if err := validateListOptions(len(opts), option.Limit, option.Offset); err != nil {
		return nil, err
	}
	q := url.Values{}
	if len(opts) > 0 {
		if opts[0].Name != "" {
			q.Set("name", opts[0].Name)
		}
		if opts[0].Limit > 0 {
			q.Set("limit", strconv.Itoa(opts[0].Limit))
		}
		if opts[0].Offset > 0 {
			q.Set("offset", strconv.Itoa(opts[0].Offset))
		}
	}
	var templates []Template
	path := "/v1/templates"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	err := c.do(ctx, http.MethodGet, path, nil, &templates)
	return templates, err
}

// GetTemplate returns a template family by its ID.
func (c *Client) GetTemplate(ctx context.Context, id string) (*Template, error) {
	path, err := resourcePath("/v1/templates/", id)
	if err != nil {
		return nil, err
	}
	var template Template
	if err := c.do(ctx, http.MethodGet, path, nil, &template); err != nil {
		return nil, err
	}
	return &template, nil
}

// UpdateTemplate updates mutable family settings using optimistic concurrency.
func (c *Client) UpdateTemplate(ctx context.Context, id string, opts UpdateTemplateOptions) (*Template, error) {
	if opts.ExpectedGeneration == 0 {
		return nil, fmt.Errorf("cellbox: expected generation is required")
	}
	if opts.Description == nil && opts.DefaultWarmSlots == nil && opts.StableRevisionID == nil {
		return nil, fmt.Errorf("cellbox: at least one template field is required")
	}
	if opts.DefaultWarmSlots != nil && (*opts.DefaultWarmSlots < 0 || *opts.DefaultWarmSlots > 1024) {
		return nil, fmt.Errorf("cellbox: default warm slots must be 0..1024")
	}
	path, err := resourcePath("/v1/templates/", id)
	if err != nil {
		return nil, err
	}
	var template Template
	err = c.do(ctx, http.MethodPatch, path, opts, &template)
	if err != nil {
		return nil, err
	}
	return &template, nil
}

func (c *Client) SetTemplateWarmSlots(ctx context.Context, id string, slots int, expectedGeneration uint64) (*Template, error) {
	return c.UpdateTemplate(ctx, id, UpdateTemplateOptions{DefaultWarmSlots: &slots, ExpectedGeneration: expectedGeneration})
}

// SetTemplateStableRevision selects the default immutable revision for new
// sandboxes. Pass an empty revisionID to clear the stable revision.
func (c *Client) SetTemplateStableRevision(ctx context.Context, id, revisionID string, expectedGeneration uint64) (*Template, error) {
	return c.UpdateTemplate(ctx, id, UpdateTemplateOptions{StableRevisionID: &revisionID, ExpectedGeneration: expectedGeneration})
}

// BuildRevision submits an asynchronous build once and returns its durable
// operation. Build scripts can have arbitrary side effects, so the SDK does
// not attach an idempotency key or retry a submission whose response is lost.
func (c *Client) BuildRevision(ctx context.Context, templateID string, opts CreateRevisionOptions) (*Operation, error) {
	if strings.TrimSpace(opts.UserVersion) == "" || strings.TrimSpace(opts.BaseImage) == "" || strings.TrimSpace(opts.ProfileID) == "" {
		return nil, fmt.Errorf("cellbox: user version, base image, and profile ID are required")
	}
	if opts.WarmSlots != nil && (*opts.WarmSlots < 0 || *opts.WarmSlots > 1024) {
		return nil, fmt.Errorf("cellbox: warm slots must be 0..1024")
	}
	path, err := resourcePath("/v1/templates/", templateID)
	if err != nil {
		return nil, err
	}
	return c.submit(ctx, path+"/revisions", opts)
}

// WaitBuildRevision waits for a build and loads the resulting immutable revision.
func (c *Client) WaitBuildRevision(ctx context.Context, op *Operation) (*TemplateRevision, error) {
	if op == nil || op.ID == "" {
		return nil, fmt.Errorf("cellbox: operation is required")
	}
	if op.Kind != "template-build" {
		return nil, fmt.Errorf("cellbox: operation %s is %q, want template-build", op.ID, op.Kind)
	}
	completed, err := c.wait(ctx, op)
	if err != nil {
		return nil, err
	}
	var revisionID string
	if completed.Result != nil {
		revisionID = completed.Result["revisionId"]
	}
	if revisionID == "" {
		return nil, &OperationError{Operation: completed, Err: fmt.Errorf("build result missing revision ID")}
	}
	revision, err := c.GetTemplateRevision(ctx, revisionID)
	if err != nil {
		return nil, &OperationError{Operation: completed, Err: err}
	}
	return revision, nil
}

func (c *Client) ListTemplateRevisions(ctx context.Context, templateID string, opts ...RevisionListOptions) ([]TemplateRevision, error) {
	var option RevisionListOptions
	if len(opts) > 0 {
		option = opts[0]
	}
	if err := validateListOptions(len(opts), option.Limit, option.Offset); err != nil {
		return nil, err
	}
	path, err := resourcePath("/v1/templates/", templateID)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	if len(opts) > 0 {
		if opts[0].UserVersion != "" {
			q.Set("userVersion", opts[0].UserVersion)
		}
		if opts[0].Limit > 0 {
			q.Set("limit", strconv.Itoa(opts[0].Limit))
		}
		if opts[0].Offset > 0 {
			q.Set("offset", strconv.Itoa(opts[0].Offset))
		}
	}
	path += "/revisions"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var revisions []TemplateRevision
	err = c.do(ctx, http.MethodGet, path, nil, &revisions)
	return revisions, err
}

func (c *Client) GetTemplateRevision(ctx context.Context, id string) (*TemplateRevision, error) {
	path, err := resourcePath("/v1/template-revisions/", id)
	if err != nil {
		return nil, err
	}
	var revision TemplateRevision
	err = c.do(ctx, http.MethodGet, path, nil, &revision)
	return &revision, err
}

func (c *Client) UpdateTemplateRevision(ctx context.Context, id string, opts UpdateRevisionOptions) (*TemplateRevision, error) {
	if opts.ExpectedGeneration == 0 {
		return nil, fmt.Errorf("cellbox: expected generation is required")
	}
	if opts.WarmSlots != nil && (*opts.WarmSlots < 0 || *opts.WarmSlots > 1024) {
		return nil, fmt.Errorf("cellbox: warm slots must be 0..1024")
	}
	if opts.Description == nil && opts.Status == nil && opts.WarmSlots == nil {
		return nil, fmt.Errorf("cellbox: at least one revision field is required")
	}
	if opts.Status != nil && *opts.Status != "active" && *opts.Status != "retired" {
		return nil, fmt.Errorf("cellbox: revision status must be active or retired")
	}
	path, err := resourcePath("/v1/template-revisions/", id)
	if err != nil {
		return nil, err
	}
	var revision TemplateRevision
	err = c.do(ctx, http.MethodPatch, path, opts, &revision)
	return &revision, err
}

// ListOperations lists template-build tasks from the local catalog. It does not
// include runtime sandbox operations.
func (c *Client) ListOperations(ctx context.Context, opts ...OperationListOptions) ([]Operation, error) {
	var option OperationListOptions
	if len(opts) > 0 {
		option = opts[0]
	}
	if err := validateListOptions(len(opts), option.Limit, option.Offset); err != nil {
		return nil, err
	}
	q := url.Values{}
	if len(opts) > 0 {
		if opts[0].TemplateID != "" {
			q.Set("templateId", opts[0].TemplateID)
		}
		if opts[0].Status != "" {
			q.Set("status", opts[0].Status)
		}
		if opts[0].Limit > 0 {
			q.Set("limit", strconv.Itoa(opts[0].Limit))
		}
		if opts[0].Offset > 0 {
			q.Set("offset", strconv.Itoa(opts[0].Offset))
		}
	}
	path := "/v1/operations"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var operations []Operation
	err := c.do(ctx, http.MethodGet, path, nil, &operations)
	return operations, err
}

// GetOperationLogs reads a template-build task's captured build output.
func (c *Client) GetOperationLogs(ctx context.Context, id string, offset int64, limit int) (*OperationLogs, error) {
	if offset < 0 {
		return nil, fmt.Errorf("cellbox: log offset must be nonnegative")
	}
	if limit < 0 || limit > 1<<20 {
		return nil, fmt.Errorf("cellbox: log limit must be 0..1048576")
	}
	path, err := resourcePath("/v1/operations/", id)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	if offset > 0 {
		q.Set("offset", strconv.FormatInt(offset, 10))
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if encoded := q.Encode(); encoded != "" {
		path += "/logs?" + encoded
	} else {
		path += "/logs"
	}
	var logs OperationLogs
	err = c.do(ctx, http.MethodGet, path, nil, &logs)
	return &logs, err
}

// CancelOperation requests cancellation of a template-build task.
func (c *Client) CancelOperation(ctx context.Context, id string) (*Operation, error) {
	path, err := resourcePath("/v1/operations/", id)
	if err != nil {
		return nil, err
	}
	var operation Operation
	err = c.do(ctx, http.MethodPost, path+":cancel", struct{}{}, &operation)
	return &operation, err
}

// DeleteTemplate removes an empty template family with no active build. Revisions
// must first be removed through a future API; the server does not query Kubernetes.
func (c *Client) DeleteTemplate(ctx context.Context, id string) error {
	path, err := resourcePath("/v1/templates/", id)
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodDelete, path, nil, nil)
}

func (c *Client) GetOperation(ctx context.Context, id string) (*Operation, error) {
	return c.getOperation(ctx, id, false)
}

func (c *Client) getOperation(ctx context.Context, id string, wait bool) (*Operation, error) {
	path, err := resourcePath("/v1/operations/", id)
	if err != nil {
		return nil, err
	}
	if wait {
		path += "?waitMs=10000"
	}
	var op Operation
	if err := c.do(ctx, http.MethodGet, path, nil, &op); err != nil {
		return nil, err
	}
	if op.ID != id {
		return nil, fmt.Errorf("cellbox: response operation ID does not match request")
	}
	return &op, nil
}

// WaitOperation uses server long polling. On failure it returns the last known
// operation and an OperationError. It never retries a mutation.
func (c *Client) WaitOperation(ctx context.Context, id string) (*Operation, error) {
	op, err := c.GetOperation(ctx, id)
	if err != nil {
		return nil, err
	}
	return c.wait(ctx, op)
}

func (c *Client) wait(ctx context.Context, op *Operation) (*Operation, error) {
	for {
		switch op.Status {
		case "succeeded":
			return op, nil
		case "failed", "cancelled":
			var err error = op.Error
			if op.Error == nil {
				err = fmt.Errorf("operation %s without error details", op.Status)
			}
			return op, &OperationError{Operation: op, Err: err}
		case "queued", "running", "cancelling":
		default:
			return op, &OperationError{Operation: op, Err: fmt.Errorf("unsupported operation status %q", op.Status)}
		}
		started := time.Now()
		next, err := c.getOperation(ctx, op.ID, true)
		if err != nil {
			return op, &OperationError{Operation: op, Err: err}
		}
		op = next
		// Avoid busy polling if an intermediary/server returns early.
		if (op.Status == "running" || op.Status == "queued" || op.Status == "cancelling") && time.Since(started) < 100*time.Millisecond {
			timer := time.NewTimer(100*time.Millisecond - time.Since(started))
			select {
			case <-ctx.Done():
				timer.Stop()
				return op, &OperationError{Operation: op, Err: ctx.Err()}
			case <-timer.C:
			}
		}
	}
}

func (c *Client) GetExecution(ctx context.Context, id string) (*Execution, error) {
	path, err := resourcePath("/v1/execs/", id)
	if err != nil {
		return nil, err
	}
	var execution Execution
	if err := c.do(ctx, http.MethodGet, path, nil, &execution); err != nil {
		return nil, err
	}
	return &execution, nil
}
