package cellbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
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

func (c *Client) do(ctx context.Context, method, path, key string, body, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("cellbox: encode request: %w", err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.clientID != "" {
		req.Header.Set("X-Cellbox-Client-ID", c.clientID)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
		// net/http may retry requests carrying an idempotency key on a stale
		// reused connection. Disable body replay: callers control recovery.
		req.GetBody = nil
	}
	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("cellbox: %s request: %w", method, err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var envelope struct {
			Error *APIError `json:"error"`
		}
		if json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&envelope) == nil && envelope.Error != nil {
			envelope.Error.StatusCode = res.StatusCode
			return envelope.Error
		}
		return &APIError{StatusCode: res.StatusCode, Code: "HTTP_ERROR", Message: http.StatusText(res.StatusCode)}
	}
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

func (c *Client) submit(ctx context.Context, path, key string, body any) (*Operation, error) {
	if key == "" {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return nil, err
		}
		key = hex.EncodeToString(random[:])
	}
	if len(key) > 200 {
		return nil, fmt.Errorf("cellbox: idempotency key exceeds 200 bytes")
	}
	var op Operation
	if err := c.do(ctx, http.MethodPost, path, key, body, &op); err != nil {
		return nil, &RequestError{IdempotencyKey: key, Err: err}
	}
	op.IdempotencyKey = key
	if op.ID == "" {
		return nil, &RequestError{IdempotencyKey: key, Err: fmt.Errorf("cellbox: response missing operation ID")}
	}
	return &op, nil
}

func resourcePath(prefix, id string) (string, error) {
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, "/\\\x00\r\n") {
		return "", fmt.Errorf("cellbox: invalid resource ID")
	}
	return prefix + url.PathEscape(id), nil
}

func (c *Client) ListProfiles(ctx context.Context) ([]Profile, error) {
	var profiles []Profile
	err := c.do(ctx, http.MethodGet, "/v1/profiles", "", nil, &profiles)
	return profiles, err
}

// CreateTemplate creates a template family. Revisions are created separately.
func (c *Client) CreateTemplate(ctx context.Context, opts CreateTemplateOptions) (*Template, error) {
	if strings.TrimSpace(opts.Name) == "" {
		return nil, fmt.Errorf("cellbox: template name is required")
	}
	if opts.DefaultWarmSlots < 0 || opts.DefaultWarmSlots > 1024 {
		return nil, fmt.Errorf("cellbox: default warm slots must be 0..1024")
	}
	var template Template
	if err := c.do(ctx, http.MethodPost, "/v1/templates", "", opts, &template); err != nil {
		return nil, err
	}
	return &template, nil
}

// ListTemplates lists template families in the client's namespace.
func (c *Client) ListTemplates(ctx context.Context, opts ...TemplateListOptions) ([]Template, error) {
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
	err := c.do(ctx, http.MethodGet, path, "", nil, &templates)
	return templates, err
}

// GetTemplate returns a template family by its ID.
func (c *Client) GetTemplate(ctx context.Context, id string) (*Template, error) {
	path, err := resourcePath("/v1/templates/", id)
	if err != nil {
		return nil, err
	}
	var template Template
	if err := c.do(ctx, http.MethodGet, path, "", nil, &template); err != nil {
		return nil, err
	}
	return &template, nil
}

// UpdateTemplate updates mutable family settings using optimistic concurrency.
func (c *Client) UpdateTemplate(ctx context.Context, id string, opts UpdateTemplateOptions) (*Template, error) {
	path, err := resourcePath("/v1/templates/", id)
	if err != nil {
		return nil, err
	}
	var template Template
	err = c.do(ctx, http.MethodPatch, path, "", opts, &template)
	if err != nil {
		return nil, err
	}
	return &template, nil
}

func (c *Client) SetTemplateWarmSlots(ctx context.Context, id string, slots int, expectedGeneration uint64) (*Template, error) {
	return c.UpdateTemplate(ctx, id, UpdateTemplateOptions{DefaultWarmSlots: &slots, ExpectedGeneration: expectedGeneration})
}

// BuildRevision submits an asynchronous build and returns its durable operation.
func (c *Client) BuildRevision(ctx context.Context, templateID string, opts CreateRevisionOptions, idempotencyKey string) (*Operation, error) {
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
	return c.submit(ctx, path+"/revisions", idempotencyKey, opts)
}

// WaitBuildRevision waits for a build and loads the resulting immutable revision.
func (c *Client) WaitBuildRevision(ctx context.Context, op *Operation) (*TemplateRevision, error) {
	if op == nil || op.ID == "" {
		return nil, fmt.Errorf("cellbox: operation is required")
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
		revisionID = completed.TargetID
	}
	if revisionID == "" {
		return nil, &OperationError{Operation: completed, Err: fmt.Errorf("build result missing revision ID")}
	}
	return c.GetTemplateRevision(ctx, revisionID)
}

func (c *Client) ListTemplateRevisions(ctx context.Context, templateID string, opts ...RevisionListOptions) ([]TemplateRevision, error) {
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
	err = c.do(ctx, http.MethodGet, path, "", nil, &revisions)
	return revisions, err
}

func (c *Client) GetTemplateRevision(ctx context.Context, id string) (*TemplateRevision, error) {
	path, err := resourcePath("/v1/template-revisions/", id)
	if err != nil {
		return nil, err
	}
	var revision TemplateRevision
	err = c.do(ctx, http.MethodGet, path, "", nil, &revision)
	return &revision, err
}

func (c *Client) UpdateTemplateRevision(ctx context.Context, id string, opts UpdateRevisionOptions) (*TemplateRevision, error) {
	if opts.ExpectedGeneration == 0 {
		return nil, fmt.Errorf("cellbox: expected generation is required")
	}
	if opts.WarmSlots != nil && (*opts.WarmSlots < 0 || *opts.WarmSlots > 1024) {
		return nil, fmt.Errorf("cellbox: warm slots must be 0..1024")
	}
	path, err := resourcePath("/v1/template-revisions/", id)
	if err != nil {
		return nil, err
	}
	var revision TemplateRevision
	err = c.do(ctx, http.MethodPatch, path, "", opts, &revision)
	return &revision, err
}

func (c *Client) ListOperations(ctx context.Context, opts ...OperationListOptions) ([]Operation, error) {
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
	err := c.do(ctx, http.MethodGet, path, "", nil, &operations)
	return operations, err
}

func (c *Client) GetOperationLogs(ctx context.Context, id string, offset int64, limit int) (*OperationLogs, error) {
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
	err = c.do(ctx, http.MethodGet, path, "", nil, &logs)
	return &logs, err
}

func (c *Client) CancelOperation(ctx context.Context, id string) (*Operation, error) {
	path, err := resourcePath("/v1/operations/", id)
	if err != nil {
		return nil, err
	}
	var operation Operation
	err = c.do(ctx, http.MethodPost, path+":cancel", "", struct{}{}, &operation)
	return &operation, err
}

// DeleteTemplate removes an unused template. The server rejects templates still
// referenced by active sandboxes or archives.
func (c *Client) DeleteTemplate(ctx context.Context, id string) error {
	path, err := resourcePath("/v1/templates/", id)
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodDelete, path, "", nil, nil)
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
	if err := c.do(ctx, http.MethodGet, path, "", nil, &op); err != nil {
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
		next.IdempotencyKey = op.IdempotencyKey
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
	if err := c.do(ctx, http.MethodGet, path, "", nil, &execution); err != nil {
		return nil, err
	}
	return &execution, nil
}
