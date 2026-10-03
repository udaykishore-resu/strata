// Package client is a Go client for the Strata API, used by the strata CLI
// and usable from CI pipelines or other tools.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client calls a Strata server.
type Client struct {
	BaseURL string
	HTTP    *http.Client
	// Token returns a bearer token (Google ID token) for each request; nil
	// means no Authorization header (dev servers).
	Token func(ctx context.Context) (string, error)
}

// New returns a client for baseURL.
func New(baseURL string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), HTTP: &http.Client{Timeout: 5 * time.Minute}}
}

// Error is a non-2xx API response.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("strata API: %d: %s", e.Status, e.Message) }

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Token != nil {
		tok, err := c.Token(ctx)
		if err != nil {
			return fmt.Errorf("get token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		var e struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		msg := strings.TrimSpace(string(b))
		if json.Unmarshal(b, &e) == nil && e.Error.Message != "" {
			msg = e.Error.Message
		}
		return &Error{Status: resp.StatusCode, Message: msg}
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

// Types mirror the server's JSON. They are kept independent of internal
// packages so the client is a stable public surface.

type PropertyDiff struct {
	Name       string `json:"name"`
	Old        any    `json:"old,omitempty"`
	New        any    `json:"new,omitempty"`
	ForcesNew  bool   `json:"forcesNew,omitempty"`
	KnownLater bool   `json:"knownAfterApply,omitempty"`
	Drift      bool   `json:"drift,omitempty"`
}

type Change struct {
	LogicalID  string         `json:"logicalId"`
	Type       string         `json:"type"`
	Action     string         `json:"action"`
	PhysicalID string         `json:"physicalId,omitempty"`
	Diffs      []PropertyDiff `json:"diffs,omitempty"`
}

type ChangeSet struct {
	ID          string         `json:"id"`
	Stack       string         `json:"stack"`
	BaseVersion int64          `json:"baseVersion"`
	Parameters  map[string]any `json:"parameters"`
	Changes     []Change       `json:"changes"`
	Status      string         `json:"status"`
	CreatedBy   string         `json:"createdBy"`
	CreatedAt   time.Time      `json:"createdAt"`
}

// HasChanges reports whether any change is not a no-op.
func (c *ChangeSet) HasChanges() bool {
	for _, ch := range c.Changes {
		if ch.Action != "NoOp" {
			return true
		}
	}
	return false
}

type Operation struct {
	ID          string     `json:"id"`
	Stack       string     `json:"stack"`
	Kind        string     `json:"kind"`
	ChangeSetID string     `json:"changeSetId,omitempty"`
	Status      string     `json:"status"`
	Result      string     `json:"result,omitempty"`
	Error       string     `json:"error,omitempty"`
	Attempts    int        `json:"attempts"`
	CreatedBy   string     `json:"createdBy,omitempty"`
	CreatedAt   time.Time  `json:"createdAt"`
	FinishedAt  *time.Time `json:"finishedAt,omitempty"`
}

// Done reports whether the operation finished.
func (o *Operation) Done() bool { return o.Status == "SUCCEEDED" || o.Status == "FAILED" }

type Resource struct {
	LogicalID    string         `json:"logicalId"`
	Type         string         `json:"type"`
	PhysicalID   string         `json:"physicalId"`
	Properties   map[string]any `json:"properties"`
	Attributes   map[string]any `json:"attributes"`
	Status       string         `json:"status"`
	StatusReason string         `json:"statusReason,omitempty"`
}

type Stack struct {
	Name             string               `json:"name"`
	Version          int64                `json:"version"`
	Status           string               `json:"status"`
	StatusReason     string               `json:"statusReason,omitempty"`
	Parameters       map[string]any       `json:"parameters,omitempty"`
	Outputs          map[string]any       `json:"outputs,omitempty"`
	Resources        map[string]*Resource `json:"resources"`
	PendingCleanup   []*Resource          `json:"pendingCleanup,omitempty"`
	CurrentOperation string               `json:"currentOperation,omitempty"`
	UpdatedAt        time.Time            `json:"updatedAt"`
}

type StackSummary struct {
	Name             string    `json:"name"`
	Status           string    `json:"status"`
	StatusReason     string    `json:"statusReason,omitempty"`
	Version          int64     `json:"version"`
	Resources        int       `json:"resources"`
	CurrentOperation string    `json:"currentOperation,omitempty"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

type Event struct {
	ID          int64     `json:"id"`
	Stack       string    `json:"stack"`
	OperationID string    `json:"operationId,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	LogicalID   string    `json:"logicalId,omitempty"`
	Type        string    `json:"type,omitempty"`
	PhysicalID  string    `json:"physicalId,omitempty"`
	Status      string    `json:"status"`
	Reason      string    `json:"reason,omitempty"`
}

type DriftResult struct {
	LogicalID  string         `json:"logicalId"`
	Type       string         `json:"type"`
	PhysicalID string         `json:"physicalId"`
	Status     string         `json:"status"`
	Diffs      []PropertyDiff `json:"diffs,omitempty"`
	Error      string         `json:"error,omitempty"`
}

type DriftReport struct {
	Stack     string        `json:"stack"`
	Drifted   bool          `json:"drifted"`
	Resources []DriftResult `json:"resources"`
}

func stackPath(stack string) string { return "/v1/stacks/" + url.PathEscape(stack) }

// CreateChangeSet plans a deployment. template is the raw template JSON.
// The server refreshes live state first; see CreateChangeSetWith.
func (c *Client) CreateChangeSet(ctx context.Context, stack string, template json.RawMessage, params map[string]any) (*ChangeSet, error) {
	return c.CreateChangeSetWith(ctx, stack, template, params, ChangeSetOptions{})
}

// ChangeSetOptions tune planning.
type ChangeSetOptions struct {
	// NoRefresh plans against recorded state only, skipping the read of
	// live resources (faster, but out-of-band changes are not restored).
	NoRefresh bool
}

// CreateChangeSetWith is CreateChangeSet with options.
func (c *Client) CreateChangeSetWith(ctx context.Context, stack string, template json.RawMessage, params map[string]any, opts ChangeSetOptions) (*ChangeSet, error) {
	body := map[string]any{"template": template, "parameters": params}
	if opts.NoRefresh {
		body["refresh"] = false
	}
	var out ChangeSet
	err := c.do(ctx, http.MethodPost, stackPath(stack)+"/changesets", body, &out)
	return &out, err
}

// GetChangeSet fetches a change set.
func (c *Client) GetChangeSet(ctx context.Context, stack, id string) (*ChangeSet, error) {
	var out ChangeSet
	err := c.do(ctx, http.MethodGet, stackPath(stack)+"/changesets/"+url.PathEscape(id), nil, &out)
	return &out, err
}

// ExecuteChangeSet starts applying a change set.
func (c *Client) ExecuteChangeSet(ctx context.Context, stack, id string) (*Operation, error) {
	var out Operation
	err := c.do(ctx, http.MethodPost, stackPath(stack)+"/changesets/"+url.PathEscape(id)+"/execute", nil, &out)
	return &out, err
}

// DeleteStack starts deleting a stack.
func (c *Client) DeleteStack(ctx context.Context, stack string) (*Operation, error) {
	var out Operation
	err := c.do(ctx, http.MethodDelete, stackPath(stack), nil, &out)
	return &out, err
}

// GetOperation fetches an operation.
func (c *Client) GetOperation(ctx context.Context, id string) (*Operation, error) {
	var out Operation
	err := c.do(ctx, http.MethodGet, "/v1/operations/"+url.PathEscape(id), nil, &out)
	return &out, err
}

// GetStack fetches a stack.
func (c *Client) GetStack(ctx context.Context, stack string) (*Stack, error) {
	var out Stack
	err := c.do(ctx, http.MethodGet, stackPath(stack), nil, &out)
	return &out, err
}

// ListStacks lists stacks.
func (c *Client) ListStacks(ctx context.Context) ([]StackSummary, error) {
	var out struct {
		Stacks []StackSummary `json:"stacks"`
	}
	err := c.do(ctx, http.MethodGet, "/v1/stacks", nil, &out)
	return out.Stacks, err
}

// Events lists stack events after the given ID.
func (c *Client) Events(ctx context.Context, stack string, after int64, limit int) ([]Event, error) {
	var out struct {
		Events []Event `json:"events"`
	}
	err := c.do(ctx, http.MethodGet, fmt.Sprintf("%s/events?after=%d&limit=%d", stackPath(stack), after, limit), nil, &out)
	return out.Events, err
}

// DetectDrift compares live resources with applied state.
func (c *Client) DetectDrift(ctx context.Context, stack string) (*DriftReport, error) {
	var out DriftReport
	err := c.do(ctx, http.MethodPost, stackPath(stack)+"/drift", nil, &out)
	return &out, err
}

// ResourceTypes returns the schemas of supported resource types.
func (c *Client) ResourceTypes(ctx context.Context) ([]map[string]any, error) {
	var out struct {
		ResourceTypes []map[string]any `json:"resourceTypes"`
	}
	err := c.do(ctx, http.MethodGet, "/v1/resource-types", nil, &out)
	return out.ResourceTypes, err
}

// Wait polls an operation until it finishes, calling onEvent for each new
// stack event (onEvent may be nil).
func (c *Client) Wait(ctx context.Context, op *Operation, afterEvent int64, onEvent func(Event)) (*Operation, error) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		evs, err := c.Events(ctx, op.Stack, afterEvent, 500)
		if err != nil {
			return nil, err
		}
		for _, e := range evs {
			afterEvent = e.ID
			if onEvent != nil && e.OperationID == op.ID {
				onEvent(e)
			}
		}
		cur, err := c.GetOperation(ctx, op.ID)
		if err != nil {
			return nil, err
		}
		if cur.Done() {
			// Drain events written just before completion.
			if evs, err := c.Events(ctx, op.Stack, afterEvent, 500); err == nil && onEvent != nil {
				for _, e := range evs {
					if e.OperationID == op.ID {
						onEvent(e)
					}
				}
			}
			return cur, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tick.C:
		}
	}
}
