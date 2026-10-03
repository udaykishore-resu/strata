// Package gcp implements Strata resource providers by calling Google Cloud
// REST APIs directly — no Terraform, no client SDKs. It contains a small
// authenticated HTTP client with retries, long-running-operation polling,
// and IAM policy read-modify-write helpers shared by all resource types.
package gcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/udaykishore-resu/strata/internal/provider"
)

// Default API endpoints; override per service for tests or emulators.
var defaultEndpoints = map[string]string{
	"storage":        "https://storage.googleapis.com",
	"iam":            "https://iam.googleapis.com",
	"pubsub":         "https://pubsub.googleapis.com",
	"run":            "https://run.googleapis.com",
	"serviceusage":   "https://serviceusage.googleapis.com",
	"secretmanager":  "https://secretmanager.googleapis.com",
	"cloudresources": "https://cloudresourcemanager.googleapis.com",
}

// Client is an authenticated JSON client for Google APIs.
type Client struct {
	HTTP         *http.Client
	Tokens       TokenSource
	Endpoints    map[string]string
	UserAgent    string
	QuotaProject string
	MaxRetries   int
	// PollInterval is the initial LRO polling interval.
	PollInterval time.Duration
	sleep        func(ctx context.Context, d time.Duration) error
}

// NewClient returns a client with production defaults.
func NewClient(ts TokenSource) *Client {
	eps := map[string]string{}
	for k, v := range defaultEndpoints {
		eps[k] = v
	}
	return &Client{
		HTTP:         &http.Client{Timeout: 60 * time.Second},
		Tokens:       ts,
		Endpoints:    eps,
		UserAgent:    "strata-engine/1.0",
		MaxRetries:   5,
		PollInterval: 2 * time.Second,
	}
}

func (c *Client) endpoint(service string) string {
	if v, ok := c.Endpoints[service]; ok {
		return strings.TrimRight(v, "/")
	}
	return defaultEndpoints[service]
}

// APIError is a non-2xx response from a Google API.
type APIError struct {
	Code    int    `json:"code"`
	Status  string `json:"status"`
	Message string `json:"message"`
	Method  string `json:"-"`
	URL     string `json:"-"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%s %s: %d %s: %s", e.Method, e.URL, e.Code, e.Status, e.Message)
}

// Is maps HTTP status codes to provider sentinel errors.
func (e *APIError) Is(target error) bool {
	switch target {
	case provider.ErrNotFound:
		return e.Code == http.StatusNotFound
	case provider.ErrAlreadyExists:
		return e.Code == http.StatusConflict && (e.Status == "ALREADY_EXISTS" || e.Status == "" ||
			strings.Contains(strings.ToLower(e.Message), "already"))
	}
	return false
}

// isConcurrencyConflict reports an etag/precondition conflict worth retrying.
func isConcurrencyConflict(err error) bool {
	var ae *APIError
	if !errors.As(err, &ae) {
		return false
	}
	return ae.Code == http.StatusPreconditionFailed || ae.Status == "ABORTED" ||
		(ae.Code == http.StatusConflict && ae.Status != "ALREADY_EXISTS" && !strings.Contains(strings.ToLower(ae.Message), "already"))
}

func retryable(method string, code int) bool {
	switch code {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	case http.StatusInternalServerError, http.StatusRequestTimeout:
		return method == http.MethodGet || method == http.MethodPut || method == http.MethodDelete
	}
	return false
}

func (c *Client) wait(ctx context.Context, d time.Duration) error {
	if c.sleep != nil {
		return c.sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func backoff(attempt int) time.Duration {
	base := time.Duration(1<<min(attempt, 5)) * 500 * time.Millisecond
	return base/2 + time.Duration(rand.Int64N(int64(base/2)+1))
}

// Do sends a JSON request and decodes a JSON response into out (if non-nil).
// in may be nil, a []byte, or any JSON-marshalable value.
func (c *Client) Do(ctx context.Context, method, url string, in, out any) error {
	var body []byte
	if in != nil {
		if b, ok := in.([]byte); ok {
			body = b
		} else {
			var err error
			if body, err = json.Marshal(in); err != nil {
				return fmt.Errorf("encode request: %w", err)
			}
		}
	}
	var lastErr error
	for attempt := 0; attempt <= c.MaxRetries; attempt++ {
		if attempt > 0 {
			if err := c.wait(ctx, backoff(attempt)); err != nil {
				return errors.Join(lastErr, err)
			}
		}
		code, respBody, err := c.once(ctx, method, url, body)
		if err != nil {
			if ctx.Err() != nil {
				return err
			}
			lastErr = err // network error: retry
			continue
		}
		if code >= 200 && code < 300 {
			if out != nil && len(respBody) > 0 {
				if err := json.Unmarshal(respBody, out); err != nil {
					return fmt.Errorf("decode %s %s response: %w", method, url, err)
				}
			}
			return nil
		}
		apiErr := parseAPIError(method, url, code, respBody)
		if !retryable(method, code) {
			return apiErr
		}
		lastErr = apiErr
	}
	return lastErr
}

func (c *Client) once(ctx context.Context, method, url string, body []byte) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return 0, nil, err
	}
	tok, err := c.Tokens.Token(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("get access token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.QuotaProject != "" {
		req.Header.Set("X-Goog-User-Project", c.QuotaProject)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	return resp.StatusCode, b, err
}

func parseAPIError(method, url string, code int, body []byte) *APIError {
	var env struct {
		Error json.RawMessage `json:"error"`
	}
	ae := &APIError{Code: code, Method: method, URL: url}
	if json.Unmarshal(body, &env) == nil && len(env.Error) > 0 {
		var inner APIError
		if json.Unmarshal(env.Error, &inner) == nil {
			ae.Status, ae.Message = inner.Status, inner.Message
		}
	}
	if ae.Message == "" {
		ae.Message = strings.TrimSpace(string(body))
		if len(ae.Message) > 500 {
			ae.Message = ae.Message[:500]
		}
	}
	if ae.Status == "" {
		ae.Status = http.StatusText(code)
	}
	return ae
}

// operation is a google.longrunning.Operation.
type operation struct {
	Name     string          `json:"name"`
	Done     bool            `json:"done"`
	Error    *opStatus       `json:"error"`
	Response json.RawMessage `json:"response"`
}

type opStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// grpcToHTTP maps google.rpc.Code to HTTP status for error classification.
var grpcToHTTP = map[int]int{3: 400, 5: 404, 6: 409, 7: 403, 8: 429, 9: 400, 10: 409, 13: 500, 14: 503, 16: 401}

// WaitOperation polls a long-running operation until it completes. service
// selects the API endpoint; versionPrefix is the path prefix before the
// operation name (for example "/v2/").
func (c *Client) WaitOperation(ctx context.Context, service, versionPrefix string, op operation) (json.RawMessage, error) {
	interval := c.PollInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	for !op.Done {
		if err := c.wait(ctx, interval); err != nil {
			return nil, fmt.Errorf("waiting for operation %s: %w", op.Name, err)
		}
		if interval < 15*time.Second {
			interval = interval * 3 / 2
		}
		next := operation{}
		if err := c.Do(ctx, http.MethodGet, c.endpoint(service)+versionPrefix+op.Name, nil, &next); err != nil {
			return nil, err
		}
		op = next
	}
	if op.Error != nil && op.Error.Code != 0 {
		code := grpcToHTTP[op.Error.Code]
		if code == 0 {
			code = 500
		}
		return nil, &APIError{Code: code, Status: fmt.Sprintf("OPERATION_FAILED(%d)", op.Error.Code), Message: op.Error.Message, Method: "LRO", URL: op.Name}
	}
	return op.Response, nil
}
