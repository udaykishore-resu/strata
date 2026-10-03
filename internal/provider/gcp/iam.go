package gcp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/udaykishore-resu/strata/internal/provider"
)

// policy is an IAM policy kept as a generic map so fields Strata does not
// manage (auditConfigs, conditional bindings) survive the round trip.
type policy map[string]any

func (p policy) bindings() []map[string]any {
	raw, _ := p["bindings"].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, b := range raw {
		if m, ok := b.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func (p policy) setBindings(bs []map[string]any) {
	raw := make([]any, 0, len(bs))
	for _, b := range bs {
		raw = append(raw, b)
	}
	p["bindings"] = raw
	for _, b := range bs {
		if _, ok := b["condition"]; ok {
			p["version"] = 3
			return
		}
	}
}

func members(b map[string]any) []string {
	raw, _ := b["members"].([]any)
	out := make([]string, 0, len(raw))
	for _, m := range raw {
		if s, ok := m.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func setMembers(b map[string]any, ms []string) {
	sort.Strings(ms)
	raw := make([]any, len(ms))
	for i, m := range ms {
		raw[i] = m
	}
	b["members"] = raw
}

// hasMember reports whether the unconditional binding for role contains member.
func (p policy) hasMember(role, member string) bool {
	for _, b := range p.bindings() {
		if b["role"] == role && b["condition"] == nil {
			for _, m := range members(b) {
				if m == member {
					return true
				}
			}
		}
	}
	return false
}

// add inserts member into the unconditional binding for role.
func (p policy) add(role, member string) bool {
	if p.hasMember(role, member) {
		return false
	}
	bs := p.bindings()
	for _, b := range bs {
		if b["role"] == role && b["condition"] == nil {
			setMembers(b, append(members(b), member))
			p.setBindings(bs)
			return true
		}
	}
	nb := map[string]any{"role": role}
	setMembers(nb, []string{member})
	p.setBindings(append(bs, nb))
	return true
}

// remove deletes member from the unconditional binding for role.
func (p policy) remove(role, member string) bool {
	changed := false
	var out []map[string]any
	for _, b := range p.bindings() {
		if b["role"] == role && b["condition"] == nil {
			var keep []string
			for _, m := range members(b) {
				if m == member {
					changed = true
					continue
				}
				keep = append(keep, m)
			}
			if len(keep) == 0 {
				continue
			}
			setMembers(b, keep)
		}
		out = append(out, b)
	}
	if changed {
		p.setBindings(out)
	}
	return changed
}

// iamTarget knows how to get and set the policy of one resource kind.
type iamTarget struct {
	typ         string // resource type, e.g. gcp:storage:BucketIamMember
	description string
	resourceKey string // property naming the resource
	// path builds the resource path from the property value and env.
	path func(env provider.Env, props map[string]any) string
	get  func(ctx context.Context, c *Client, path string) (policy, error)
	set  func(ctx context.Context, c *Client, path string, p policy) error
	// extra properties beyond resource/role/member (all ForceNew).
	extra map[string]provider.PropertySpec
}

// iamMember implements one *IamMember resource type: a single member added
// to one role on one resource, managed non-authoritatively so bindings
// created outside Strata are preserved.
type iamMember struct {
	c *Client
	t iamTarget
}

func (m *iamMember) Schema() provider.Schema {
	props := map[string]provider.PropertySpec{
		m.t.resourceKey: {Type: provider.TypeString, Required: true, ForceNew: true, Description: "The resource to grant access on."},
		"role":          {Type: provider.TypeString, Required: true, ForceNew: true, Description: "IAM role, e.g. roles/storage.objectViewer."},
		"member":        {Type: provider.TypeString, Required: true, ForceNew: true, Description: "Principal, e.g. serviceAccount:x@p.iam.gserviceaccount.com."},
	}
	for k, v := range m.t.extra {
		props[k] = v
	}
	return provider.Schema{
		Type:        m.t.typ,
		Description: m.t.description,
		Properties:  props,
		Attributes:  map[string]string{"etag": "Policy etag after the change."},
	}
}

func (m *iamMember) ids(req provider.Request) (path, role, member string) {
	return m.t.path(req.Env, req.Properties), provider.String(req.Properties, "role"), provider.String(req.Properties, "member")
}

// modify runs a read-modify-write cycle, retrying etag conflicts and the
// eventual consistency of freshly created principals.
func (m *iamMember) modify(ctx context.Context, path string, fn func(policy) bool) error {
	deadline := time.Now().Add(2 * time.Minute)
	for attempt := 0; ; attempt++ {
		p, err := m.t.get(ctx, m.c, path)
		if err != nil {
			return err
		}
		if !fn(p) {
			return nil
		}
		err = m.t.set(ctx, m.c, path, p)
		if err == nil {
			return nil
		}
		retry := isConcurrencyConflict(err) || isPrincipalPropagation(err)
		if !retry || time.Now().After(deadline) {
			return err
		}
		if werr := m.c.wait(ctx, backoff(attempt)); werr != nil {
			return errors.Join(err, werr)
		}
	}
}

// isServiceAccountPropagation detects a service account that was created
// moments ago and is not yet visible to another API (for example Cloud Run).
func isServiceAccountPropagation(err error) bool {
	var ae *APIError
	if !errors.As(err, &ae) || (ae.Code != http.StatusBadRequest && ae.Code != http.StatusNotFound && ae.Code != http.StatusForbidden) {
		return false
	}
	msg := strings.ToLower(ae.Message)
	return strings.Contains(msg, "service account") && (strings.Contains(msg, "does not exist") || strings.Contains(msg, "not found"))
}

// retryNewServiceAccount retries fn for up to two minutes while it fails
// only because a freshly created service account is still propagating.
func (c *Client) retryNewServiceAccount(ctx context.Context, fn func() error) error {
	deadline := time.Now().Add(2 * time.Minute)
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil || !isServiceAccountPropagation(err) || time.Now().After(deadline) {
			return err
		}
		if werr := c.wait(ctx, backoff(attempt)); werr != nil {
			return errors.Join(err, werr)
		}
	}
}

// isPrincipalPropagation detects "service account does not exist" errors
// that occur for a short time after a service account is created.
func isPrincipalPropagation(err error) bool {
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != http.StatusBadRequest {
		return false
	}
	msg := strings.ToLower(ae.Message)
	return strings.Contains(msg, "does not exist") || strings.Contains(msg, "not found")
}

func (m *iamMember) Create(ctx context.Context, req provider.Request) (provider.Result, error) {
	path, role, member := m.ids(req)
	if err := m.modify(ctx, path, func(p policy) bool { return p.add(role, member) }); err != nil {
		return provider.Result{}, err
	}
	return provider.Result{PhysicalID: path + "|" + role + "|" + member, Attributes: map[string]any{}}, nil
}

func (m *iamMember) Read(ctx context.Context, req provider.Request) (provider.Result, error) {
	parts := strings.SplitN(req.PhysicalID, "|", 3)
	if len(parts) != 3 {
		return provider.Result{}, fmt.Errorf("invalid IAM member ID %q", req.PhysicalID)
	}
	p, err := m.t.get(ctx, m.c, parts[0])
	if err != nil {
		return provider.Result{}, err
	}
	if !p.hasMember(parts[1], parts[2]) {
		return provider.Result{}, fmt.Errorf("%s not bound to %s on %s: %w", parts[2], parts[1], parts[0], provider.ErrNotFound)
	}
	etag, _ := p["etag"].(string)
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: map[string]any{"etag": etag},
		Observed: map[string]any{"role": parts[1], "member": parts[2]}}, nil
}

// Update is never called: every property forces replacement.
func (m *iamMember) Update(ctx context.Context, req provider.Request) (provider.Result, error) {
	return m.Create(ctx, req)
}

func (m *iamMember) Delete(ctx context.Context, req provider.Request) error {
	parts := strings.SplitN(req.PhysicalID, "|", 3)
	if len(parts) != 3 {
		return fmt.Errorf("invalid IAM member ID %q", req.PhysicalID)
	}
	err := m.modify(ctx, parts[0], func(p policy) bool { return p.remove(parts[1], parts[2]) })
	if errors.Is(err, provider.ErrNotFound) {
		return nil // the resource itself is gone
	}
	return err
}

// Policy access styles used by Google APIs.

// getPostV1: POST {path}:getIamPolicy with requestedPolicyVersion (Resource Manager).
func getPost(service, prefix string) func(context.Context, *Client, string) (policy, error) {
	return func(ctx context.Context, c *Client, path string) (policy, error) {
		var p policy
		body := map[string]any{"options": map[string]any{"requestedPolicyVersion": 3}}
		err := c.Do(ctx, http.MethodPost, c.endpoint(service)+prefix+path+":getIamPolicy", body, &p)
		if p == nil {
			p = policy{}
		}
		return p, err
	}
}

// getGet: GET {path}:getIamPolicy?options.requestedPolicyVersion=3 (Pub/Sub, Run, Secret Manager).
func getGet(service, prefix string) func(context.Context, *Client, string) (policy, error) {
	return func(ctx context.Context, c *Client, path string) (policy, error) {
		var p policy
		err := c.Do(ctx, http.MethodGet, c.endpoint(service)+prefix+path+":getIamPolicy?options.requestedPolicyVersion=3", nil, &p)
		if p == nil {
			p = policy{}
		}
		return p, err
	}
}

// setPost: POST {path}:setIamPolicy {"policy": ...}.
func setPost(service, prefix string) func(context.Context, *Client, string, policy) error {
	return func(ctx context.Context, c *Client, path string, p policy) error {
		return c.Do(ctx, http.MethodPost, c.endpoint(service)+prefix+path+":setIamPolicy", map[string]any{"policy": p}, nil)
	}
}

func fullName(value, collection string, env provider.Env) string {
	if strings.HasPrefix(value, "projects/") {
		return value
	}
	return fmt.Sprintf("projects/%s/%s/%s", env.Project, collection, value)
}

func iamProviders(c *Client) []provider.Provider {
	targets := []iamTarget{
		{
			typ: "gcp:projects:IamMember", description: "Grants a role on the project to one member (non-authoritative).",
			resourceKey: "project",
			path: func(env provider.Env, props map[string]any) string {
				if p := provider.String(props, "project"); p != "" {
					return "projects/" + strings.TrimPrefix(p, "projects/")
				}
				return "projects/" + env.Project
			},
			get: getPost("cloudresources", "/v1/"),
			set: setPost("cloudresources", "/v1/"),
		},
		{
			typ: "gcp:storage:BucketIamMember", description: "Grants a role on a Cloud Storage bucket to one member.",
			resourceKey: "bucket",
			path:        func(_ provider.Env, props map[string]any) string { return provider.String(props, "bucket") },
			get: func(ctx context.Context, c *Client, bucket string) (policy, error) {
				var p policy
				err := c.Do(ctx, http.MethodGet, c.endpoint("storage")+"/storage/v1/b/"+url.PathEscape(bucket)+"/iam?optionsRequestedPolicyVersion=3", nil, &p)
				if p == nil {
					p = policy{}
				}
				return p, err
			},
			set: func(ctx context.Context, c *Client, bucket string, p policy) error {
				return c.Do(ctx, http.MethodPut, c.endpoint("storage")+"/storage/v1/b/"+url.PathEscape(bucket)+"/iam", p, nil)
			},
		},
		{
			typ: "gcp:pubsub:TopicIamMember", description: "Grants a role on a Pub/Sub topic to one member.",
			resourceKey: "topic",
			path: func(env provider.Env, props map[string]any) string {
				return fullName(provider.String(props, "topic"), "topics", env)
			},
			get: getGet("pubsub", "/v1/"), set: setPost("pubsub", "/v1/"),
		},
		{
			typ: "gcp:pubsub:SubscriptionIamMember", description: "Grants a role on a Pub/Sub subscription to one member.",
			resourceKey: "subscription",
			path: func(env provider.Env, props map[string]any) string {
				return fullName(provider.String(props, "subscription"), "subscriptions", env)
			},
			get: getGet("pubsub", "/v1/"), set: setPost("pubsub", "/v1/"),
		},
		{
			typ: "gcp:run:ServiceIamMember", description: "Grants a role on a Cloud Run service to one member (e.g. roles/run.invoker).",
			resourceKey: "service",
			extra: map[string]provider.PropertySpec{
				"region": {Type: provider.TypeString, ForceNew: true, Description: "Service region; defaults to the engine region."},
			},
			path: func(env provider.Env, props map[string]any) string {
				svc := provider.String(props, "service")
				if strings.HasPrefix(svc, "projects/") {
					return svc
				}
				region := provider.String(props, "region")
				if region == "" {
					region = env.Region
				}
				return fmt.Sprintf("projects/%s/locations/%s/services/%s", env.Project, region, svc)
			},
			get: getGet("run", "/v2/"), set: setPost("run", "/v2/"),
		},
		{
			typ: "gcp:secretmanager:SecretIamMember", description: "Grants a role on a Secret Manager secret to one member.",
			resourceKey: "secret",
			path: func(env provider.Env, props map[string]any) string {
				return fullName(provider.String(props, "secret"), "secrets", env)
			},
			get: getGet("secretmanager", "/v1/"), set: setPost("secretmanager", "/v1/"),
		},
	}
	out := make([]provider.Provider, 0, len(targets))
	for _, t := range targets {
		out = append(out, &iamMember{c: c, t: t})
	}
	return out
}
