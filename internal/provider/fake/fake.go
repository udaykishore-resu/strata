// Package fake provides in-memory providers that mirror the schemas of real
// providers. They back `strata-server --dev`, local demos and engine tests,
// and support fault injection to exercise rollback paths.
package fake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/udaykishore-resu/strata/internal/provider"
)

// Faults injects errors into fake providers, keyed by "<action>:<logicalID>"
// (action is create, update, delete or read), e.g. "create:Uploads".
type Faults struct {
	mu sync.Mutex
	m  map[string]int // remaining failures; -1 = always
}

// NewFaults parses a comma separated spec such as "create:Api,delete:Topic".
// A suffix "*N" fails only N times: "create:Api*1".
func NewFaults(spec string) *Faults {
	f := &Faults{m: map[string]int{}}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n := -1
		if i := strings.LastIndex(part, "*"); i > 0 {
			fmt.Sscanf(part[i+1:], "%d", &n)
			part = part[:i]
		}
		f.m[part] = n
	}
	return f
}

// Set adds a fault that fires n times (-1 forever).
func (f *Faults) Set(key string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = n
}

// Clear removes all faults.
func (f *Faults) Clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m = map[string]int{}
}

func (f *Faults) check(action, lid string) error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := action + ":" + lid
	n, ok := f.m[key]
	if !ok || n == 0 {
		return nil
	}
	if n > 0 {
		f.m[key] = n - 1
	}
	return fmt.Errorf("injected fault %s", key)
}

// Cloud is the shared in-memory "cloud" all fake providers write to.
type Cloud struct {
	mu      sync.Mutex
	objects map[string]map[string]map[string]any // type -> physical ID -> props
	Faults  *Faults
	// Latency simulates slow APIs.
	Latency time.Duration
	// Hook, when set, runs before every call (tests use it to cancel mid-run).
	Hook  func(action, logicalID string)
	calls []string
}

// NewCloud returns an empty fake cloud.
func NewCloud() *Cloud {
	return &Cloud{objects: map[string]map[string]map[string]any{}, Faults: NewFaults("")}
}

// Calls returns the recorded call log ("create:Type:physicalID").
func (c *Cloud) Calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

// Get returns the stored properties of an object.
func (c *Cloud) Get(typ, id string) (map[string]any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.objects[typ][id]
	return p, ok
}

// Count returns how many objects of all types exist.
func (c *Cloud) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, m := range c.objects {
		n += len(m)
	}
	return n
}

// Mutate changes an object out-of-band (drift simulation).
func (c *Cloud) Mutate(typ, id, key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p, ok := c.objects[typ][id]; ok {
		p[key] = value
	}
}

// Put creates an object out-of-band (e.g. a resource owned by someone else).
func (c *Cloud) Put(typ, id string, props map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.objects[typ] == nil {
		c.objects[typ] = map[string]map[string]any{}
	}
	c.objects[typ][id] = copyProps(props)
}

// Remove deletes an object out-of-band.
func (c *Cloud) Remove(typ, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.objects[typ], id)
}

// Provider is a fake implementation of one resource type.
type Provider struct {
	schema provider.Schema
	cloud  *Cloud
}

// Mirror returns fake providers for every given schema, sharing one cloud.
func Mirror(cloud *Cloud, schemas []provider.Schema) []provider.Provider {
	out := make([]provider.Provider, 0, len(schemas))
	for _, s := range schemas {
		out = append(out, &Provider{schema: s, cloud: cloud})
	}
	return out
}

func (p *Provider) Schema() provider.Schema { return p.schema }

func (p *Provider) before(ctx context.Context, action, lid string) error {
	if p.cloud.Hook != nil {
		p.cloud.Hook(action, lid)
	}
	if p.cloud.Latency > 0 {
		select {
		case <-time.After(p.cloud.Latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.cloud.Faults.check(action, lid)
}

func (p *Provider) physicalID(req provider.Request) string {
	if p.schema.NameProperty != "" {
		if n := provider.String(req.Properties, p.schema.NameProperty); n != "" {
			return n
		}
	}
	b, _ := json.Marshal(req.Properties)
	sum := sha256.Sum256(append([]byte(req.LogicalID), b...))
	return strings.ToLower(req.LogicalID) + "-" + hex.EncodeToString(sum[:4])
}

func (p *Provider) attributes(id string, req provider.Request) map[string]any {
	attrs := map[string]any{}
	for name := range p.schema.Attributes {
		switch name {
		case "name":
			attrs[name] = id
		case "id":
			attrs[name] = fmt.Sprintf("projects/%s/%s/%s", req.Env.Project, strings.ToLower(kind(p.schema.Type)), id)
		case "email":
			attrs[name] = fmt.Sprintf("%s@%s.iam.gserviceaccount.com", id, req.Env.Project)
		case "member":
			attrs[name] = fmt.Sprintf("serviceAccount:%s@%s.iam.gserviceaccount.com", id, req.Env.Project)
		case "uri":
			attrs[name] = fmt.Sprintf("https://%s-fake.a.run.app", id)
		case "url":
			attrs[name] = "gs://" + id
		default:
			attrs[name] = id + "/" + name
		}
	}
	return attrs
}

func kind(t string) string {
	parts := strings.Split(t, ":")
	return parts[len(parts)-1] + "s"
}

func (p *Provider) record(action, id string) {
	p.cloud.calls = append(p.cloud.calls, action+":"+p.schema.Type+":"+id)
}

func copyProps(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	if out == nil {
		out = map[string]any{}
	}
	return out
}

func (p *Provider) Create(ctx context.Context, req provider.Request) (provider.Result, error) {
	if err := p.before(ctx, "create", req.LogicalID); err != nil {
		return provider.Result{}, err
	}
	id := p.physicalID(req)
	p.cloud.mu.Lock()
	defer p.cloud.mu.Unlock()
	objs := p.cloud.objects[p.schema.Type]
	if objs == nil {
		objs = map[string]map[string]any{}
		p.cloud.objects[p.schema.Type] = objs
	}
	if _, exists := objs[id]; exists {
		if p.schema.NameProperty == "" || p.schema.Shared {
			// Types without a name property, and shared settings such as
			// enabled APIs, must create idempotently.
			return provider.Result{PhysicalID: id, Attributes: p.attributes(id, req)}, nil
		}
		return provider.Result{}, fmt.Errorf("%s %q: %w", p.schema.Type, id, provider.ErrAlreadyExists)
	}
	objs[id] = copyProps(req.Properties)
	p.record("create", id)
	return provider.Result{PhysicalID: id, Attributes: p.attributes(id, req)}, nil
}

func (p *Provider) Read(ctx context.Context, req provider.Request) (provider.Result, error) {
	if err := p.before(ctx, "read", req.LogicalID); err != nil {
		return provider.Result{}, err
	}
	p.cloud.mu.Lock()
	defer p.cloud.mu.Unlock()
	props, ok := p.cloud.objects[p.schema.Type][req.PhysicalID]
	if !ok {
		return provider.Result{}, fmt.Errorf("%s %q: %w", p.schema.Type, req.PhysicalID, provider.ErrNotFound)
	}
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: p.attributes(req.PhysicalID, req), Observed: copyProps(props)}, nil
}

func (p *Provider) Update(ctx context.Context, req provider.Request) (provider.Result, error) {
	if err := p.before(ctx, "update", req.LogicalID); err != nil {
		return provider.Result{}, err
	}
	p.cloud.mu.Lock()
	defer p.cloud.mu.Unlock()
	if _, ok := p.cloud.objects[p.schema.Type][req.PhysicalID]; !ok {
		return provider.Result{}, fmt.Errorf("%s %q: %w", p.schema.Type, req.PhysicalID, provider.ErrNotFound)
	}
	p.cloud.objects[p.schema.Type][req.PhysicalID] = copyProps(req.Properties)
	p.record("update", req.PhysicalID)
	return provider.Result{PhysicalID: req.PhysicalID, Attributes: p.attributes(req.PhysicalID, req)}, nil
}

func (p *Provider) Delete(ctx context.Context, req provider.Request) error {
	if err := p.before(ctx, "delete", req.LogicalID); err != nil {
		return err
	}
	p.cloud.mu.Lock()
	defer p.cloud.mu.Unlock()
	delete(p.cloud.objects[p.schema.Type], req.PhysicalID)
	p.record("delete", req.PhysicalID)
	return nil
}

// Types lists the types currently holding objects (debugging aid).
func (c *Cloud) Types() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for t, m := range c.objects {
		if len(m) > 0 {
			out = append(out, t)
		}
	}
	sort.Strings(out)
	return out
}
