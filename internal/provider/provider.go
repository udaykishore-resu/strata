// Package provider defines the contract every resource type implements and
// the registry the engine resolves types from. It mirrors the CloudFormation
// resource-provider model: a static schema plus Create/Read/Update/Delete
// handlers that each run to completion (polling long-running operations
// internally) or fail.
package provider

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/udaykishore-resu/strata/pkg/template"
)

// Sentinel errors providers wrap so the engine can make decisions.
var (
	ErrNotFound      = errors.New("resource not found")
	ErrAlreadyExists = errors.New("resource already exists")
)

// PropertyType is the static type of a property value.
type PropertyType string

const (
	TypeString PropertyType = "string"
	TypeInt    PropertyType = "int"
	TypeBool   PropertyType = "bool"
	TypeMap    PropertyType = "map"  // map of string -> string
	TypeList   PropertyType = "list" // list of strings
	TypeAny    PropertyType = "any"
)

// PropertySpec describes one input property.
type PropertySpec struct {
	Type        PropertyType `json:"type"`
	Required    bool         `json:"required,omitempty"`
	ForceNew    bool         `json:"forceNew,omitempty"` // change requires replacement
	Default     any          `json:"default,omitempty"`
	Description string       `json:"description,omitempty"`
}

// NameConstraints shape generated physical names.
type NameConstraints struct {
	MinLen int `json:"minLen,omitempty"`
	MaxLen int `json:"maxLen"`
}

// Schema is the static description of a resource type.
type Schema struct {
	Type        string                  `json:"type"`
	Description string                  `json:"description"`
	Properties  map[string]PropertySpec `json:"properties"`
	Attributes  map[string]string       `json:"attributes"`
	// NameProperty is the property holding the resource's physical name.
	// When the template omits it, the engine generates a unique name so the
	// resource can be replaced without a name collision. Empty means the
	// physical ID is derived by the provider (for example IAM bindings).
	NameProperty string          `json:"nameProperty,omitempty"`
	Name         NameConstraints `json:"nameConstraints,omitempty"`
	// Labels reports whether the type carries GCP labels. The engine adds
	// ownership labels (strata-stack, strata-lid) to labeled resources.
	Labels bool `json:"labels,omitempty"`
	// Shared marks project-level settings that many stacks (and people) can
	// hold at once, such as an enabled API. An existing one is adopted
	// instead of rejected, Create must be idempotent, and a rollback never
	// removes one that existed before the deploy.
	Shared bool `json:"shared,omitempty"`
}

// Env carries deployment-wide context to providers.
type Env struct {
	Project string
	Region  string
	Stack   string
}

// Request is the input to every handler.
type Request struct {
	Env           Env
	LogicalID     string
	PhysicalID    string         // empty on Create
	Properties    map[string]any // desired, fully resolved, defaults applied
	OldProperties map[string]any // previous properties (Update only)
}

// Result is the output of Create, Read and Update.
type Result struct {
	PhysicalID string
	Attributes map[string]any
	// Observed holds the provider's view of input properties (Read only).
	// Drift detection compares it with the applied properties; keys absent
	// from Observed are not checked.
	Observed map[string]any
}

// Provider implements one resource type.
//
// Contract: handlers run to completion (polling long-running operations),
// Delete treats a missing resource as success, Read returns ErrNotFound for
// a missing resource, and types without a NameProperty must make Create
// idempotent (repeating it yields the same single resource), because their
// physical IDs are derived from their properties.
type Provider interface {
	Schema() Schema
	Create(ctx context.Context, req Request) (Result, error)
	Read(ctx context.Context, req Request) (Result, error)
	Update(ctx context.Context, req Request) (Result, error)
	Delete(ctx context.Context, req Request) error
}

// Registry maps resource types to providers.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]Provider
}

// NewRegistry returns a registry containing the given providers.
func NewRegistry(ps ...Provider) *Registry {
	r := &Registry{providers: map[string]Provider{}}
	for _, p := range ps {
		r.Register(p)
	}
	return r
}

// Register adds a provider, panicking on duplicates (a programming error).
func (r *Registry) Register(p Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t := p.Schema().Type
	if _, dup := r.providers[t]; dup {
		panic("provider: duplicate registration for " + t)
	}
	r.providers[t] = p
}

// Get returns the provider for a type.
func (r *Registry) Get(typ string) (Provider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.providers[typ]
	if !ok {
		return nil, fmt.Errorf("unsupported resource type %q", typ)
	}
	return p, nil
}

// Schemas returns all schemas sorted by type.
func (r *Registry) Schemas() []Schema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Schema, 0, len(r.providers))
	for _, p := range r.providers {
		out = append(out, p.Schema())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out
}

// ValidateProperties statically checks template properties against a schema:
// unknown keys, missing required keys, and the types of literal values.
// Intrinsic values are skipped because they resolve at deploy time.
func ValidateProperties(s Schema, props map[string]any) []error {
	var errs []error
	for k, v := range props {
		spec, ok := s.Properties[k]
		if !ok {
			errs = append(errs, fmt.Errorf("unknown property %q", k))
			continue
		}
		if err := checkType(spec.Type, v); err != nil {
			errs = append(errs, fmt.Errorf("property %q: %w", k, err))
		}
	}
	for k, spec := range s.Properties {
		if spec.Required {
			if _, ok := props[k]; !ok {
				errs = append(errs, fmt.Errorf("missing required property %q", k))
			}
		}
	}
	sort.Slice(errs, func(i, j int) bool { return errs[i].Error() < errs[j].Error() })
	return errs
}

func checkType(t PropertyType, v any) error {
	if _, _, ok := template.AsIntrinsic(v); ok {
		return nil
	}
	if _, ok := v.(template.Unknown); ok {
		return nil
	}
	switch t {
	case TypeString:
		if _, ok := v.(string); !ok {
			return fmt.Errorf("want string, got %T", v)
		}
	case TypeInt:
		f, ok := toFloat(v)
		if !ok || f != float64(int64(f)) {
			return fmt.Errorf("want integer, got %v", v)
		}
	case TypeBool:
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("want bool, got %T", v)
		}
	case TypeMap:
		m, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("want object, got %T", v)
		}
		for k, e := range m {
			if _, _, isI := template.AsIntrinsic(e); isI {
				continue
			}
			if _, ok := e.(string); !ok {
				return fmt.Errorf("key %q: want string value, got %T", k, e)
			}
		}
	case TypeList:
		l, ok := v.([]any)
		if !ok {
			return fmt.Errorf("want list, got %T", v)
		}
		for i, e := range l {
			if _, _, isI := template.AsIntrinsic(e); isI {
				continue
			}
			if _, ok := e.(string); !ok {
				return fmt.Errorf("[%d]: want string, got %T", i, e)
			}
		}
	}
	return nil
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	}
	return 0, false
}

// ApplyDefaults returns props with schema defaults filled in.
func ApplyDefaults(s Schema, props map[string]any) map[string]any {
	out := make(map[string]any, len(props))
	for k, v := range props {
		out[k] = v
	}
	for k, spec := range s.Properties {
		if _, ok := out[k]; !ok && spec.Default != nil {
			out[k] = template.Normalize(spec.Default)
		}
	}
	return out
}

// GenerateName builds a unique, valid physical name of the form
// <stack>-<logicalid>-<random>, respecting the type's length limits.
func GenerateName(s Schema, stack, logicalID string) string {
	maxLen := s.Name.MaxLen
	if maxLen <= 0 {
		maxLen = 63
	}
	suffix := "-" + randomSuffix(6)
	base := sanitize(stack + "-" + logicalID)
	if budget := maxLen - len(suffix); len(base) > budget {
		base = strings.TrimRight(base[:budget], "-")
	}
	name := base + suffix
	for len(name) < s.Name.MinLen {
		name += randomSuffix(1)
	}
	return name
}

func sanitize(s string) string {
	var b strings.Builder
	prevDash := false
	for _, r := range strings.ToLower(s) {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			prevDash = false
		} else if !prevDash {
			b.WriteByte('-')
			prevDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" || out[0] < 'a' || out[0] > 'z' {
		out = "s" + out
	}
	return out
}

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func randomSuffix(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	for i := range buf {
		buf[i] = alphabet[int(buf[i])%len(alphabet)]
	}
	return string(buf)
}

// String, Int, Bool and StringMap read typed values from resolved properties.
func String(props map[string]any, key string) string {
	s, _ := props[key].(string)
	return s
}

func Int(props map[string]any, key string) int {
	f, _ := toFloat(props[key])
	return int(f)
}

func Bool(props map[string]any, key string) bool {
	b, _ := props[key].(bool)
	return b
}

func StringMap(props map[string]any, key string) map[string]string {
	out := map[string]string{}
	if m, ok := props[key].(map[string]any); ok {
		for k, v := range m {
			out[k] = fmt.Sprint(v)
		}
	}
	return out
}

func StringList(props map[string]any, key string) []string {
	var out []string
	if l, ok := props[key].([]any); ok {
		for _, v := range l {
			out = append(out, fmt.Sprint(v))
		}
	}
	return out
}

// Ownership label keys the engine attaches to labeled resources.
const (
	LabelStack     = "strata-stack"
	LabelLogicalID = "strata-lid"
)

// OwnershipLabels returns the labels identifying a resource's owner.
func OwnershipLabels(stack, logicalID string) map[string]string {
	lid := strings.ToLower(logicalID)
	if len(lid) > 63 {
		lid = lid[:63]
	}
	return map[string]string{LabelStack: stack, LabelLogicalID: lid}
}

// OwnedBy reports whether observed labels mark the resource as belonging to
// stack/logicalID.
func OwnedBy(observed map[string]any, stack, logicalID string) bool {
	labels, _ := observed["labels"].(map[string]any)
	want := OwnershipLabels(stack, logicalID)
	return labels != nil && labels[LabelStack] == want[LabelStack] && labels[LabelLogicalID] == want[LabelLogicalID]
}
