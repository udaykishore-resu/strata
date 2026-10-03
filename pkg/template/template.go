// Package template defines the Strata template format: the declarative
// document a stack is deployed from. It is the contract between the
// construct library (which synthesizes templates) and the engine (which
// plans and applies them).
package template

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// FormatVersion is the only template format version this engine accepts.
const FormatVersion = "2026-10-01"

// Deletion policies.
const (
	DeletionPolicyDelete = "Delete"
	DeletionPolicyRetain = "Retain"
)

// Pseudo parameters are always available to {"param": ...} without being
// declared in the template.
const (
	PseudoProject = "strata:project"
	PseudoRegion  = "strata:region"
	PseudoStack   = "strata:stack"
)

// Template is a complete stack definition.
type Template struct {
	FormatVersion string               `json:"formatVersion"`
	Description   string               `json:"description,omitempty"`
	Parameters    map[string]Parameter `json:"parameters,omitempty"`
	Resources     map[string]Resource  `json:"resources"`
	Outputs       map[string]Output    `json:"outputs,omitempty"`
}

// Parameter is an input supplied at deploy time.
type Parameter struct {
	Type        string `json:"type"` // string | number | bool
	Default     any    `json:"default,omitempty"`
	Allowed     []any  `json:"allowed,omitempty"`
	Description string `json:"description,omitempty"`
}

// Resource is one managed cloud resource.
type Resource struct {
	Type           string         `json:"type"`
	Properties     map[string]any `json:"properties,omitempty"`
	DependsOn      []string       `json:"dependsOn,omitempty"`
	DeletionPolicy string         `json:"deletionPolicy,omitempty"`
}

// Output is a value exported from the stack after deployment.
type Output struct {
	Value       any    `json:"value"`
	Description string `json:"description,omitempty"`
}

var (
	logicalIDRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,254}$`)
	typeRe      = regexp.MustCompile(`^[a-z0-9]+:[a-z0-9]+:[A-Z][A-Za-z0-9]*$`)
	paramNameRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,127}$`)
)

// Parse decodes a JSON template. Unknown top-level fields are rejected so
// typos fail loudly instead of being silently ignored.
func Parse(data []byte) (*Template, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var t Template
	if err := dec.Decode(&t); err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}
	if dec.More() {
		return nil, errors.New("parse template: trailing data after JSON document")
	}
	return &t, nil
}

// Clone returns a deep copy of the template.
func (t *Template) Clone() *Template {
	if t == nil {
		return nil
	}
	b, err := json.Marshal(t)
	if err != nil {
		panic(fmt.Sprintf("template: clone: %v", err))
	}
	var out Template
	if err := json.Unmarshal(b, &out); err != nil {
		panic(fmt.Sprintf("template: clone: %v", err))
	}
	return &out
}

// EffectiveDeletionPolicy returns the resource's deletion policy, defaulting to Delete.
func (r Resource) EffectiveDeletionPolicy() string {
	if r.DeletionPolicy == "" {
		return DeletionPolicyDelete
	}
	return r.DeletionPolicy
}

// Validate checks the structural correctness of a template: identifiers,
// references, parameter declarations and dependency cycles. It does not
// know about resource schemas; the engine validates properties separately.
func (t *Template) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if t.FormatVersion != FormatVersion {
		add("formatVersion must be %q, got %q", FormatVersion, t.FormatVersion)
	}
	if len(t.Resources) == 0 {
		add("template must declare at least one resource")
	}
	for name, p := range t.Parameters {
		if !paramNameRe.MatchString(name) {
			add("parameter %q: invalid name", name)
		}
		switch p.Type {
		case "string", "number", "bool":
		default:
			add("parameter %q: type must be string, number or bool", name)
		}
		if p.Default != nil {
			if err := CheckParamValue(p, p.Default); err != nil {
				add("parameter %q: default: %v", name, err)
			}
		}
	}

	for _, id := range sortedKeys(t.Resources) {
		r := t.Resources[id]
		if !logicalIDRe.MatchString(id) {
			add("resource %q: logical ID must be alphanumeric and start with a letter", id)
		}
		if !typeRe.MatchString(r.Type) {
			add("resource %q: invalid type %q (want provider:service:Kind)", id, r.Type)
		}
		switch r.DeletionPolicy {
		case "", DeletionPolicyDelete, DeletionPolicyRetain:
		default:
			add("resource %q: deletionPolicy must be Delete or Retain", id)
		}
		for _, d := range r.DependsOn {
			if _, ok := t.Resources[d]; !ok {
				add("resource %q: dependsOn references unknown resource %q", id, d)
			}
			if d == id {
				add("resource %q: depends on itself", id)
			}
		}
		if err := t.checkExpr(r.Properties, fmt.Sprintf("resource %q", id), id); err != nil {
			errs = append(errs, err)
		}
	}
	for _, name := range sortedKeys(t.Outputs) {
		if err := t.checkExpr(t.Outputs[name].Value, fmt.Sprintf("output %q", name), ""); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		if _, err := BuildGraph(t); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// checkExpr validates every intrinsic inside v.
func (t *Template) checkExpr(v any, where, self string) error {
	var errs []error
	err := Walk(v, func(kind string, arg any) error {
		switch kind {
		case KindRef:
			id, _ := arg.(string)
			if _, ok := t.Resources[id]; !ok {
				return fmt.Errorf("%s: ref to unknown resource %q", where, id)
			}
			if id == self {
				return fmt.Errorf("%s: references itself", where)
			}
		case KindGetAtt:
			id, _, _ := getAttArgs(arg)
			if _, ok := t.Resources[id]; !ok {
				return fmt.Errorf("%s: getAtt on unknown resource %q", where, id)
			}
			if id == self {
				return fmt.Errorf("%s: references itself", where)
			}
		case KindParam:
			name, _ := arg.(string)
			if _, ok := t.Parameters[name]; !ok && !IsPseudoParam(name) {
				return fmt.Errorf("%s: unknown parameter %q", where, name)
			}
		}
		return nil
	})
	if err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// IsPseudoParam reports whether name is a built-in parameter.
func IsPseudoParam(name string) bool {
	switch name {
	case PseudoProject, PseudoRegion, PseudoStack:
		return true
	}
	return false
}

// CheckParamValue validates a concrete value against a parameter declaration.
func CheckParamValue(p Parameter, v any) error {
	switch p.Type {
	case "string":
		if _, ok := v.(string); !ok {
			return fmt.Errorf("want string, got %T", v)
		}
	case "number":
		switch v.(type) {
		case float64, int, int64, json.Number:
		default:
			return fmt.Errorf("want number, got %T", v)
		}
	case "bool":
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("want bool, got %T", v)
		}
	}
	if len(p.Allowed) > 0 {
		for _, a := range p.Allowed {
			if Equal(a, v) {
				return nil
			}
		}
		parts := make([]string, len(p.Allowed))
		for i, a := range p.Allowed {
			parts[i] = fmt.Sprint(a)
		}
		return fmt.Errorf("value %v not in allowed values [%s]", v, strings.Join(parts, ", "))
	}
	return nil
}

// ResolveParameters merges supplied values with defaults and validates them.
// The returned map includes pseudo parameters.
func (t *Template) ResolveParameters(supplied map[string]any, project, region, stack string) (map[string]any, error) {
	var errs []error
	out := map[string]any{
		PseudoProject: project,
		PseudoRegion:  region,
		PseudoStack:   stack,
	}
	for name := range supplied {
		if _, ok := t.Parameters[name]; !ok {
			errs = append(errs, fmt.Errorf("parameter %q is not declared by the template", name))
		}
	}
	for _, name := range sortedKeys(t.Parameters) {
		p := t.Parameters[name]
		v, ok := supplied[name]
		if !ok {
			if p.Default == nil {
				errs = append(errs, fmt.Errorf("parameter %q is required", name))
				continue
			}
			v = p.Default
		}
		v = Normalize(v)
		if err := CheckParamValue(p, v); err != nil {
			errs = append(errs, fmt.Errorf("parameter %q: %w", name, err))
			continue
		}
		out[name] = v
	}
	return out, errors.Join(errs...)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
