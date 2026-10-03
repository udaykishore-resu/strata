// Package engine is Strata's deployment engine: it turns a template into a
// change set (plan), then applies that change set as a durable, resumable
// operation with dependency-ordered parallelism, automatic rollback on
// failure and a cleanup phase for replaced or removed resources.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/udaykishore-resu/strata/internal/provider"
	"github.com/udaykishore-resu/strata/internal/state"
	"github.com/udaykishore-resu/strata/pkg/template"
)

// Engine plans and executes stack operations.
type Engine struct {
	Store    state.Store
	Registry *provider.Registry
	Project  string
	Region   string

	// Concurrency bounds parallel resource actions within one operation.
	Concurrency int
	// ActionTimeout bounds a single provider call (including LRO polling).
	ActionTimeout time.Duration
	// DenyPublicMembers rejects IAM bindings to allUsers/allAuthenticatedUsers.
	DenyPublicMembers bool

	Log *slog.Logger
	// Observe, when set, is called after every provider call (metrics hook).
	Observe func(resourceType, action string, err error, d time.Duration)
}

// ValidationError is returned for invalid user input (HTTP 400).
type ValidationError struct{ Err error }

func (e *ValidationError) Error() string { return e.Err.Error() }
func (e *ValidationError) Unwrap() error { return e.Err }

var stackNameRe = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,38}[a-z0-9])?$`)

// ValidateStackName checks stack naming rules (label-safe, max 40 chars).
func ValidateStackName(name string) error {
	if !stackNameRe.MatchString(name) {
		return &ValidationError{fmt.Errorf("invalid stack name %q: use 1-40 lowercase letters, digits and hyphens, starting with a letter", name)}
	}
	return nil
}

func (e *Engine) env(stack string) provider.Env {
	return provider.Env{Project: e.Project, Region: e.Region, Stack: stack}
}

func (e *Engine) logger() *slog.Logger {
	if e.Log != nil {
		return e.Log
	}
	return slog.Default()
}

// NewID returns a sortable, unique identifier with the given prefix.
func NewID(prefix string) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%s-%s", prefix, time.Now().UTC().Format("20060102t150405"), hex.EncodeToString(b))
}

// Plan validates a template and computes the change set that would deploy it
// to the named stack. The change set is persisted and can be executed later.
func (e *Engine) Plan(ctx context.Context, stackName string, tmpl *template.Template, params map[string]any, createdBy string) (*state.ChangeSet, error) {
	if err := ValidateStackName(stackName); err != nil {
		return nil, err
	}
	if tmpl == nil {
		return nil, &ValidationError{errors.New("template is required")}
	}
	if err := tmpl.Validate(); err != nil {
		return nil, &ValidationError{err}
	}
	resolvedParams, err := tmpl.ResolveParameters(params, e.Project, e.Region, stackName)
	if err != nil {
		return nil, &ValidationError{err}
	}
	if err := e.validateResources(tmpl, resolvedParams); err != nil {
		return nil, &ValidationError{err}
	}

	cur, err := e.Store.GetStack(ctx, stackName)
	if errors.Is(err, state.ErrNotFound) {
		cur = nil
	} else if err != nil {
		return nil, err
	}

	changes, err := e.diff(cur, tmpl, resolvedParams)
	if err != nil {
		return nil, &ValidationError{err}
	}
	cs := &state.ChangeSet{
		ID:         NewID("cs"),
		Stack:      stackName,
		Template:   tmpl,
		Parameters: resolvedParams,
		Changes:    changes,
		Status:     state.ChangeSetReady,
		CreatedBy:  createdBy,
		CreatedAt:  time.Now().UTC(),
	}
	if cur != nil {
		cs.BaseVersion = cur.Version
	}
	if err := e.Store.CreateChangeSet(ctx, cs); err != nil {
		return nil, err
	}
	return cs, nil
}

// validateResources checks every resource against its provider schema.
func (e *Engine) validateResources(tmpl *template.Template, params map[string]any) error {
	var errs []error
	ids := make([]string, 0, len(tmpl.Resources))
	for id := range tmpl.Resources {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		res := tmpl.Resources[id]
		p, err := e.Registry.Get(res.Type)
		if err != nil {
			errs = append(errs, fmt.Errorf("resource %q: %w", id, err))
			continue
		}
		for _, perr := range provider.ValidateProperties(p.Schema(), res.Properties) {
			errs = append(errs, fmt.Errorf("resource %q: %w", id, perr))
		}
		// getAtt targets must name a real attribute.
		_ = template.Walk(res.Properties, func(kind string, arg any) error {
			if kind != template.KindGetAtt {
				return nil
			}
			l := arg.([]any)
			target, attr := l[0].(string), l[1].(string)
			if tr, ok := tmpl.Resources[target]; ok {
				if tp, err := e.Registry.Get(tr.Type); err == nil {
					if _, ok := tp.Schema().Attributes[attr]; !ok {
						errs = append(errs, fmt.Errorf("resource %q: %s has no attribute %q", id, tr.Type, attr))
					}
				}
			}
			return nil
		})
		if e.DenyPublicMembers {
			if m, ok := res.Properties["member"]; ok {
				if v, err := template.Resolve(m, paramOnly(params)); err == nil {
					if s, _ := v.(string); s == "allUsers" || s == "allAuthenticatedUsers" {
						errs = append(errs, fmt.Errorf("resource %q: public IAM member %q is denied by server policy", id, s))
					}
				}
			}
		}
	}
	return errors.Join(errs...)
}

// paramOnly resolves params and treats resource references as unknown.
type paramOnly map[string]any

func (p paramOnly) Ref(string) (any, error)            { return template.Unknown{}, nil }
func (p paramOnly) GetAtt(string, string) (any, error) { return template.Unknown{}, nil }
func (p paramOnly) Param(n string) (any, error)        { return p[n], nil }

// planResolver resolves references against current state; resources that
// will be (re)created resolve to Unknown.
type planResolver struct {
	cur     *state.Stack
	actions map[string]state.Action
	params  map[string]any
}

func (r *planResolver) lookup(id string) (*state.ResourceState, bool) {
	switch r.actions[id] {
	case state.ActionCreate, state.ActionReplace:
		return nil, false
	}
	if r.cur == nil {
		return nil, false
	}
	rs, ok := r.cur.Resources[id]
	return rs, ok
}

func (r *planResolver) Ref(id string) (any, error) {
	if rs, ok := r.lookup(id); ok {
		return rs.PhysicalID, nil
	}
	return template.Unknown{}, nil
}

func (r *planResolver) GetAtt(id, attr string) (any, error) {
	if rs, ok := r.lookup(id); ok {
		if v, ok := rs.Attributes[attr]; ok {
			return v, nil
		}
	}
	return template.Unknown{}, nil
}

func (r *planResolver) Param(n string) (any, error) { return r.params[n], nil }

// diff computes per-resource actions in dependency order, then deletions.
func (e *Engine) diff(cur *state.Stack, tmpl *template.Template, params map[string]any) ([]state.Change, error) {
	g, err := template.BuildGraph(tmpl)
	if err != nil {
		return nil, err
	}
	res := &planResolver{cur: cur, actions: map[string]state.Action{}, params: params}
	var changes []state.Change
	var errs []error

	for _, id := range g.Order() {
		tr := tmpl.Resources[id]
		p, err := e.Registry.Get(tr.Type)
		if err != nil {
			return nil, err
		}
		schema := p.Schema()
		var existing *state.ResourceState
		if cur != nil {
			existing = cur.Resources[id]
		}
		ch := state.Change{LogicalID: id, Type: tr.Type}

		desired, err := template.ResolveMap(tr.Properties, res)
		if err != nil {
			errs = append(errs, fmt.Errorf("resource %q: %w", id, err))
			continue
		}
		desired = provider.ApplyDefaults(schema, desired)

		switch {
		case existing == nil:
			ch.Action = state.ActionCreate
			for _, k := range sortedKeys(desired) {
				ch.Diffs = append(ch.Diffs, state.PropertyDiff{Name: k, New: desired[k], KnownLater: template.ContainsUnknown(desired[k])})
			}
		case existing.Type != tr.Type:
			ch.Action = state.ActionReplace
			ch.PhysicalID = existing.PhysicalID
			ch.Diffs = []state.PropertyDiff{{Name: "(type)", Old: existing.Type, New: tr.Type, ForcesNew: true}}
		default:
			ch.PhysicalID = existing.PhysicalID
			forceNew := false
			for _, k := range unionKeys(desired, existing.Properties) {
				nv, ov := desired[k], existing.Properties[k]
				unknown := template.ContainsUnknown(nv)
				if !unknown && template.Equal(nv, ov) {
					continue
				}
				d := state.PropertyDiff{Name: k, Old: ov, New: nv, KnownLater: unknown}
				if spec, ok := schema.Properties[k]; ok && spec.ForceNew {
					d.ForcesNew = true
					forceNew = true
				}
				ch.Diffs = append(ch.Diffs, d)
			}
			oldPolicy := existing.DeletionPolicy
			if oldPolicy == "" {
				oldPolicy = template.DeletionPolicyDelete
			}
			if newPolicy := tr.EffectiveDeletionPolicy(); newPolicy != oldPolicy {
				ch.Diffs = append(ch.Diffs, state.PropertyDiff{Name: "(deletionPolicy)", Old: oldPolicy, New: newPolicy})
			}
			switch {
			case len(ch.Diffs) == 0 && existing.Status == state.ResourceReady:
				ch.Action = state.ActionNoOp
			case forceNew:
				ch.Action = state.ActionReplace
				if schema.NameProperty != "" {
					if _, named := tr.Properties[schema.NameProperty]; named && !nameChanged(ch.Diffs, schema.NameProperty) {
						errs = append(errs, fmt.Errorf("resource %q: changing %s requires replacement, but the resource has a custom %s; change the %s too or remove it to let Strata generate one",
							id, forceNewNames(ch.Diffs), schema.NameProperty, schema.NameProperty))
					}
				}
			default:
				ch.Action = state.ActionUpdate
			}
		}
		res.actions[id] = ch.Action
		changes = append(changes, ch)
	}

	if cur != nil {
		var removed []string
		for id := range cur.Resources {
			if _, ok := tmpl.Resources[id]; !ok {
				removed = append(removed, id)
			}
		}
		sort.Strings(removed)
		for _, id := range removed {
			rs := cur.Resources[id]
			changes = append(changes, state.Change{LogicalID: id, Type: rs.Type, Action: state.ActionDelete, PhysicalID: rs.PhysicalID})
		}
	}
	return changes, errors.Join(errs...)
}

func nameChanged(diffs []state.PropertyDiff, nameProp string) bool {
	for _, d := range diffs {
		if d.Name == nameProp {
			return true
		}
	}
	return false
}

func forceNewNames(diffs []state.PropertyDiff) string {
	var names []string
	for _, d := range diffs {
		if d.ForcesNew {
			names = append(names, d.Name)
		}
	}
	return strings.Join(names, ", ")
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func unionKeys(a, b map[string]any) []string {
	set := map[string]bool{}
	for k := range a {
		set[k] = true
	}
	for k := range b {
		set[k] = true
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// callProvider wraps a provider call with timeout and metrics.
func (e *Engine) callProvider(ctx context.Context, typ, action string, fn func(context.Context) error) error {
	timeout := e.ActionTimeout
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()
	err := fn(cctx)
	if e.Observe != nil {
		e.Observe(typ, action, err, time.Since(start))
	}
	return err
}
