// Package cdk is Strata's construct library: define GCP infrastructure as
// Go code and synthesize it into Strata templates, the same model as the
// AWS CDK (App → Stack → constructs) without CloudFormation or Terraform.
//
//	app := cdk.NewApp()
//	stack := cdk.NewStack(app, "orders", nil)
//	bucket := storage.NewBucket(stack, "Uploads", &storage.BucketProps{Versioned: true})
//	api := run.NewService(stack, "Api", &run.ServiceProps{Image: "gcr.io/p/api:1"})
//	bucket.GrantReadWrite(api.ServiceAccount())
//	app.Synth("strata.out")
package cdk

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/udaykishore-resu/strata/pkg/template"
)

// Construct is any node in the construct tree.
type Construct interface {
	Node() *Node
}

// Node is a construct's position in the tree.
type Node struct {
	id       string
	parent   *Node
	children map[string]*Node
	stack    *Stack
	app      *App
}

var idRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*$`)

func newNode(scope Construct, id string) *Node {
	if scope == nil {
		panic("cdk: scope must not be nil")
	}
	if !idRe.MatchString(id) {
		panic(fmt.Sprintf("cdk: construct id %q must be alphanumeric and start with a letter", id))
	}
	parent := scope.Node()
	if _, dup := parent.children[id]; dup {
		panic(fmt.Sprintf("cdk: there is already a construct named %q in %q", id, parent.Path()))
	}
	n := &Node{id: id, parent: parent, children: map[string]*Node{}, stack: parent.stack, app: parent.app}
	parent.children[id] = n
	return n
}

// ID returns the construct id.
func (n *Node) ID() string { return n.id }

// Path returns the slash-separated path from the app root.
func (n *Node) Path() string {
	var parts []string
	for c := n; c != nil && c.parent != nil; c = c.parent {
		parts = append([]string{c.id}, parts...)
	}
	return strings.Join(parts, "/")
}

// Stack returns the stack containing the node.
func (n *Node) Stack() *Stack { return n.stack }

// NewConstruct creates a plain grouping construct (for building your own
// higher-level constructs).
func NewConstruct(scope Construct, id string) Construct {
	return &group{node: newNode(scope, id)}
}

type group struct{ node *Node }

func (g *group) Node() *Node { return g.node }

// App is the root of a construct tree.
type App struct {
	node   *Node
	stacks []*Stack
}

// NewApp creates an app.
func NewApp() *App {
	a := &App{}
	a.node = &Node{children: map[string]*Node{}, app: a}
	return a
}

func (a *App) Node() *Node { return a.node }

// Stacks returns the app's stacks in creation order.
func (a *App) Stacks() []*Stack { return append([]*Stack(nil), a.stacks...) }

// Synth validates every stack and writes <dir>/<stack>.template.json.
func (a *App) Synth(dir string) ([]string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	var paths []string
	for _, s := range a.stacks {
		b, err := s.Synth()
		if err != nil {
			return nil, fmt.Errorf("stack %s: %w", s.name, err)
		}
		p := filepath.Join(dir, s.name+".template.json")
		if err := os.WriteFile(p, b, 0o644); err != nil {
			return nil, err
		}
		paths = append(paths, p)
	}
	return paths, nil
}

// StackProps configures a stack.
type StackProps struct {
	Description string
}

// Stack is a unit of deployment: it synthesizes to one template.
type Stack struct {
	node        *Node
	name        string
	props       StackProps
	resources   []*Resource
	params      map[string]template.Parameter
	outputs     map[string]template.Output
	services    map[string]*Resource
	validations []func(*template.Template) error
}

var stackNameRe = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,38}[a-z0-9])?$`)

// NewStack adds a stack to the app. Names are 1-40 lowercase letters,
// digits and hyphens.
func NewStack(app *App, name string, props *StackProps) *Stack {
	if !stackNameRe.MatchString(name) {
		panic(fmt.Sprintf("cdk: invalid stack name %q", name))
	}
	for _, s := range app.stacks {
		if s.name == name {
			panic(fmt.Sprintf("cdk: duplicate stack %q", name))
		}
	}
	s := &Stack{name: name, params: map[string]template.Parameter{}, outputs: map[string]template.Output{}, services: map[string]*Resource{}}
	if props != nil {
		s.props = *props
	}
	s.node = &Node{id: name, parent: app.node, children: map[string]*Node{}, stack: s, app: app}
	app.node.children[name] = s.node
	app.stacks = append(app.stacks, s)
	return s
}

func (s *Stack) Node() *Node { return s.node }

// Name returns the stack name.
func (s *Stack) Name() string { return s.name }

// StackOf returns the stack that contains c.
func StackOf(c Construct) *Stack {
	st := c.Node().stack
	if st == nil {
		panic("cdk: construct is not inside a stack")
	}
	return st
}

// AddParameter declares a deploy-time parameter and returns a token for it.
func (s *Stack) AddParameter(name string, p template.Parameter) any {
	if _, dup := s.params[name]; dup {
		panic(fmt.Sprintf("cdk: duplicate parameter %q", name))
	}
	s.params[name] = p
	return Param(name)
}

// AddOutput exports a value (literal or token) after deployment.
func (s *Stack) AddOutput(name string, value any, description string) {
	if _, dup := s.outputs[name]; dup {
		panic(fmt.Sprintf("cdk: duplicate output %q", name))
	}
	s.outputs[name] = template.Output{Value: value, Description: description}
}

// AddValidation registers a check run at synth time (policy as code).
func (s *Stack) AddValidation(fn func(*template.Template) error) {
	s.validations = append(s.validations, fn)
}

// RequireService ensures a Google API is enabled by the stack and returns
// the enabling resource; constructs depend on it so APIs are on first.
func (s *Stack) RequireService(api string) *Resource {
	if r, ok := s.services[api]; ok {
		return r
	}
	first := strings.SplitN(api, ".", 2)[0]
	id := "Api" + pascal(first)
	for i := 2; s.node.children[id] != nil; i++ {
		id = fmt.Sprintf("Api%s%d", pascal(first), i)
	}
	r := NewResource(s, id, "gcp:serviceusage:Service", map[string]any{"service": api})
	s.services[api] = r
	return r
}

// Template builds and validates the stack's template.
func (s *Stack) Template() (*template.Template, error) {
	t := &template.Template{
		FormatVersion: template.FormatVersion,
		Description:   s.props.Description,
		Resources:     map[string]template.Resource{},
	}
	if len(s.params) > 0 {
		t.Parameters = s.params
	}
	if len(s.outputs) > 0 {
		t.Outputs = s.outputs
	}
	for _, r := range s.resources {
		lid := r.LogicalID()
		if _, dup := t.Resources[lid]; dup {
			return nil, fmt.Errorf("logical ID collision on %q (%s)", lid, r.node.Path())
		}
		var deps []string
		seen := map[string]bool{}
		for _, d := range r.deps {
			if d.node.stack != s {
				return nil, fmt.Errorf("%s depends on %s in another stack; cross-stack references are not supported", r.node.Path(), d.node.Path())
			}
			if id := d.LogicalID(); !seen[id] {
				seen[id] = true
				deps = append(deps, id)
			}
		}
		sort.Strings(deps)
		props := map[string]any{}
		for k, v := range r.props {
			if v != nil {
				props[k] = v
			}
		}
		if len(props) == 0 {
			props = nil
		}
		t.Resources[lid] = template.Resource{Type: r.Type, Properties: props, DependsOn: deps, DeletionPolicy: r.DeletionPolicy}
	}
	// Round-trip through JSON so tokens and typed values become plain JSON.
	b, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	out, err := template.Parse(b)
	if err != nil {
		return nil, err
	}
	var errs []error
	if err := out.Validate(); err != nil {
		errs = append(errs, err)
	}
	for _, v := range s.validations {
		if err := v(out); err != nil {
			errs = append(errs, err)
		}
	}
	return out, errors.Join(errs...)
}

// Synth returns the template as indented JSON.
func (s *Stack) Synth() ([]byte, error) {
	t, err := s.Template()
	if err != nil {
		return nil, err
	}
	b, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// Resource is a low-level (L1) resource: a template resource of any type.
type Resource struct {
	node           *Node
	Type           string
	props          map[string]any
	deps           []*Resource
	DeletionPolicy string
}

// NewResource adds a resource of type typ to scope.
func NewResource(scope Construct, id, typ string, props map[string]any) *Resource {
	n := newNode(scope, id)
	if n.stack == nil {
		panic("cdk: resources must be created inside a stack")
	}
	if props == nil {
		props = map[string]any{}
	}
	r := &Resource{node: n, Type: typ, props: props}
	n.stack.resources = append(n.stack.resources, r)
	return r
}

func (r *Resource) Node() *Node { return r.node }

// LogicalID derives a stable logical ID from the construct path. Top-level
// resources use their id; nested resources append a short path hash.
func (r *Resource) LogicalID() string {
	var parts []string
	for c := r.node; c != nil && c != c.stack.node; c = c.parent {
		parts = append([]string{c.id}, parts...)
	}
	if len(parts) == 1 {
		return parts[0]
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "/")))
	id := strings.Join(parts, "") + strings.ToUpper(hex.EncodeToString(sum[:4]))
	if len(id) > 255 {
		id = id[len(id)-255:]
		id = "R" + id[1:]
	}
	return id
}

// Ref returns a token for the resource's physical ID.
func (r *Resource) Ref() any { return map[string]any{template.KindRef: r.LogicalID()} }

// Attr returns a token for one of the resource's attributes.
func (r *Resource) Attr(name string) any {
	return map[string]any{template.KindGetAtt: []any{r.LogicalID(), name}}
}

// Set sets a property (nil removes it).
func (r *Resource) Set(key string, value any) { r.props[key] = value }

// Property returns a property value.
func (r *Resource) Property(key string) any { return r.props[key] }

// AddDependency makes r deploy after others (and delete before them).
func (r *Resource) AddDependency(others ...*Resource) {
	for _, o := range others {
		if o != nil && o != r {
			r.deps = append(r.deps, o)
		}
	}
}

// Retain keeps the cloud resource when it is removed from the stack.
func (r *Resource) Retain() { r.DeletionPolicy = template.DeletionPolicyRetain }

// Param returns a token for a parameter or pseudo parameter.
func Param(name string) any { return map[string]any{template.KindParam: name} }

// Join returns a token that concatenates parts (literals or tokens).
func Join(sep string, parts ...any) any {
	return map[string]any{template.KindJoin: []any{sep, parts}}
}

// Pseudo parameter tokens.
var (
	ProjectID = Param(template.PseudoProject)
	Region    = Param(template.PseudoRegion)
	StackName = Param(template.PseudoStack)
)

// Grantee is anything that can be granted IAM roles.
type Grantee interface {
	GrantMember() any
}

// Principal is a literal IAM member such as "user:alice@example.com",
// "group:devs@example.com" or "serviceAccount:x@p.iam.gserviceaccount.com".
type Principal string

func (p Principal) GrantMember() any { return string(p) }

// AllUsers is the public principal (use with care).
const AllUsers = Principal("allUsers")

// GrantID returns a readable, unique child id for a grant of role on scope.
func GrantID(scope Construct, role string) string {
	base := role[strings.LastIndex(role, "/")+1:]
	base = strings.TrimPrefix(base, "roles.")
	id := pascal(strings.ReplaceAll(base, ".", "-")) + "Grant"
	for i := 2; scope.Node().children[id] != nil; i++ {
		id = fmt.Sprintf("%sGrant%d", pascal(strings.ReplaceAll(base, ".", "-")), i)
	}
	return id
}

func pascal(s string) string {
	var b strings.Builder
	up := true
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			if up {
				r -= 'a' - 'A'
			}
			b.WriteRune(r)
			up = false
		case r >= 'A' && r <= 'Z' || r >= '0' && r <= '9':
			b.WriteRune(r)
			up = false
		default:
			up = true
		}
	}
	return b.String()
}
