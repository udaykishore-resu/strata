package template

import (
	"fmt"
	"sort"
	"strings"
)

// Graph is the resource dependency graph of a template. An edge A -> B means
// A depends on B, so B must be created before A and deleted after A.
type Graph struct {
	Nodes      []string            // sorted logical IDs
	Deps       map[string][]string // node -> nodes it depends on
	Dependents map[string][]string // node -> nodes that depend on it
	order      []string
}

// BuildGraph derives dependencies from ref/getAtt usage plus explicit
// dependsOn, and rejects cycles.
func BuildGraph(t *Template) (*Graph, error) {
	g := &Graph{Deps: map[string][]string{}, Dependents: map[string][]string{}}
	for id := range t.Resources {
		g.Nodes = append(g.Nodes, id)
	}
	sort.Strings(g.Nodes)
	for _, id := range g.Nodes {
		r := t.Resources[id]
		set := map[string]bool{}
		for _, d := range References(r.Properties) {
			set[d] = true
		}
		for _, d := range r.DependsOn {
			set[d] = true
		}
		for d := range set {
			if _, ok := t.Resources[d]; !ok {
				return nil, fmt.Errorf("resource %q depends on unknown resource %q", id, d)
			}
			g.Deps[id] = append(g.Deps[id], d)
			g.Dependents[d] = append(g.Dependents[d], id)
		}
		sort.Strings(g.Deps[id])
	}
	for k := range g.Dependents {
		sort.Strings(g.Dependents[k])
	}
	order, err := g.topo()
	if err != nil {
		return nil, err
	}
	g.order = order
	return g, nil
}

// Order returns a deterministic topological order (dependencies first).
func (g *Graph) Order() []string { return append([]string(nil), g.order...) }

// ReverseOrder returns dependents first; the safe order for deletion.
func (g *Graph) ReverseOrder() []string {
	out := g.Order()
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func (g *Graph) topo() ([]string, error) {
	indeg := map[string]int{}
	for _, n := range g.Nodes {
		indeg[n] = len(g.Deps[n])
	}
	var ready []string
	for _, n := range g.Nodes {
		if indeg[n] == 0 {
			ready = append(ready, n)
		}
	}
	var order []string
	for len(ready) > 0 {
		sort.Strings(ready)
		n := ready[0]
		ready = ready[1:]
		order = append(order, n)
		for _, m := range g.Dependents[n] {
			indeg[m]--
			if indeg[m] == 0 {
				ready = append(ready, m)
			}
		}
	}
	if len(order) != len(g.Nodes) {
		var cyc []string
		for _, n := range g.Nodes {
			if indeg[n] > 0 {
				cyc = append(cyc, n)
			}
		}
		return nil, fmt.Errorf("dependency cycle between resources: %s", strings.Join(cyc, ", "))
	}
	return order, nil
}
