package engine

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/udaykishore-resu/strata/internal/provider"
	"github.com/udaykishore-resu/strata/internal/state"
	"github.com/udaykishore-resu/strata/pkg/template"
)

// Drift statuses.
const (
	DriftInSync     = "IN_SYNC"
	DriftModified   = "MODIFIED"
	DriftDeleted    = "DELETED"
	DriftNotChecked = "NOT_CHECKED"
	DriftError      = "ERROR"
)

// DriftResult is the drift status of one resource.
type DriftResult struct {
	LogicalID  string               `json:"logicalId"`
	Type       string               `json:"type"`
	PhysicalID string               `json:"physicalId"`
	Status     string               `json:"status"`
	Diffs      []state.PropertyDiff `json:"diffs,omitempty"`
	Error      string               `json:"error,omitempty"`
}

// DriftReport summarizes drift for a stack.
type DriftReport struct {
	Stack     string        `json:"stack"`
	Drifted   bool          `json:"drifted"`
	Resources []DriftResult `json:"resources"`
}

// DetectDrift reads every resource from the cloud and compares the
// observable properties with what Strata last applied.
func (e *Engine) DetectDrift(ctx context.Context, stackName string) (*DriftReport, error) {
	st, err := e.Store.GetStack(ctx, stackName)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(st.Resources))
	for id := range st.Resources {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	results := make([]DriftResult, len(ids))
	sem := make(chan struct{}, max(1, e.Concurrency))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func(i int, rs *state.ResourceState) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = e.driftOne(ctx, st.Name, rs)
		}(i, st.Resources[id])
	}
	wg.Wait()

	rep := &DriftReport{Stack: st.Name, Resources: results}
	for _, r := range results {
		if r.Status == DriftModified || r.Status == DriftDeleted {
			rep.Drifted = true
		}
	}
	return rep, nil
}

func (e *Engine) driftOne(ctx context.Context, stack string, rs *state.ResourceState) DriftResult {
	out := DriftResult{LogicalID: rs.LogicalID, Type: rs.Type, PhysicalID: rs.PhysicalID}
	p, err := e.Registry.Get(rs.Type)
	if err != nil {
		out.Status, out.Error = DriftError, err.Error()
		return out
	}
	var res provider.Result
	err = e.callProvider(ctx, rs.Type, "read", func(c context.Context) error {
		var rerr error
		res, rerr = p.Read(c, provider.Request{Env: e.env(stack), LogicalID: rs.LogicalID, PhysicalID: rs.PhysicalID, Properties: rs.Properties})
		return rerr
	})
	switch {
	case errors.Is(err, provider.ErrNotFound):
		out.Status = DriftDeleted
		return out
	case err != nil:
		out.Status, out.Error = DriftError, err.Error()
		return out
	case res.Observed == nil:
		out.Status = DriftNotChecked
		return out
	}
	schema := p.Schema()
	keys := make([]string, 0, len(res.Observed))
	for k := range res.Observed {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if k == schema.NameProperty {
			continue
		}
		if _, known := schema.Properties[k]; !known {
			continue
		}
		obs, want := res.Observed[k], rs.Properties[k]
		if k == "labels" {
			obs = stripOwnership(obs)
			if want == nil {
				want = map[string]any{}
			}
		}
		if !template.Equal(obs, want) {
			out.Diffs = append(out.Diffs, state.PropertyDiff{Name: k, Old: want, New: obs})
		}
	}
	out.Status = DriftInSync
	if len(out.Diffs) > 0 {
		out.Status = DriftModified
	}
	return out
}

func stripOwnership(v any) any {
	m, ok := template.Normalize(v).(map[string]any)
	if !ok {
		return map[string]any{}
	}
	delete(m, provider.LabelStack)
	delete(m, provider.LabelLogicalID)
	return m
}
