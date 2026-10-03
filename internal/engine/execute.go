package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/udaykishore-resu/strata/internal/provider"
	"github.com/udaykishore-resu/strata/internal/state"
	"github.com/udaykishore-resu/strata/pkg/template"
)

// Execute runs or resumes an operation the caller has leased. It returns an
// error only when the run could not continue for infrastructure reasons (a
// store failure, a lost lease, shutdown); the operation is then resumed by
// whichever worker next acquires the lease. Resource failures are recorded
// on the operation and stack and return nil.
func (e *Engine) Execute(ctx context.Context, op *state.Operation) error {
	stack, err := e.Store.GetStack(ctx, op.Stack)
	if errors.Is(err, state.ErrNotFound) && op.Kind == state.OpDelete {
		// A previous attempt deleted the stack but crashed before recording it.
		return e.finishOrphan(ctx, op, state.OpSucceeded, state.StackDeleted, "")
	}
	if err != nil {
		return fmt.Errorf("load stack: %w", err)
	}
	if stack.CurrentOperation != op.ID {
		return e.finishOrphan(ctx, op, state.OpFailed, stack.Status, "operation no longer holds the stack lock")
	}
	r := &run{
		e:     e,
		op:    op,
		stack: stack,
		log:   e.logger().With("stack", op.Stack, "operation", op.ID),
	}
	r.initMaps()
	switch op.Kind {
	case state.OpDeploy:
		return r.deploy(ctx)
	case state.OpDelete:
		return r.destroy(ctx)
	default:
		return e.finishOrphan(ctx, op, state.OpFailed, stack.Status, "unknown operation kind "+string(op.Kind))
	}
}

func (e *Engine) finishOrphan(ctx context.Context, op *state.Operation, status state.OperationStatus, result state.StackStatus, msg string) error {
	now := time.Now().UTC()
	op.Status, op.Result, op.Error, op.FinishedAt = status, result, msg, &now
	op.Checkpoint.Phase = state.PhaseDone
	return e.Store.SaveOperation(context.WithoutCancel(ctx), op)
}

// run holds the in-memory state of one operation execution. mu guards stack
// and op; provider calls happen outside the lock.
type run struct {
	e     *Engine
	op    *state.Operation
	stack *state.Stack
	cs    *state.ChangeSet
	mu    sync.Mutex
	log   *slog.Logger
}

func (r *run) cp() *state.Checkpoint { return &r.op.Checkpoint }

func (r *run) initMaps() {
	cp := r.cp()
	if cp.Done == nil {
		cp.Done = map[string]bool{}
	}
	if cp.InFlight == nil {
		cp.InFlight = map[string]state.Journal{}
	}
	if r.stack.Resources == nil {
		r.stack.Resources = map[string]*state.ResourceState{}
	}
}

// persist atomically saves stack and checkpoint. Caller holds r.mu.
// Persistence uses a detached context so progress is recorded even while
// the run is being cancelled.
func (r *run) persist(ctx context.Context) error {
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := r.e.Store.SaveProgress(pctx, r.stack, r.op); err != nil {
		return fmt.Errorf("save progress: %w", err)
	}
	return nil
}

func (r *run) lockedPersist(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.persist(ctx)
}

func (r *run) event(ctx context.Context, lid, typ, physical, status, reason string) {
	ev := state.Event{
		Stack: r.stack.Name, OperationID: r.op.ID, Timestamp: time.Now().UTC(),
		LogicalID: lid, Type: typ, PhysicalID: physical, Status: status, Reason: reason,
	}
	ectx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := r.e.Store.AppendEvents(ectx, ev); err != nil {
		r.log.Warn("append event failed", "err", err)
	}
	level := slog.LevelInfo
	if strings.HasSuffix(status, "FAILED") {
		level = slog.LevelWarn
	}
	r.log.Log(ctx, level, "resource event", "logicalId", lid, "type", typ, "physicalId", physical, "status", status, "reason", reason)
}

func (r *run) env() provider.Env { return r.e.env(r.stack.Name) }

// requestProps builds the property map sent to a provider: the applied
// properties plus the physical name and ownership labels.
func (r *run) requestProps(s provider.Schema, lid string, props map[string]any, name string) map[string]any {
	out := deepCopy(props)
	if s.NameProperty != "" && name != "" {
		out[s.NameProperty] = name
	}
	if s.Labels {
		labels := map[string]any{}
		if m, ok := out["labels"].(map[string]any); ok {
			for k, v := range m {
				labels[k] = v
			}
		}
		for k, v := range provider.OwnershipLabels(r.stack.Name, lid) {
			labels[k] = v
		}
		out["labels"] = labels
	}
	return out
}

func physicalName(s provider.Schema, rs *state.ResourceState) string {
	if s.NameProperty == "" || rs == nil {
		return ""
	}
	return rs.PhysicalID
}

// execResolver resolves intrinsics against applied state.
type execResolver struct {
	s      *state.Stack
	params map[string]any
}

func (x execResolver) Ref(id string) (any, error) {
	rs, ok := x.s.Resources[id]
	if !ok || rs.PhysicalID == "" {
		return nil, fmt.Errorf("resource %q has not been created", id)
	}
	return rs.PhysicalID, nil
}

func (x execResolver) GetAtt(id, attr string) (any, error) {
	rs, ok := x.s.Resources[id]
	if !ok {
		return nil, fmt.Errorf("resource %q has not been created", id)
	}
	v, ok := rs.Attributes[attr]
	if !ok {
		return nil, fmt.Errorf("resource %q has no attribute %q", id, attr)
	}
	return v, nil
}

func (x execResolver) Param(n string) (any, error) {
	v, ok := x.params[n]
	if !ok {
		return nil, fmt.Errorf("parameter %q has no value", n)
	}
	return v, nil
}

// ---------------------------------------------------------------- deploy

func (r *run) deploy(ctx context.Context) error {
	cs, err := r.e.Store.GetChangeSet(ctx, r.op.ChangeSetID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) && r.cp().Phase == "" {
			return r.abort(ctx, fmt.Errorf("load change set: %w", err))
		}
		// Transient: keep the checkpoint (and undo log) and let the operation be retried.
		return fmt.Errorf("load change set: %w", err)
	}
	r.cs = cs
	cp := r.cp()
	if cp.Phase == "" {
		cp.Phase = state.PhaseApply
		cp.PrevTemplate = r.stack.Template
		cp.PrevParameters = r.stack.Parameters
		cp.PrevOutputs = r.stack.Outputs
		if err := r.lockedPersist(ctx); err != nil {
			return err
		}
		r.event(ctx, "", "", "", "DEPLOY_IN_PROGRESS", "executing change set "+cs.ID)
		r.saveChangeSet(ctx, state.ChangeSetExecuting)
	}
	for {
		var err error
		switch cp.Phase {
		case state.PhaseApply:
			err = r.apply(ctx)
		case state.PhaseRollback:
			err = r.rollback(ctx)
		case state.PhaseCleanup:
			err = r.cleanup(ctx)
		case state.PhaseDone:
			return nil
		default:
			return r.abort(ctx, fmt.Errorf("unknown phase %q", cp.Phase))
		}
		if err != nil {
			if ctx.Err() != nil {
				return errInterrupted
			}
			return err
		}
	}
}

// abort ends an operation that failed before changing any resource.
func (r *run) abort(ctx context.Context, cause error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stack.Template == nil {
		r.stack.Status = state.StackRollbackComplete
	} else {
		r.stack.Status = state.StackReady
	}
	r.stack.StatusReason = cause.Error()
	r.stack.CurrentOperation = ""
	now := time.Now().UTC()
	r.op.Status, r.op.Result, r.op.Error, r.op.FinishedAt = state.OpFailed, r.stack.Status, cause.Error(), &now
	r.cp().Phase = state.PhaseDone
	return r.persist(ctx)
}

func (r *run) saveChangeSet(ctx context.Context, status state.ChangeSetStatus) {
	if r.cs == nil {
		return
	}
	r.cs.Status = status
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := r.e.Store.SaveChangeSet(sctx, r.cs); err != nil {
		r.log.Warn("save change set failed", "err", err)
	}
}

func (r *run) apply(ctx context.Context) error {
	failedReconcile, err := r.reconcileApply(ctx)
	if err != nil {
		return err
	}
	g, err := template.BuildGraph(r.cs.Template)
	if err != nil {
		return r.abort(ctx, err)
	}
	changes := map[string]state.Change{}
	for _, ch := range r.cs.Changes {
		if ch.Action != state.ActionDelete {
			changes[ch.LogicalID] = ch
		}
	}
	r.mu.Lock()
	// A step that failed before an interruption still requires rollback.
	failed := failedReconcile || len(r.cp().Failures) > 0
	r.mu.Unlock()
	if !failed {
		r.mu.Lock()
		done := copyBoolMap(r.cp().Done)
		r.mu.Unlock()
		var fatal error
		failed, fatal = runGraph(ctx, g.Order(), g.Deps, done, r.e.Concurrency, false,
			func(ctx context.Context, id string) error { return r.applyOne(ctx, changes[id]) })
		if fatal != nil {
			if errors.Is(fatal, errInterrupted) || ctx.Err() != nil {
				return errInterrupted
			}
			return fatal
		}
		if ctx.Err() != nil {
			return errInterrupted
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	cp := r.cp()
	cp.Done = map[string]bool{}
	if failed {
		cp.Phase = state.PhaseRollback
		r.stack.Status = state.StackRollingBack
		r.stack.StatusReason = strings.Join(cp.Failures, "; ")
		// The full error is already on the UPDATE_FAILED/CREATE_FAILED event
		// and in the final status; here, just name what failed.
		r.event(ctx, "", "", "", "ROLLBACK_IN_PROGRESS", "undoing changes after failure of "+failedIDs(cp.Failures))
	} else {
		r.enterCleanup()
		cp.Phase = state.PhaseCleanup
	}
	return r.persist(ctx)
}

func (r *run) applyOne(ctx context.Context, ch state.Change) error {
	lid := ch.LogicalID
	res := r.cs.Template.Resources[lid]
	p, err := r.e.Registry.Get(res.Type)
	if err != nil {
		return r.recordFailure(ctx, ch, res.Type, "", err, nil)
	}
	schema := p.Schema()

	r.mu.Lock()
	props, rerr := template.ResolveMap(res.Properties, execResolver{r.stack, r.cs.Parameters})
	cur := cloneState(r.stack.Resources[lid])
	r.mu.Unlock()
	if rerr != nil {
		return r.recordFailure(ctx, ch, res.Type, "", rerr, nil)
	}
	props = provider.ApplyDefaults(schema, props)

	switch ch.Action {
	case state.ActionNoOp:
		r.mu.Lock()
		defer r.mu.Unlock()
		if rs := r.stack.Resources[lid]; rs != nil {
			rs.DeletionPolicy = res.EffectiveDeletionPolicy()
		}
		r.cp().Done[lid] = true
		return r.persist(ctx)
	case state.ActionCreate, state.ActionReplace:
		if ch.Action == state.ActionCreate {
			cur = nil
		}
		return r.create(ctx, p, ch, res, props, cur)
	case state.ActionUpdate:
		if cur == nil {
			// The resource vanished from state (e.g. a failed rollback); create it.
			return r.create(ctx, p, state.Change{LogicalID: lid, Type: res.Type, Action: state.ActionCreate}, res, props, nil)
		}
		return r.update(ctx, p, ch, res, props, cur)
	}
	return r.recordFailure(ctx, ch, res.Type, "", fmt.Errorf("unexpected action %s", ch.Action), nil)
}

func (r *run) create(ctx context.Context, p provider.Provider, ch state.Change, res template.Resource, props map[string]any, prev *state.ResourceState) error {
	lid, schema := ch.LogicalID, p.Schema()
	name, explicit := "", false
	if schema.NameProperty != "" {
		name = provider.String(props, schema.NameProperty)
		explicit = name != ""
		if !explicit {
			name = provider.GenerateName(schema, r.stack.Name, lid)
		}
	}
	verb := "CREATE"
	if ch.Action == state.ActionReplace {
		verb = "REPLACE"
		if prev != nil && name != "" && name == prev.PhysicalID {
			return r.recordFailure(ctx, ch, res.Type, name, fmt.Errorf(
				"replacement must use a different %s than the existing %q; change it or let Strata generate one", schema.NameProperty, name), nil)
		}
	}
	req := provider.Request{Env: r.env(), LogicalID: lid, Properties: r.requestProps(schema, lid, props, name)}

	// A custom name may already be taken by a resource this stack does not
	// own. Check before creating so a collision can never be "rolled back" by
	// deleting someone else's resource, and so a journaled name is always ours.
	if explicit {
		var rd provider.Result
		rerr := r.e.callProvider(ctx, res.Type, "read", func(c context.Context) error {
			var e error
			rd, e = p.Read(c, provider.Request{Env: req.Env, LogicalID: lid, PhysicalID: name, Properties: req.Properties})
			return e
		})
		switch {
		case rerr == nil:
			if !schema.Labels || !provider.OwnedBy(rd.Observed, r.stack.Name, lid) {
				return r.recordFailure(ctx, ch, res.Type, name, fmt.Errorf(
					"%q already exists and is not managed by stack %q: %w", name, r.stack.Name, provider.ErrAlreadyExists), nil)
			}
			// Left over from an earlier run of this stack: adopt it below.
		case errors.Is(rerr, provider.ErrNotFound):
		default:
			if ctx.Err() != nil {
				return errInterrupted
			}
			return r.recordFailure(ctx, ch, res.Type, name, rerr, nil)
		}
	}

	r.mu.Lock()
	r.cp().InFlight[lid] = state.Journal{Action: ch.Action, Type: res.Type, PhysicalID: name, Prev: prev, StartedAt: time.Now().UTC()}
	err := r.persist(ctx)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	r.event(ctx, lid, res.Type, name, verb+"_IN_PROGRESS", "")

	var result provider.Result
	err = r.e.callProvider(ctx, res.Type, "create", func(c context.Context) error {
		var cerr error
		result, cerr = p.Create(c, req)
		return cerr
	})
	alreadyExisted := errors.Is(err, provider.ErrAlreadyExists)
	if alreadyExisted && name != "" {
		result, err = r.adopt(ctx, p, req, name)
	}
	if err != nil {
		if ctx.Err() != nil {
			return errInterrupted // keep the journal; resume will reconcile
		}
		var undo *state.UndoEntry
		if name != "" && !alreadyExisted {
			// The name was free (generated, or checked above), so anything
			// left behind by this failed create is ours: remove it on rollback.
			kind := state.UndoDeleteCreated
			if ch.Action == state.ActionReplace {
				kind = state.UndoRevertReplace
			}
			undo = &state.UndoEntry{Kind: kind, LogicalID: lid, Prev: prev, Created: &state.ResourceState{
				LogicalID: lid, Type: res.Type, PhysicalID: name, Properties: props, Status: state.ResourceFailed,
			}}
		}
		return r.recordFailure(ctx, ch, res.Type, name, err, undo)
	}

	if result.PhysicalID == "" {
		result.PhysicalID = name
	}
	ns := &state.ResourceState{
		LogicalID: lid, Type: res.Type, PhysicalID: result.PhysicalID,
		Properties: props, Attributes: result.Attributes, Status: state.ResourceReady,
		DeletionPolicy: res.EffectiveDeletionPolicy(), UpdatedAt: time.Now().UTC(),
	}
	kind := state.UndoDeleteCreated
	if ch.Action == state.ActionReplace {
		kind = state.UndoRevertReplace
	}
	r.mu.Lock()
	r.stack.Resources[lid] = ns
	cp := r.cp()
	cp.Undo = append(cp.Undo, state.UndoEntry{Kind: kind, LogicalID: lid, Created: cloneState(ns), Prev: prev})
	delete(cp.InFlight, lid)
	cp.Done[lid] = true
	err = r.persist(ctx)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	r.event(ctx, lid, res.Type, ns.PhysicalID, verb+"_COMPLETE", "")
	return nil
}

// adopt takes over an existing resource with the requested name when its
// ownership labels show it belongs to this stack and logical ID (left over
// from an interrupted run), then converges it to the desired properties.
func (r *run) adopt(ctx context.Context, p provider.Provider, req provider.Request, name string) (provider.Result, error) {
	var rd provider.Result
	err := r.e.callProvider(ctx, p.Schema().Type, "read", func(c context.Context) error {
		var e error
		rd, e = p.Read(c, provider.Request{Env: req.Env, LogicalID: req.LogicalID, PhysicalID: name})
		return e
	})
	if err != nil {
		return provider.Result{}, err
	}
	if !p.Schema().Labels || !provider.OwnedBy(rd.Observed, r.stack.Name, req.LogicalID) {
		return provider.Result{}, fmt.Errorf("%q already exists and is not managed by stack %q: %w", name, r.stack.Name, provider.ErrAlreadyExists)
	}
	r.log.Info("adopting existing resource owned by this stack", "logicalId", req.LogicalID, "physicalId", name)
	upd := req
	upd.PhysicalID = name
	upd.OldProperties = rd.Observed
	var res provider.Result
	err = r.e.callProvider(ctx, p.Schema().Type, "update", func(c context.Context) error {
		var e error
		res, e = p.Update(c, upd)
		return e
	})
	if res.PhysicalID == "" {
		res.PhysicalID = name
	}
	return res, err
}

func (r *run) update(ctx context.Context, p provider.Provider, ch state.Change, res template.Resource, props map[string]any, cur *state.ResourceState) error {
	lid, schema := ch.LogicalID, p.Schema()
	name := physicalName(schema, cur)

	if metadataOnly(ch) && cur.Status == state.ResourceReady {
		ns := cloneState(cur)
		ns.DeletionPolicy = res.EffectiveDeletionPolicy()
		ns.UpdatedAt = time.Now().UTC()
		r.mu.Lock()
		defer r.mu.Unlock()
		r.stack.Resources[lid] = ns
		cp := r.cp()
		cp.Undo = append(cp.Undo, state.UndoEntry{Kind: state.UndoRevertUpdate, LogicalID: lid, Created: cloneState(ns), Prev: cur})
		cp.Done[lid] = true
		return r.persist(ctx)
	}

	r.mu.Lock()
	r.cp().InFlight[lid] = state.Journal{Action: state.ActionUpdate, Type: res.Type, PhysicalID: cur.PhysicalID, StartedAt: time.Now().UTC()}
	err := r.persist(ctx)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	r.event(ctx, lid, res.Type, cur.PhysicalID, "UPDATE_IN_PROGRESS", "")

	req := provider.Request{
		Env: r.env(), LogicalID: lid, PhysicalID: cur.PhysicalID,
		Properties:    r.requestProps(schema, lid, props, name),
		OldProperties: r.requestProps(schema, lid, withLive(cur.Properties, ch.Live), name),
	}
	var result provider.Result
	err = r.e.callProvider(ctx, res.Type, "update", func(c context.Context) error {
		var uerr error
		result, uerr = p.Update(c, req)
		return uerr
	})
	if err != nil {
		if ctx.Err() != nil {
			return errInterrupted
		}
		attempted := cloneState(cur)
		attempted.Properties = props
		return r.recordFailure(ctx, ch, res.Type, cur.PhysicalID, err,
			&state.UndoEntry{Kind: state.UndoRevertUpdate, LogicalID: lid, Created: attempted, Prev: cur})
	}

	ns := cloneState(cur)
	ns.Properties = props
	ns.Attributes = mergeMaps(cur.Attributes, result.Attributes)
	ns.Status, ns.StatusReason = state.ResourceReady, ""
	ns.DeletionPolicy = res.EffectiveDeletionPolicy()
	ns.UpdatedAt = time.Now().UTC()
	r.mu.Lock()
	r.stack.Resources[lid] = ns
	cp := r.cp()
	cp.Undo = append(cp.Undo, state.UndoEntry{Kind: state.UndoRevertUpdate, LogicalID: lid, Created: cloneState(ns), Prev: cur})
	delete(cp.InFlight, lid)
	cp.Done[lid] = true
	err = r.persist(ctx)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	r.event(ctx, lid, res.Type, ns.PhysicalID, "UPDATE_COMPLETE", "")
	return nil
}

func (r *run) recordFailure(ctx context.Context, ch state.Change, typ, physical string, cause error, undo *state.UndoEntry) error {
	verb := strings.ToUpper(string(ch.Action))
	r.mu.Lock()
	cp := r.cp()
	delete(cp.InFlight, ch.LogicalID)
	if undo != nil {
		cp.Undo = append(cp.Undo, *undo)
	}
	cp.Failures = append(cp.Failures, fmt.Sprintf("%s: %v", ch.LogicalID, cause))
	err := r.persist(ctx)
	r.mu.Unlock()
	r.event(ctx, ch.LogicalID, typ, physical, verb+"_FAILED", cause.Error())
	if err != nil {
		return err
	}
	return stepFailed(cause)
}

// reconcileApply settles provider calls that were in flight when a previous
// worker died. A create that landed is adopted; anything else is retried.
func (r *run) reconcileApply(ctx context.Context) (failed bool, err error) {
	r.mu.Lock()
	ids := make([]string, 0, len(r.cp().InFlight))
	for id := range r.cp().InFlight {
		ids = append(ids, id)
	}
	r.mu.Unlock()
	sort.Strings(ids)

	for _, lid := range ids {
		r.mu.Lock()
		j := r.cp().InFlight[lid]
		r.mu.Unlock()
		if (j.Action != state.ActionCreate && j.Action != state.ActionReplace) || j.PhysicalID == "" {
			r.mu.Lock()
			delete(r.cp().InFlight, lid) // idempotent action: just redo it
			r.mu.Unlock()
			continue
		}
		p, perr := r.e.Registry.Get(j.Type)
		if perr != nil {
			return false, perr
		}
		var rd provider.Result
		rerr := r.e.callProvider(ctx, j.Type, "read", func(c context.Context) error {
			var e error
			rd, e = p.Read(c, provider.Request{Env: r.env(), LogicalID: lid, PhysicalID: j.PhysicalID})
			return e
		})
		if errors.Is(rerr, provider.ErrNotFound) {
			r.mu.Lock()
			delete(r.cp().InFlight, lid) // never landed: recreate
			r.mu.Unlock()
			continue
		}
		if rerr != nil {
			return false, fmt.Errorf("reconcile %s: %w", lid, rerr)
		}
		// Journaled names are generated or were verified free before the
		// call, so the resource is ours; labeled types are double-checked in
		// case something else claimed the name in between.
		if p.Schema().Labels && !provider.OwnedBy(rd.Observed, r.stack.Name, lid) {
			r.mu.Lock()
			delete(r.cp().InFlight, lid) // not ours: the redo will report the conflict
			r.mu.Unlock()
			continue
		}

		// The create landed before the crash. Converge it to the desired state.
		res := r.cs.Template.Resources[lid]
		r.mu.Lock()
		props, err := template.ResolveMap(res.Properties, execResolver{r.stack, r.cs.Parameters})
		r.mu.Unlock()
		if err != nil {
			return false, err
		}
		props = provider.ApplyDefaults(p.Schema(), props)
		upd := provider.Request{
			Env: r.env(), LogicalID: lid, PhysicalID: j.PhysicalID,
			Properties: r.requestProps(p.Schema(), lid, props, j.PhysicalID), OldProperties: rd.Observed,
		}
		var ur provider.Result
		uerr := r.e.callProvider(ctx, j.Type, "update", func(c context.Context) error {
			var e error
			ur, e = p.Update(c, upd)
			return e
		})
		if ctx.Err() != nil {
			return false, errInterrupted
		}
		ns := &state.ResourceState{
			LogicalID: lid, Type: j.Type, PhysicalID: j.PhysicalID, Properties: props,
			Attributes: mergeMaps(rd.Attributes, ur.Attributes), Status: state.ResourceReady,
			DeletionPolicy: res.EffectiveDeletionPolicy(), UpdatedAt: time.Now().UTC(),
		}
		kind := state.UndoDeleteCreated
		if j.Action == state.ActionReplace {
			kind = state.UndoRevertReplace
		}
		r.mu.Lock()
		cp := r.cp()
		delete(cp.InFlight, lid)
		cp.Undo = append(cp.Undo, state.UndoEntry{Kind: kind, LogicalID: lid, Created: cloneState(ns), Prev: j.Prev})
		if uerr != nil {
			ns.Status, ns.StatusReason = state.ResourceFailed, uerr.Error()
			cp.Failures = append(cp.Failures, fmt.Sprintf("%s: %v", lid, uerr))
			failed = true
		} else {
			cp.Done[lid] = true
		}
		r.stack.Resources[lid] = ns
		err = r.persist(ctx)
		r.mu.Unlock()
		if err != nil {
			return false, err
		}
		r.event(ctx, lid, j.Type, j.PhysicalID, "CREATE_RECOVERED", "adopted resource created by an interrupted run")
	}
	return failed, r.lockedPersist(ctx)
}

// enterCleanup moves replaced and removed resources to PendingCleanup,
// ordered so dependents are deleted before their dependencies. Caller holds r.mu.
func (r *run) enterCleanup() {
	items := append([]*state.ResourceState(nil), r.stack.PendingCleanup...)
	for _, u := range r.cp().Undo {
		if u.Kind == state.UndoRevertReplace && u.Prev != nil && u.Prev.PhysicalID != "" {
			items = append(items, u.Prev)
		}
	}
	for _, ch := range r.cs.Changes {
		if ch.Action != state.ActionDelete {
			continue
		}
		if rs := r.stack.Resources[ch.LogicalID]; rs != nil {
			items = append(items, rs)
			delete(r.stack.Resources, ch.LogicalID)
		}
	}
	r.stack.PendingCleanup = orderForDeletion(items, r.cp().PrevTemplate)
}

// orderForDeletion sorts resources dependents-first using the template they
// were deployed with; resources unknown to it go first.
func orderForDeletion(items []*state.ResourceState, tmpl *template.Template) []*state.ResourceState {
	rank := map[string]int{}
	if tmpl != nil {
		if g, err := template.BuildGraph(tmpl); err == nil {
			for i, id := range g.ReverseOrder() {
				rank[id] = i + 1
			}
		}
	}
	out := append([]*state.ResourceState(nil), items...)
	sort.SliceStable(out, func(i, j int) bool { return rank[out[i].LogicalID] < rank[out[j].LogicalID] })
	return out
}

func (r *run) cleanup(ctx context.Context) error {
	if err := r.drainPendingCleanup(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	outputs, oerr := resolveOutputs(r.cs.Template, r.stack, r.cs.Parameters)
	r.stack.Template = r.cs.Template
	r.stack.Parameters = r.cs.Parameters
	r.stack.Outputs = outputs
	r.stack.Status = state.StackReady
	r.stack.StatusReason = ""
	if n := len(r.stack.PendingCleanup); n > 0 {
		r.stack.StatusReason = fmt.Sprintf("%d replaced or removed resource(s) could not be deleted; they will be retried on the next deploy", n)
	}
	if oerr != nil {
		r.stack.StatusReason = strings.TrimPrefix(r.stack.StatusReason+"; outputs: "+oerr.Error(), "; ")
	}
	r.stack.CurrentOperation = ""
	now := time.Now().UTC()
	r.op.Status, r.op.Result, r.op.FinishedAt = state.OpSucceeded, state.StackReady, &now
	r.cp().Phase = state.PhaseDone
	err := r.persist(ctx)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	r.event(ctx, "", "", "", "DEPLOY_COMPLETE", r.stack.StatusReason)
	r.saveChangeSet(ctx, state.ChangeSetExecuted)
	return nil
}

// drainPendingCleanup deletes everything in PendingCleanup. Failures are
// recorded as events and the resource stays queued; they never fail the deploy.
func (r *run) drainPendingCleanup(ctx context.Context) error {
	r.mu.Lock()
	items := append([]*state.ResourceState(nil), r.stack.PendingCleanup...)
	r.mu.Unlock()
	var remaining []*state.ResourceState
	for i, it := range items {
		if ctx.Err() != nil {
			return errInterrupted
		}
		ok := r.deleteResource(ctx, it, "cleanup")
		if ctx.Err() != nil {
			return errInterrupted
		}
		if !ok {
			remaining = append(remaining, it)
		}
		r.mu.Lock()
		r.stack.PendingCleanup = append(append([]*state.ResourceState(nil), remaining...), items[i+1:]...)
		err := r.persist(ctx)
		r.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// deleteResource deletes one resource honoring its deletion policy and
// reports success. NotFound counts as success.
func (r *run) deleteResource(ctx context.Context, rs *state.ResourceState, why string) bool {
	if rs.DeletionPolicy == template.DeletionPolicyRetain {
		r.event(ctx, rs.LogicalID, rs.Type, rs.PhysicalID, "DELETE_SKIPPED", "deletionPolicy is Retain")
		return true
	}
	p, err := r.e.Registry.Get(rs.Type)
	if err != nil {
		r.event(ctx, rs.LogicalID, rs.Type, rs.PhysicalID, "DELETE_FAILED", err.Error())
		return false
	}
	r.event(ctx, rs.LogicalID, rs.Type, rs.PhysicalID, "DELETE_IN_PROGRESS", why)
	schema := p.Schema()
	req := provider.Request{
		Env: r.env(), LogicalID: rs.LogicalID, PhysicalID: rs.PhysicalID,
		Properties: r.requestProps(schema, rs.LogicalID, rs.Properties, physicalName(schema, rs)),
	}
	err = r.e.callProvider(ctx, rs.Type, "delete", func(c context.Context) error { return p.Delete(c, req) })
	if err != nil && !errors.Is(err, provider.ErrNotFound) {
		if ctx.Err() == nil {
			r.event(ctx, rs.LogicalID, rs.Type, rs.PhysicalID, "DELETE_FAILED", err.Error())
		}
		return false
	}
	r.event(ctx, rs.LogicalID, rs.Type, rs.PhysicalID, "DELETE_COMPLETE", why)
	return true
}

func resolveOutputs(t *template.Template, s *state.Stack, params map[string]any) (map[string]any, error) {
	out := map[string]any{}
	var errs []error
	for name, o := range t.Outputs {
		v, err := template.Resolve(o.Value, execResolver{s, params})
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			continue
		}
		out[name] = v
	}
	return out, errors.Join(errs...)
}

// -------------------------------------------------------------- rollback

func (r *run) rollback(ctx context.Context) error {
	for {
		r.mu.Lock()
		cp := r.cp()
		if cp.UndoPos >= len(cp.Undo) {
			r.mu.Unlock()
			break
		}
		u := cp.Undo[len(cp.Undo)-1-cp.UndoPos]
		r.mu.Unlock()

		err := r.undoOne(ctx, u)
		if ctx.Err() != nil {
			return errInterrupted
		}
		r.mu.Lock()
		cp = r.cp()
		switch {
		case err != nil:
			cp.RollbackErrors = append(cp.RollbackErrors, fmt.Sprintf("%s: %v", u.LogicalID, err))
			switch u.Kind {
			case state.UndoRevertUpdate:
				failedState := cloneState(u.Created)
				failedState.Status, failedState.StatusReason = state.ResourceFailed, "rollback failed: "+err.Error()
				r.stack.Resources[u.LogicalID] = failedState
			case state.UndoRevertReplace:
				r.restore(u.LogicalID, u.Prev)
				r.stack.PendingCleanup = append(r.stack.PendingCleanup, cloneState(u.Created))
			case state.UndoDeleteCreated:
				if rs := r.stack.Resources[u.LogicalID]; rs != nil && rs.PhysicalID == u.Created.PhysicalID {
					delete(r.stack.Resources, u.LogicalID)
				}
				r.stack.PendingCleanup = append(r.stack.PendingCleanup, cloneState(u.Created))
			}
		case u.Kind == state.UndoDeleteCreated:
			if rs := r.stack.Resources[u.LogicalID]; rs != nil && u.Created != nil && rs.PhysicalID == u.Created.PhysicalID {
				delete(r.stack.Resources, u.LogicalID)
			}
		default: // revert update / replace succeeded
			r.restore(u.LogicalID, u.Prev)
		}
		cp.UndoPos++
		perr := r.persist(ctx)
		r.mu.Unlock()
		if perr != nil {
			return perr
		}
	}

	r.mu.Lock()
	cp := r.cp()
	r.stack.Template = cp.PrevTemplate
	r.stack.Parameters = cp.PrevParameters
	r.stack.Outputs = cp.PrevOutputs
	status := state.StackRollbackComplete
	reason := "deploy failed and was rolled back: " + strings.Join(cp.Failures, "; ")
	if len(cp.RollbackErrors) > 0 {
		status = state.StackRollbackFailed
		reason += "; rollback errors: " + strings.Join(cp.RollbackErrors, "; ")
	}
	r.stack.Status, r.stack.StatusReason = status, reason
	r.stack.CurrentOperation = ""
	now := time.Now().UTC()
	r.op.Status, r.op.Result, r.op.Error, r.op.FinishedAt = state.OpFailed, status, reason, &now
	cp.Phase = state.PhaseDone
	err := r.persist(ctx)
	r.mu.Unlock()
	if err != nil {
		return err
	}
	r.event(ctx, "", "", "", string(status), reason)
	r.saveChangeSet(ctx, state.ChangeSetFailed)
	return nil
}

func (r *run) restore(lid string, prev *state.ResourceState) {
	if prev == nil {
		delete(r.stack.Resources, lid)
		return
	}
	r.stack.Resources[lid] = cloneState(prev)
}

func (r *run) undoOne(ctx context.Context, u state.UndoEntry) error {
	switch u.Kind {
	case state.UndoDeleteCreated, state.UndoRevertReplace:
		if u.Created == nil || u.Created.PhysicalID == "" {
			return nil
		}
		p, err := r.e.Registry.Get(u.Created.Type)
		if err != nil {
			return err
		}
		schema := p.Schema()
		r.event(ctx, u.LogicalID, u.Created.Type, u.Created.PhysicalID, "DELETE_IN_PROGRESS", "rollback")
		req := provider.Request{
			Env: r.env(), LogicalID: u.LogicalID, PhysicalID: u.Created.PhysicalID,
			Properties: r.requestProps(schema, u.LogicalID, u.Created.Properties, physicalName(schema, u.Created)),
		}
		err = r.e.callProvider(ctx, u.Created.Type, "delete", func(c context.Context) error { return p.Delete(c, req) })
		if err != nil && !errors.Is(err, provider.ErrNotFound) {
			r.event(ctx, u.LogicalID, u.Created.Type, u.Created.PhysicalID, "DELETE_FAILED", "rollback: "+err.Error())
			return err
		}
		r.event(ctx, u.LogicalID, u.Created.Type, u.Created.PhysicalID, "DELETE_COMPLETE", "rollback")
		return nil
	case state.UndoRevertUpdate:
		if u.Prev == nil {
			return nil
		}
		if u.Created != nil && u.Created.Status == state.ResourceReady && template.Equal(u.Prev.Properties, u.Created.Properties) {
			return nil // metadata-only change (e.g. deletionPolicy): nothing to revert in the cloud
		}
		p, err := r.e.Registry.Get(u.Prev.Type)
		if err != nil {
			return err
		}
		schema := p.Schema()
		name := physicalName(schema, u.Prev)
		r.event(ctx, u.LogicalID, u.Prev.Type, u.Prev.PhysicalID, "UPDATE_IN_PROGRESS", "rollback")
		old := map[string]any{}
		if u.Created != nil {
			old = u.Created.Properties
		}
		req := provider.Request{
			Env: r.env(), LogicalID: u.LogicalID, PhysicalID: u.Prev.PhysicalID,
			Properties:    r.requestProps(schema, u.LogicalID, u.Prev.Properties, name),
			OldProperties: r.requestProps(schema, u.LogicalID, old, name),
		}
		err = r.e.callProvider(ctx, u.Prev.Type, "update", func(c context.Context) error {
			_, e := p.Update(c, req)
			return e
		})
		if err != nil {
			r.event(ctx, u.LogicalID, u.Prev.Type, u.Prev.PhysicalID, "UPDATE_FAILED", "rollback: "+err.Error())
			return err
		}
		r.event(ctx, u.LogicalID, u.Prev.Type, u.Prev.PhysicalID, "UPDATE_COMPLETE", "rollback")
		return nil
	}
	return fmt.Errorf("unknown undo kind %q", u.Kind)
}

// ---------------------------------------------------------------- delete

func (r *run) destroy(ctx context.Context) error {
	cp := r.cp()
	if cp.Phase == "" {
		cp.Phase = state.PhaseDelete
		if err := r.lockedPersist(ctx); err != nil {
			return err
		}
		r.event(ctx, "", "", "", "DELETE_IN_PROGRESS", "")
	}
	if cp.Phase == state.PhaseDone {
		return nil
	}
	if err := r.drainPendingCleanup(ctx); err != nil {
		return err
	}

	r.mu.Lock()
	nodes := make([]string, 0, len(r.stack.Resources))
	for id := range r.stack.Resources {
		nodes = append(nodes, id)
	}
	deps := map[string][]string{}
	if r.stack.Template != nil {
		if g, err := template.BuildGraph(r.stack.Template); err == nil {
			deps = g.Dependents // delete dependents first
		}
	}
	done := copyBoolMap(cp.Done)
	r.mu.Unlock()
	sort.Strings(nodes)

	failed, fatal := runGraph(ctx, nodes, deps, done, r.e.Concurrency, true, func(ctx context.Context, id string) error {
		r.mu.Lock()
		rs := cloneState(r.stack.Resources[id])
		r.mu.Unlock()
		ok := r.deleteResource(ctx, rs, "stack delete")
		if ctx.Err() != nil {
			return errInterrupted
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		if !ok {
			if cur := r.stack.Resources[id]; cur != nil {
				cur.Status, cur.StatusReason = state.ResourceFailed, "delete failed"
			}
			r.cp().Failures = append(r.cp().Failures, id+": delete failed")
			if err := r.persist(ctx); err != nil {
				return err
			}
			return stepFailed(fmt.Errorf("delete %s failed", id))
		}
		delete(r.stack.Resources, id)
		r.cp().Done[id] = true
		return r.persist(ctx)
	})
	if fatal != nil {
		if ctx.Err() != nil {
			return errInterrupted
		}
		return fatal
	}
	if ctx.Err() != nil {
		return errInterrupted
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	cp.Phase = state.PhaseDone
	r.op.FinishedAt = &now
	if failed || len(r.stack.Resources) > 0 || len(r.stack.PendingCleanup) > 0 {
		reason := "stack delete failed: " + strings.Join(cp.Failures, "; ")
		if len(r.stack.PendingCleanup) > 0 {
			reason += fmt.Sprintf("; %d resource(s) pending cleanup", len(r.stack.PendingCleanup))
		}
		r.stack.Status, r.stack.StatusReason = state.StackDeleteFailed, reason
		r.stack.CurrentOperation = ""
		r.op.Status, r.op.Result, r.op.Error = state.OpFailed, state.StackDeleteFailed, reason
		if err := r.persist(ctx); err != nil {
			return err
		}
		r.event(ctx, "", "", "", string(state.StackDeleteFailed), reason)
		return nil
	}
	r.op.Status, r.op.Result = state.OpSucceeded, state.StackDeleted
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := r.e.Store.FinishDelete(fctx, r.stack, r.op); err != nil {
		return fmt.Errorf("finish delete: %w", err)
	}
	r.event(ctx, "", "", "", "DELETE_COMPLETE", "")
	return nil
}

// --------------------------------------------------------------- helpers

// metadataOnly reports whether every diff is an engine-level setting such as
// deletionPolicy (named in parentheses) rather than a cloud property.
func metadataOnly(ch state.Change) bool {
	if len(ch.Diffs) == 0 {
		return false
	}
	for _, d := range ch.Diffs {
		if !strings.HasPrefix(d.Name, "(") {
			return false
		}
	}
	return true
}

func cloneState(rs *state.ResourceState) *state.ResourceState {
	if rs == nil {
		return nil
	}
	b, _ := json.Marshal(rs)
	var out state.ResourceState
	_ = json.Unmarshal(b, &out)
	return &out
}

func deepCopy(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	v := template.Normalize(m)
	out, _ := v.(map[string]any)
	if out == nil {
		out = map[string]any{}
	}
	return out
}

func mergeMaps(a, b map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func copyBoolMap(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// withLive overlays refreshed (drifted) values on the applied properties, so
// a provider computing a patch from old→new sees the out-of-band change.
func withLive(applied, live map[string]any) map[string]any {
	if len(live) == 0 {
		return applied
	}
	out := make(map[string]any, len(applied)+len(live))
	for k, v := range applied {
		out[k] = v
	}
	for k, v := range live {
		out[k] = v
	}
	return out
}

// failedIDs extracts the logical IDs from "LogicalID: error" failure lines.
func failedIDs(failures []string) string {
	ids := make([]string, 0, len(failures))
	for _, f := range failures {
		id, _, _ := strings.Cut(f, ":")
		ids = append(ids, id)
	}
	return strings.Join(ids, ", ")
}
