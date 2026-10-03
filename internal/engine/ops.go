package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/udaykishore-resu/strata/internal/state"
)

// Errors returned when an operation cannot start (HTTP 409).
var (
	ErrBusy     = fmt.Errorf("stack has an operation in progress: %w", state.ErrConflict)
	ErrStale    = fmt.Errorf("stack changed since the change set was created; create a new change set: %w", state.ErrConflict)
	ErrConsumed = fmt.Errorf("change set was already executed or failed: %w", state.ErrConflict)
)

// StartDeploy locks the stack and enqueues execution of a change set.
func (e *Engine) StartDeploy(ctx context.Context, stackName, changeSetID, createdBy string) (*state.Operation, error) {
	cs, err := e.Store.GetChangeSet(ctx, changeSetID)
	if err != nil {
		return nil, err
	}
	if cs.Stack != stackName {
		return nil, state.ErrNotFound
	}
	if cs.Status != state.ChangeSetReady {
		return nil, ErrConsumed
	}
	st, err := e.Store.GetStack(ctx, stackName)
	switch {
	case errors.Is(err, state.ErrNotFound):
		if cs.BaseVersion != 0 {
			return nil, ErrStale
		}
		st = &state.Stack{Name: stackName, Resources: map[string]*state.ResourceState{}, Status: state.StackCreating}
	case err != nil:
		return nil, err
	default:
		if st.CurrentOperation != "" || st.Status.Busy() {
			return nil, ErrBusy
		}
		if st.Version != cs.BaseVersion {
			return nil, ErrStale
		}
		st.Status = state.StackUpdating
		if st.Template == nil {
			st.Status = state.StackCreating
		}
	}
	op := &state.Operation{
		ID: NewID("op"), Stack: stackName, Kind: state.OpDeploy, ChangeSetID: cs.ID,
		Status: state.OpPending, CreatedBy: createdBy,
	}
	st.CurrentOperation = op.ID
	st.StatusReason = ""
	if err := e.Store.StartOperation(ctx, st, op); err != nil {
		if errors.Is(err, state.ErrConflict) {
			return nil, ErrBusy
		}
		return nil, err
	}
	return op, nil
}

// StartDelete locks the stack and enqueues its deletion.
func (e *Engine) StartDelete(ctx context.Context, stackName, createdBy string) (*state.Operation, error) {
	st, err := e.Store.GetStack(ctx, stackName)
	if err != nil {
		return nil, err
	}
	if st.CurrentOperation != "" || st.Status.Busy() {
		return nil, ErrBusy
	}
	op := &state.Operation{
		ID: NewID("op"), Stack: stackName, Kind: state.OpDelete,
		Status: state.OpPending, CreatedBy: createdBy,
	}
	st.Status = state.StackDeleting
	st.StatusReason = ""
	st.CurrentOperation = op.ID
	if err := e.Store.StartOperation(ctx, st, op); err != nil {
		if errors.Is(err, state.ErrConflict) {
			return nil, ErrBusy
		}
		return nil, err
	}
	return op, nil
}
