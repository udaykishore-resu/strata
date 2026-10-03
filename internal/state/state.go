// Package state holds the persistent data model (stacks, change sets,
// operations, events) and the Store interface the engine, API and worker
// share. Implementations live in internal/store.
package state

import (
	"context"
	"errors"
	"time"

	"github.com/udaykishore-resu/strata/pkg/template"
)

// Store errors.
var (
	ErrNotFound  = errors.New("not found")
	ErrConflict  = errors.New("conflict: object was modified concurrently")
	ErrLeaseLost = errors.New("operation lease lost")
)

// StackStatus is the lifecycle status of a stack.
type StackStatus string

const (
	StackCreating         StackStatus = "CREATING"
	StackUpdating         StackStatus = "UPDATING"
	StackReady            StackStatus = "READY"
	StackRollingBack      StackStatus = "ROLLING_BACK"
	StackRollbackComplete StackStatus = "ROLLBACK_COMPLETE"
	StackRollbackFailed   StackStatus = "ROLLBACK_FAILED"
	StackDeleting         StackStatus = "DELETING"
	StackDeleteFailed     StackStatus = "DELETE_FAILED"
	// StackDeleted is only used as an operation result; deleted stacks are removed.
	StackDeleted StackStatus = "DELETED"
)

// Busy reports whether an operation is in flight for the status.
func (s StackStatus) Busy() bool {
	switch s {
	case StackCreating, StackUpdating, StackRollingBack, StackDeleting:
		return true
	}
	return false
}

// ResourceStatus is the status of an individual resource.
type ResourceStatus string

const (
	ResourceReady  ResourceStatus = "READY"
	ResourceFailed ResourceStatus = "FAILED"
)

// Stack is the applied state of one stack.
type Stack struct {
	Name         string      `json:"name"`
	Version      int64       `json:"version"` // optimistic concurrency token; 0 = not yet stored
	Status       StackStatus `json:"status"`
	StatusReason string      `json:"statusReason,omitempty"`

	// Template and Parameters are the last successfully applied inputs.
	Template   *template.Template `json:"template,omitempty"`
	Parameters map[string]any     `json:"parameters,omitempty"`
	Outputs    map[string]any     `json:"outputs,omitempty"`

	Resources map[string]*ResourceState `json:"resources"`
	// PendingCleanup holds replaced or removed resources whose deletion
	// failed during a previous cleanup phase; they are retried next deploy.
	PendingCleanup []*ResourceState `json:"pendingCleanup,omitempty"`

	CurrentOperation string    `json:"currentOperation,omitempty"`
	CreatedAt        time.Time `json:"createdAt"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

// ResourceState is the applied state of one resource.
type ResourceState struct {
	LogicalID      string         `json:"logicalId"`
	Type           string         `json:"type"`
	PhysicalID     string         `json:"physicalId"`
	Properties     map[string]any `json:"properties"` // resolved inputs as applied
	Attributes     map[string]any `json:"attributes,omitempty"`
	Status         ResourceStatus `json:"status"`
	StatusReason   string         `json:"statusReason,omitempty"`
	DeletionPolicy string         `json:"deletionPolicy,omitempty"`
	UpdatedAt      time.Time      `json:"updatedAt"`
}

// Action is the planned action for a resource.
type Action string

const (
	ActionCreate  Action = "Create"
	ActionUpdate  Action = "Update"
	ActionReplace Action = "Replace"
	ActionDelete  Action = "Delete"
	ActionNoOp    Action = "NoOp"
)

// PropertyDiff describes one changed property in a plan.
type PropertyDiff struct {
	Name       string `json:"name"`
	Old        any    `json:"old,omitempty"`
	New        any    `json:"new,omitempty"`
	ForcesNew  bool   `json:"forcesNew,omitempty"`
	KnownLater bool   `json:"knownAfterApply,omitempty"`
}

// Change is one planned resource change.
type Change struct {
	LogicalID  string         `json:"logicalId"`
	Type       string         `json:"type"`
	Action     Action         `json:"action"`
	PhysicalID string         `json:"physicalId,omitempty"`
	Diffs      []PropertyDiff `json:"diffs,omitempty"`
}

// ChangeSetStatus is the status of a change set.
type ChangeSetStatus string

const (
	ChangeSetReady     ChangeSetStatus = "READY"
	ChangeSetExecuting ChangeSetStatus = "EXECUTING"
	ChangeSetExecuted  ChangeSetStatus = "EXECUTED"
	ChangeSetFailed    ChangeSetStatus = "FAILED"
)

// ChangeSet is a reviewed plan that can be executed once, and only against
// the stack version it was computed from.
type ChangeSet struct {
	ID          string             `json:"id"`
	Stack       string             `json:"stack"`
	BaseVersion int64              `json:"baseVersion"`
	Template    *template.Template `json:"template"`
	Parameters  map[string]any     `json:"parameters"`
	Changes     []Change           `json:"changes"`
	Status      ChangeSetStatus    `json:"status"`
	CreatedBy   string             `json:"createdBy,omitempty"`
	CreatedAt   time.Time          `json:"createdAt"`
}

// HasChanges reports whether executing the change set would do anything.
func (c *ChangeSet) HasChanges() bool {
	for _, ch := range c.Changes {
		if ch.Action != ActionNoOp {
			return true
		}
	}
	return false
}

// OperationKind is the kind of long-running operation.
type OperationKind string

const (
	OpDeploy OperationKind = "DEPLOY"
	OpDelete OperationKind = "DELETE"
)

// OperationStatus is the status of an operation.
type OperationStatus string

const (
	OpPending   OperationStatus = "PENDING"
	OpRunning   OperationStatus = "RUNNING"
	OpSucceeded OperationStatus = "SUCCEEDED"
	OpFailed    OperationStatus = "FAILED"
)

// Terminal reports whether the operation has finished.
func (s OperationStatus) Terminal() bool { return s == OpSucceeded || s == OpFailed }

// Operation is a durable unit of work executed by a worker under a lease.
type Operation struct {
	ID          string          `json:"id"`
	Stack       string          `json:"stack"`
	Kind        OperationKind   `json:"kind"`
	ChangeSetID string          `json:"changeSetId,omitempty"`
	Status      OperationStatus `json:"status"`
	// Result is the stack status the operation ended in.
	Result StackStatus `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`

	Checkpoint Checkpoint `json:"checkpoint"`

	LeaseOwner   string    `json:"leaseOwner,omitempty"`
	LeaseExpires time.Time `json:"leaseExpires,omitempty"`
	Attempts     int       `json:"attempts"`

	CreatedBy  string     `json:"createdBy,omitempty"`
	CreatedAt  time.Time  `json:"createdAt"`
	UpdatedAt  time.Time  `json:"updatedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
}

// Phases of an operation.
const (
	PhaseApply    = "apply"
	PhaseRollback = "rollback"
	PhaseCleanup  = "cleanup"
	PhaseDelete   = "delete"
	PhaseDone     = "done"
)

// Checkpoint is the durable progress of an operation, written after every
// resource step so a different worker can resume after a crash.
type Checkpoint struct {
	Phase string `json:"phase,omitempty"`

	// Snapshot of the stack before the operation, used to restore on rollback.
	PrevTemplate   *template.Template `json:"prevTemplate,omitempty"`
	PrevParameters map[string]any     `json:"prevParameters,omitempty"`
	PrevOutputs    map[string]any     `json:"prevOutputs,omitempty"`

	Done     map[string]bool    `json:"done,omitempty"`     // logical IDs finished in the current phase
	InFlight map[string]Journal `json:"inFlight,omitempty"` // intent recorded before each provider call
	Undo     []UndoEntry        `json:"undo,omitempty"`
	UndoPos  int                `json:"undoPos"` // number of undo entries already reverted (from the end)

	Failures       []string `json:"failures,omitempty"`       // apply/delete step failures
	RollbackErrors []string `json:"rollbackErrors,omitempty"` // undo steps that failed
}

// Journal records an intended provider call so a resumed operation can
// reconcile it (did the create land before the crash?).
type Journal struct {
	Action     Action         `json:"action"`
	Type       string         `json:"type"`
	PhysicalID string         `json:"physicalId"`
	Prev       *ResourceState `json:"prev,omitempty"` // state being replaced (Replace only)
	StartedAt  time.Time      `json:"startedAt"`
}

// UndoKind is how to revert one completed step.
type UndoKind string

const (
	UndoDeleteCreated UndoKind = "deleteCreated" // delete a resource the operation created
	UndoRevertUpdate  UndoKind = "revertUpdate"  // update back to Prev properties
	UndoRevertReplace UndoKind = "revertReplace" // delete the replacement, reinstate Prev
)

// UndoEntry reverts one completed step during rollback.
type UndoEntry struct {
	Kind      UndoKind       `json:"kind"`
	LogicalID string         `json:"logicalId"`
	Created   *ResourceState `json:"created,omitempty"` // the resource the step produced
	Prev      *ResourceState `json:"prev,omitempty"`    // the resource as it was before
}

// Event is an append-only audit record of a resource or stack transition.
type Event struct {
	ID          int64     `json:"id"`
	Stack       string    `json:"stack"`
	OperationID string    `json:"operationId,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	LogicalID   string    `json:"logicalId,omitempty"` // empty for stack-level events
	Type        string    `json:"type,omitempty"`
	PhysicalID  string    `json:"physicalId,omitempty"`
	Status      string    `json:"status"`
	Reason      string    `json:"reason,omitempty"`
}

// Store persists all engine state.
type Store interface {
	Ping(ctx context.Context) error

	GetStack(ctx context.Context, name string) (*Stack, error)
	ListStacks(ctx context.Context) ([]*Stack, error)
	// SaveStack writes s if its Version matches the stored version and
	// increments s.Version. Returns ErrConflict otherwise.
	SaveStack(ctx context.Context, s *Stack) error
	// DeleteStack removes the stack if its version matches.
	DeleteStack(ctx context.Context, name string, version int64) error

	CreateChangeSet(ctx context.Context, cs *ChangeSet) error
	GetChangeSet(ctx context.Context, id string) (*ChangeSet, error)
	SaveChangeSet(ctx context.Context, cs *ChangeSet) error

	// StartOperation atomically saves the stack (inserting it when
	// Version == 0, else compare-and-swap) and enqueues op.
	StartOperation(ctx context.Context, s *Stack, op *Operation) error
	GetOperation(ctx context.Context, id string) (*Operation, error)
	// SaveOperation persists op only while op.LeaseOwner still holds the lease.
	SaveOperation(ctx context.Context, op *Operation) error
	// SaveProgress atomically applies SaveStack(s) and SaveOperation(op), so
	// applied resource state and operation checkpoints never diverge.
	SaveProgress(ctx context.Context, s *Stack, op *Operation) error
	// FinishDelete atomically removes the stack and saves the final operation.
	FinishDelete(ctx context.Context, s *Stack, op *Operation) error
	// ClaimOperation leases the oldest runnable operation (pending, or
	// running with an expired lease). Returns nil, nil when there is none.
	// The returned op.LeaseOwner is a per-claim token ("<owner>/<attempt>")
	// that fences every later write and renewal.
	ClaimOperation(ctx context.Context, owner string, lease time.Duration) (*Operation, error)
	// RenewLease extends the lease held by token; a zero duration releases
	// it so another worker can resume immediately.
	RenewLease(ctx context.Context, id, token string, lease time.Duration) error

	AppendEvents(ctx context.Context, events ...Event) error
	ListEvents(ctx context.Context, stack string, afterID int64, limit int) ([]Event, error)
}
