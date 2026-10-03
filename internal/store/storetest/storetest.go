// Package storetest is a conformance suite every state.Store implementation
// must pass, so the in-memory and Postgres stores behave identically.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/udaykishore-resu/strata/internal/state"
	"github.com/udaykishore-resu/strata/pkg/template"
)

// Run executes the suite. newStore must return an empty store.
func Run(t *testing.T, newStore func(t *testing.T) state.Store) {
	t.Run("StackCAS", func(t *testing.T) { testStackCAS(t, newStore(t)) })
	t.Run("ChangeSets", func(t *testing.T) { testChangeSets(t, newStore(t)) })
	t.Run("OperationLeases", func(t *testing.T) { testLeases(t, newStore(t)) })
	t.Run("ConcurrentClaims", func(t *testing.T) { testConcurrentClaims(t, newStore(t)) })
	t.Run("ProgressAndDelete", func(t *testing.T) { testProgress(t, newStore(t)) })
	t.Run("Events", func(t *testing.T) { testEvents(t, newStore(t)) })
}

func testStackCAS(t *testing.T, s state.Store) {
	ctx := context.Background()
	st := &state.Stack{Name: "orders", Status: state.StackCreating, Resources: map[string]*state.ResourceState{}}
	if err := s.SaveStack(ctx, st); err != nil || st.Version != 1 {
		t.Fatalf("insert: %v v=%d", err, st.Version)
	}
	dup := &state.Stack{Name: "orders", Status: state.StackCreating}
	if err := s.SaveStack(ctx, dup); !errors.Is(err, state.ErrConflict) {
		t.Fatalf("duplicate insert should conflict, got %v", err)
	}
	a, _ := s.GetStack(ctx, "orders")
	b, _ := s.GetStack(ctx, "orders")
	a.Status = state.StackReady
	a.Template = &template.Template{FormatVersion: template.FormatVersion, Resources: map[string]template.Resource{"X": {Type: "gcp:x:Y"}}}
	if err := s.SaveStack(ctx, a); err != nil || a.Version != 2 {
		t.Fatalf("update: %v v=%d", err, a.Version)
	}
	b.Status = state.StackDeleting
	if err := s.SaveStack(ctx, b); !errors.Is(err, state.ErrConflict) {
		t.Fatalf("stale update should conflict, got %v", err)
	}
	got, err := s.GetStack(ctx, "orders")
	if err != nil || got.Status != state.StackReady || got.Version != 2 || got.Template.Resources["X"].Type != "gcp:x:Y" {
		t.Fatalf("get = %+v %v", got, err)
	}
	list, _ := s.ListStacks(ctx)
	if len(list) != 1 {
		t.Fatalf("list = %d", len(list))
	}
	if err := s.DeleteStack(ctx, "orders", 1); !errors.Is(err, state.ErrConflict) {
		t.Fatal("delete with stale version should conflict")
	}
	if err := s.DeleteStack(ctx, "orders", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetStack(ctx, "orders"); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func testChangeSets(t *testing.T, s state.Store) {
	ctx := context.Background()
	cs := &state.ChangeSet{ID: "cs-1", Stack: "orders", Status: state.ChangeSetReady, CreatedAt: time.Now().UTC(),
		Changes: []state.Change{{LogicalID: "A", Type: "gcp:x:Y", Action: state.ActionCreate}}}
	if err := s.CreateChangeSet(ctx, cs); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateChangeSet(ctx, cs); !errors.Is(err, state.ErrConflict) {
		t.Fatalf("duplicate change set should conflict, got %v", err)
	}
	cs.Status = state.ChangeSetExecuted
	if err := s.SaveChangeSet(ctx, cs); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetChangeSet(ctx, "cs-1")
	if err != nil || got.Status != state.ChangeSetExecuted || got.Changes[0].Action != state.ActionCreate {
		t.Fatalf("got %+v %v", got, err)
	}
	if _, err := s.GetChangeSet(ctx, "nope"); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func startOp(t *testing.T, s state.Store, stack, id string) *state.Operation {
	t.Helper()
	st := &state.Stack{Name: stack, Status: state.StackCreating, CurrentOperation: id, Resources: map[string]*state.ResourceState{}}
	op := &state.Operation{ID: id, Stack: stack, Kind: state.OpDeploy, Status: state.OpPending}
	if err := s.StartOperation(context.Background(), st, op); err != nil {
		t.Fatalf("start op: %v", err)
	}
	return op
}

func testLeases(t *testing.T, s state.Store) {
	ctx := context.Background()
	startOp(t, s, "orders", "op-1")
	// StartOperation on an existing stack with a stale version conflicts and enqueues nothing.
	if err := s.StartOperation(ctx, &state.Stack{Name: "orders"}, &state.Operation{ID: "op-x", Stack: "orders", Status: state.OpPending}); !errors.Is(err, state.ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if _, err := s.GetOperation(ctx, "op-x"); !errors.Is(err, state.ErrNotFound) {
		t.Fatal("failed StartOperation must not enqueue")
	}

	lease := 300 * time.Millisecond
	a, err := s.ClaimOperation(ctx, "worker-a", lease)
	if err != nil || a == nil || a.ID != "op-1" || a.Status != state.OpRunning || a.Attempts != 1 {
		t.Fatalf("claim = %+v %v", a, err)
	}
	if b, _ := s.ClaimOperation(ctx, "worker-b", lease); b != nil {
		t.Fatal("leased op must not be claimable")
	}
	if !strings.HasPrefix(a.LeaseOwner, "worker-a/") {
		t.Fatalf("lease token = %q", a.LeaseOwner)
	}
	if err := s.RenewLease(ctx, "op-1", a.LeaseOwner, lease); err != nil {
		t.Fatal(err)
	}
	if err := s.RenewLease(ctx, "op-1", "worker-b/1", lease); !errors.Is(err, state.ErrLeaseLost) {
		t.Fatalf("foreign renew should fail, got %v", err)
	}
	a.Checkpoint.Phase = state.PhaseApply
	if err := s.SaveOperation(ctx, a); err != nil {
		t.Fatal(err)
	}

	time.Sleep(lease + 200*time.Millisecond)
	b, err := s.ClaimOperation(ctx, "worker-b", lease)
	if err != nil || b == nil || b.LeaseOwner != "worker-b/2" || b.Attempts != 2 || b.Checkpoint.Phase != state.PhaseApply {
		t.Fatalf("reclaim = %+v %v", b, err)
	}
	// Fencing is per claim: the same worker ID with an older claim is rejected.
	if err := s.RenewLease(ctx, "op-1", "worker-b/1", lease); !errors.Is(err, state.ErrLeaseLost) {
		t.Fatalf("stale claim token should be fenced, got %v", err)
	}
	a.Checkpoint.Phase = state.PhaseDone
	if err := s.SaveOperation(ctx, a); !errors.Is(err, state.ErrLeaseLost) {
		t.Fatalf("stale owner write must be fenced, got %v", err)
	}
	// Releasing the lease (duration 0) makes it claimable immediately.
	if err := s.RenewLease(ctx, "op-1", b.LeaseOwner, 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	c, err := s.ClaimOperation(ctx, "worker-c", lease)
	if err != nil || c == nil || c.LeaseOwner != "worker-c/3" {
		t.Fatalf("claim after release = %+v %v", c, err)
	}
	c.Status = state.OpSucceeded
	if err := s.SaveOperation(ctx, c); err != nil {
		t.Fatal(err)
	}
	time.Sleep(lease + 200*time.Millisecond)
	if d, _ := s.ClaimOperation(ctx, "worker-d", lease); d != nil {
		t.Fatal("finished operations must not be claimable")
	}
}

func testConcurrentClaims(t *testing.T, s state.Store) {
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		startOp(t, s, fmt.Sprintf("stack-%d", i), fmt.Sprintf("op-%02d", i))
	}
	var mu sync.Mutex
	claimed := map[string]string{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				op, err := s.ClaimOperation(ctx, fmt.Sprintf("w%d", w), time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if op == nil {
					return
				}
				mu.Lock()
				if prev, dup := claimed[op.ID]; dup {
					t.Errorf("%s claimed by %s and w%d", op.ID, prev, w)
				}
				claimed[op.ID] = fmt.Sprintf("w%d", w)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if len(claimed) != 10 {
		t.Fatalf("claimed %d of 10", len(claimed))
	}
}

func testProgress(t *testing.T, s state.Store) {
	ctx := context.Background()
	startOp(t, s, "orders", "op-1")
	op, _ := s.ClaimOperation(ctx, "w", time.Minute)
	st, _ := s.GetStack(ctx, "orders")
	st.Resources["A"] = &state.ResourceState{LogicalID: "A", Type: "gcp:x:Y", PhysicalID: "a-1", Status: state.ResourceReady}
	op.Checkpoint.Done = map[string]bool{"A": true}
	if err := s.SaveProgress(ctx, st, op); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetStack(ctx, "orders")
	gop, _ := s.GetOperation(ctx, "op-1")
	if got.Resources["A"].PhysicalID != "a-1" || !gop.Checkpoint.Done["A"] || got.Version != st.Version {
		t.Fatalf("progress not saved: %+v %+v", got, gop.Checkpoint)
	}
	// A fenced-out worker cannot write stack state either (atomicity).
	stale := *op
	stale.LeaseOwner = "someone-else"
	st.Resources["B"] = &state.ResourceState{LogicalID: "B"}
	if err := s.SaveProgress(ctx, st, &stale); !errors.Is(err, state.ErrLeaseLost) {
		t.Fatalf("expected lease lost, got %v", err)
	}
	if got, _ := s.GetStack(ctx, "orders"); got.Resources["B"] != nil {
		t.Fatal("stack must not change when the lease check fails")
	}
	delete(st.Resources, "B")
	op.Status = state.OpSucceeded
	if err := s.FinishDelete(ctx, st, op); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetStack(ctx, "orders"); !errors.Is(err, state.ErrNotFound) {
		t.Fatal("stack should be deleted")
	}
	if gop, _ := s.GetOperation(ctx, "op-1"); gop.Status != state.OpSucceeded {
		t.Fatalf("op status = %s", gop.Status)
	}
}

func testEvents(t *testing.T, s state.Store) {
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := s.AppendEvents(ctx, state.Event{Stack: "orders", Status: fmt.Sprintf("S%d", i)}, state.Event{Stack: "other", Status: "X"}); err != nil {
			t.Fatal(err)
		}
	}
	evs, err := s.ListEvents(ctx, "orders", 0, 3)
	if err != nil || len(evs) != 3 || evs[0].Status != "S0" || evs[0].Timestamp.IsZero() {
		t.Fatalf("events = %+v %v", evs, err)
	}
	more, _ := s.ListEvents(ctx, "orders", evs[2].ID, 100)
	if len(more) != 2 || more[1].Status != "S4" {
		t.Fatalf("paged events = %+v", more)
	}
}
