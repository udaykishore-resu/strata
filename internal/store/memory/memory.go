// Package memory is an in-process state.Store for development and tests.
// It has the same concurrency semantics as the Postgres store (version
// compare-and-swap, operation leases) but does not survive restarts.
package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/udaykishore-resu/strata/internal/state"
)

// Store is an in-memory state.Store.
type Store struct {
	mu         sync.Mutex
	stacks     map[string]*state.Stack
	changeSets map[string]*state.ChangeSet
	ops        map[string]*state.Operation
	events     []state.Event
	nextEvent  int64
	now        func() time.Time
}

// New returns an empty store.
func New() *Store {
	return &Store{
		stacks:     map[string]*state.Stack{},
		changeSets: map[string]*state.ChangeSet{},
		ops:        map[string]*state.Operation{},
		now:        time.Now,
	}
}

func clone[T any](v *T) *T {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		panic(err)
	}
	return &out
}

func (s *Store) Ping(context.Context) error { return nil }

func (s *Store) GetStack(_ context.Context, name string) (*state.Stack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.stacks[name]
	if !ok {
		return nil, state.ErrNotFound
	}
	return clone(st), nil
}

func (s *Store) ListStacks(context.Context) ([]*state.Stack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*state.Stack, 0, len(s.stacks))
	for _, st := range s.stacks {
		out = append(out, clone(st))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *Store) saveStackLocked(st *state.Stack) error {
	cur, exists := s.stacks[st.Name]
	switch {
	case st.Version == 0 && exists:
		return state.ErrConflict
	case st.Version != 0 && (!exists || cur.Version != st.Version):
		return state.ErrConflict
	}
	now := s.now().UTC()
	if st.Version == 0 {
		st.CreatedAt = now
	}
	st.UpdatedAt = now
	st.Version++
	s.stacks[st.Name] = clone(st)
	return nil
}

func (s *Store) SaveStack(_ context.Context, st *state.Stack) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveStackLocked(st)
}

func (s *Store) DeleteStack(_ context.Context, name string, version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.stacks[name]
	if !ok || cur.Version != version {
		return state.ErrConflict
	}
	delete(s.stacks, name)
	return nil
}

func (s *Store) CreateChangeSet(_ context.Context, cs *state.ChangeSet) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.changeSets[cs.ID]; dup {
		return state.ErrConflict
	}
	s.changeSets[cs.ID] = clone(cs)
	return nil
}

func (s *Store) GetChangeSet(_ context.Context, id string) (*state.ChangeSet, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cs, ok := s.changeSets[id]
	if !ok {
		return nil, state.ErrNotFound
	}
	return clone(cs), nil
}

func (s *Store) SaveChangeSet(_ context.Context, cs *state.ChangeSet) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.changeSets[cs.ID]; !ok {
		return state.ErrNotFound
	}
	s.changeSets[cs.ID] = clone(cs)
	return nil
}

func (s *Store) StartOperation(_ context.Context, st *state.Stack, op *state.Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.ops[op.ID]; dup {
		return state.ErrConflict
	}
	if err := s.saveStackLocked(st); err != nil {
		return err
	}
	now := s.now().UTC()
	op.CreatedAt, op.UpdatedAt = now, now
	s.ops[op.ID] = clone(op)
	return nil
}

func (s *Store) GetOperation(_ context.Context, id string) (*state.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.ops[id]
	if !ok {
		return nil, state.ErrNotFound
	}
	return clone(op), nil
}

func (s *Store) SaveProgress(_ context.Context, st *state.Stack, op *state.Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLeaseLocked(op); err != nil {
		return err
	}
	if err := s.saveStackLocked(st); err != nil {
		return err
	}
	return s.saveOpLocked(op)
}

func (s *Store) FinishDelete(_ context.Context, st *state.Stack, op *state.Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLeaseLocked(op); err != nil {
		return err
	}
	cur, ok := s.stacks[st.Name]
	if !ok || cur.Version != st.Version {
		return state.ErrConflict
	}
	delete(s.stacks, st.Name)
	return s.saveOpLocked(op)
}

func (s *Store) checkLeaseLocked(op *state.Operation) error {
	cur, ok := s.ops[op.ID]
	if !ok {
		return state.ErrNotFound
	}
	if cur.LeaseOwner != op.LeaseOwner {
		return state.ErrLeaseLost
	}
	return nil
}

func (s *Store) saveOpLocked(op *state.Operation) error {
	cur := s.ops[op.ID]
	op.UpdatedAt = s.now().UTC()
	op.LeaseExpires = cur.LeaseExpires
	s.ops[op.ID] = clone(op)
	return nil
}

func (s *Store) SaveOperation(_ context.Context, op *state.Operation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.ops[op.ID]
	if !ok {
		return state.ErrNotFound
	}
	if cur.LeaseOwner != op.LeaseOwner {
		return state.ErrLeaseLost
	}
	op.UpdatedAt = s.now().UTC()
	op.LeaseExpires = cur.LeaseExpires
	s.ops[op.ID] = clone(op)
	return nil
}

func (s *Store) ClaimOperation(_ context.Context, owner string, lease time.Duration) (*state.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	var candidates []*state.Operation
	for _, op := range s.ops {
		runnable := op.Status == state.OpPending ||
			(op.Status == state.OpRunning && now.After(op.LeaseExpires))
		if runnable {
			candidates = append(candidates, op)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].CreatedAt.Before(candidates[j].CreatedAt) })
	op := candidates[0]
	op.Status = state.OpRunning
	op.Attempts++
	// The lease token is unique per claim, so it doubles as a fencing token:
	// a stale copy of the same worker cannot write after a reclaim.
	op.LeaseOwner = fmt.Sprintf("%s/%d", owner, op.Attempts)
	op.LeaseExpires = now.Add(lease)
	op.UpdatedAt = now
	return clone(op), nil
}

func (s *Store) RenewLease(_ context.Context, id, owner string, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.ops[id]
	if !ok || op.LeaseOwner != owner || op.Status != state.OpRunning {
		return state.ErrLeaseLost
	}
	op.LeaseExpires = s.now().UTC().Add(lease)
	return nil
}

func (s *Store) AppendEvents(_ context.Context, evs ...state.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range evs {
		s.nextEvent++
		e.ID = s.nextEvent
		if e.Timestamp.IsZero() {
			e.Timestamp = s.now().UTC()
		}
		s.events = append(s.events, e)
	}
	return nil
}

func (s *Store) ListEvents(_ context.Context, stack string, afterID int64, limit int) ([]state.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	var out []state.Event
	for _, e := range s.events {
		if e.Stack == stack && e.ID > afterID {
			out = append(out, e)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

// SetClock overrides the clock (tests only).
func (s *Store) SetClock(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}
