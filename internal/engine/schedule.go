package engine

import (
	"context"
	"errors"
	"sort"
)

// stepError marks a resource-level failure (as opposed to an infrastructure
// failure such as a lost lease, which aborts the whole run).
type stepError struct{ err error }

func (e *stepError) Error() string { return e.err.Error() }
func (e *stepError) Unwrap() error { return e.err }

func stepFailed(err error) error { return &stepError{err} }

// errInterrupted means the run was cancelled (shutdown or lease loss) and
// must be resumed later; it never triggers rollback.
var errInterrupted = errors.New("operation interrupted; it will resume")

// runGraph executes fn for every node not already done, running a node only
// after all of its dependencies (restricted to nodes in the set) completed.
// Up to concurrency nodes run at once.
//
// A stepError marks the node failed. With continueOnError=false no new nodes
// start after the first failure; with true, independent nodes keep running
// while dependents of the failed node stay blocked. Any other error is fatal
// and returned once in-flight nodes drain.
func runGraph(ctx context.Context, nodes []string, deps map[string][]string, done map[string]bool,
	concurrency int, continueOnError bool, fn func(context.Context, string) error) (failed bool, fatal error) {

	if concurrency <= 0 {
		concurrency = 1
	}
	inSet := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		inSet[n] = true
	}
	completed := map[string]bool{}
	pending := map[string]bool{}
	for _, n := range nodes {
		if done[n] {
			completed[n] = true
		} else {
			pending[n] = true
		}
	}

	type result struct {
		id  string
		err error
	}
	results := make(chan result)
	running := 0
	stop := false

	ready := func() []string {
		var out []string
		for n := range pending {
			ok := true
			for _, d := range deps[n] {
				if inSet[d] && !completed[d] {
					ok = false
					break
				}
			}
			if ok {
				out = append(out, n)
			}
		}
		sort.Strings(out)
		return out
	}

	for {
		if !stop && ctx.Err() == nil {
			for _, n := range ready() {
				if running >= concurrency {
					break
				}
				delete(pending, n)
				running++
				go func(id string) { results <- result{id, fn(ctx, id)} }(n)
			}
		}
		if running == 0 {
			break
		}
		res := <-results
		running--
		var se *stepError
		switch {
		case res.err == nil:
			completed[res.id] = true
		case errors.As(res.err, &se):
			failed = true
			if !continueOnError {
				stop = true
			}
		default:
			if fatal == nil {
				fatal = res.err
			}
			stop = true
		}
	}
	if len(pending) > 0 && !stop && ctx.Err() == nil && fatal == nil {
		// Remaining nodes are blocked behind failures.
		failed = true
	}
	return failed, fatal
}
