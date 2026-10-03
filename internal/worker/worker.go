// Package worker runs queued stack operations. Any number of worker
// processes can run against the same store: operations are claimed with a
// lease, the lease is renewed by a heartbeat while the operation runs, and
// an operation whose worker dies is resumed by another once its lease expires.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/udaykishore-resu/strata/internal/engine"
	"github.com/udaykishore-resu/strata/internal/obs"
	"github.com/udaykishore-resu/strata/internal/state"
)

// Worker claims and executes operations.
type Worker struct {
	Store        state.Store
	Engine       *engine.Engine
	ID           string
	Concurrency  int
	Lease        time.Duration
	PollInterval time.Duration
	Log          *slog.Logger
	Metrics      *obs.Metrics

	running atomic.Int64
	wg      sync.WaitGroup
}

// Running returns the number of operations in progress.
func (w *Worker) Running() int64 { return w.running.Load() }

func (w *Worker) defaults() {
	if w.Concurrency <= 0 {
		w.Concurrency = 4
	}
	if w.Lease <= 0 {
		w.Lease = 60 * time.Second
	}
	if w.PollInterval <= 0 {
		w.PollInterval = time.Second
	}
	if w.Log == nil {
		w.Log = slog.Default()
	}
}

// Run polls for work until ctx is cancelled, then waits for in-flight
// operations to checkpoint and release their leases.
func (w *Worker) Run(ctx context.Context) {
	w.defaults()
	w.Log.Info("worker started", "worker", w.ID, "concurrency", w.Concurrency, "lease", w.Lease.String())
	sem := make(chan struct{}, w.Concurrency)
	idle := w.PollInterval
	for {
		select {
		case <-ctx.Done():
			w.wg.Wait()
			w.Log.Info("worker stopped", "worker", w.ID)
			return
		case sem <- struct{}{}:
		}
		op, err := w.Store.ClaimOperation(ctx, w.ID, w.Lease)
		if err != nil || op == nil {
			<-sem
			if err != nil && ctx.Err() == nil {
				w.Log.Error("claim operation failed", "err", err)
			}
			if !sleep(ctx, idle) {
				continue
			}
			if idle < 5*w.PollInterval {
				idle += w.PollInterval / 2
			}
			continue
		}
		idle = w.PollInterval
		w.wg.Add(1)
		w.running.Add(1)
		go func() {
			defer func() { <-sem; w.running.Add(-1); w.wg.Done() }()
			w.execute(ctx, op)
		}()
	}
}

func (w *Worker) execute(parent context.Context, op *state.Operation) {
	log := w.Log.With("operation", op.ID, "stack", op.Stack, "kind", op.Kind, "attempt", op.Attempts)
	log.Info("operation claimed")
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	// Heartbeat: renew the lease with this claim's token. Losing it, or
	// failing to renew for most of a lease period, cancels the run before
	// another worker can take over.
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(w.Lease / 3)
		defer t.Stop()
		lastOK := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), w.Lease/3)
				err := w.Store.RenewLease(rctx, op.ID, op.LeaseOwner, w.Lease)
				rcancel()
				switch {
				case err == nil:
					lastOK = time.Now()
				case errors.Is(err, state.ErrLeaseLost):
					log.Warn("lease lost; abandoning operation")
					cancel()
					return
				default:
					log.Warn("lease renewal failed", "err", err)
					if time.Since(lastOK) > w.Lease*3/4 {
						log.Warn("lease may have expired; abandoning operation")
						cancel()
						return
					}
				}
			}
		}
	}()

	start := time.Now()
	err := w.Engine.Execute(ctx, op)
	cancel()
	<-hbDone

	final, gerr := w.Store.GetOperation(context.WithoutCancel(parent), op.ID)
	switch {
	case gerr == nil && final.Status.Terminal():
		log.Info("operation finished", "status", final.Status, "result", final.Result, "durationMs", time.Since(start).Milliseconds())
		if w.Metrics != nil {
			w.Metrics.Operations.Inc(string(op.Kind), string(final.Result))
			w.Metrics.OperationDuration.Observe(time.Since(start).Seconds(), string(op.Kind))
		}
	default:
		if err == nil {
			err = gerr
		}
		log.Warn("operation paused; it will resume", "err", err)
		// Hand the lease back immediately so another worker can resume now.
		rctx, rcancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
		_ = w.Store.RenewLease(rctx, op.ID, op.LeaseOwner, 0)
		rcancel()
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
