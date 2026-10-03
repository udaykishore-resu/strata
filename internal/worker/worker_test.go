package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/udaykishore-resu/strata/internal/engine"
	"github.com/udaykishore-resu/strata/internal/provider"
	"github.com/udaykishore-resu/strata/internal/provider/fake"
	"github.com/udaykishore-resu/strata/internal/provider/gcp"
	"github.com/udaykishore-resu/strata/internal/state"
	"github.com/udaykishore-resu/strata/internal/store/memory"
	"github.com/udaykishore-resu/strata/pkg/template"
)

const doc = `{"formatVersion":"2026-10-01","resources":{
  "A":{"type":"gcp:pubsub:Topic"},"B":{"type":"gcp:pubsub:Topic","dependsOn":["A"]},
  "C":{"type":"gcp:pubsub:Topic","dependsOn":["B"]},"D":{"type":"gcp:pubsub:Topic","dependsOn":["C"]}}}`

// A worker that shuts down mid-operation hands its lease back so another
// worker finishes the operation without waiting for the lease to expire.
func TestGracefulHandoff(t *testing.T) {
	store := memory.New()
	cloud := fake.NewCloud()
	cloud.Latency = 150 * time.Millisecond
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng := &engine.Engine{Store: store, Registry: provider.NewRegistry(fake.Mirror(cloud, gcp.Schemas())...),
		Project: "p", Region: "r", Concurrency: 1, ActionTimeout: time.Minute, Log: log}

	var tmpl template.Template
	_ = json.Unmarshal([]byte(doc), &tmpl)
	cs, err := eng.Plan(context.Background(), "s", &tmpl, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	op, err := eng.StartDeploy(context.Background(), "s", cs.ID, "")
	if err != nil {
		t.Fatal(err)
	}

	lease := time.Minute // long: a handoff that waited for expiry would time out the test
	ctxA, stopA := context.WithCancel(context.Background())
	a := &Worker{Store: store, Engine: eng, ID: "a", Lease: lease, PollInterval: 10 * time.Millisecond, Log: log}
	doneA := make(chan struct{})
	go func() { a.Run(ctxA); close(doneA) }()

	deadline := time.After(5 * time.Second)
	for len(cloud.Calls()) < 2 {
		select {
		case <-deadline:
			t.Fatal("worker a never started")
		case <-time.After(10 * time.Millisecond):
		}
	}
	stopA()
	<-doneA

	mid, _ := store.GetOperation(context.Background(), op.ID)
	if mid.Status.Terminal() {
		t.Fatal("operation finished before the handoff")
	}

	ctxB, stopB := context.WithCancel(context.Background())
	defer stopB()
	b := &Worker{Store: store, Engine: eng, ID: "b", Lease: lease, PollInterval: 10 * time.Millisecond, Log: log}
	go b.Run(ctxB)
	for {
		final, _ := store.GetOperation(context.Background(), op.ID)
		if final.Status.Terminal() {
			if final.Status != state.OpSucceeded || final.Attempts != 2 {
				t.Fatalf("final = %s attempts=%d %s", final.Status, final.Attempts, final.Error)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("worker b did not take over promptly")
		case <-time.After(20 * time.Millisecond):
		}
	}
	if n := cloud.Count(); n != 4 {
		t.Fatalf("expected 4 topics, got %d", n)
	}
}
