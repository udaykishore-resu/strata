package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/udaykishore-resu/strata/internal/engine"
	"github.com/udaykishore-resu/strata/internal/provider"
	"github.com/udaykishore-resu/strata/internal/provider/fake"
	"github.com/udaykishore-resu/strata/internal/provider/gcp"
	"github.com/udaykishore-resu/strata/internal/state"
	"github.com/udaykishore-resu/strata/internal/store/memory"
)

func deploy(t *testing.T, eng *engine.Engine, store *memory.Store, params map[string]any) *state.Operation {
	t.Helper()
	return deployWith(t, eng, store, params, engine.PlanOptions{Refresh: true})
}

func deployWith(t *testing.T, eng *engine.Engine, store *memory.Store, params map[string]any, opts engine.PlanOptions) *state.Operation {
	t.Helper()
	tmpl, err := Build().Stacks()[0].Template()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cs, err := eng.PlanWith(ctx, "hello", tmpl, params, "test", opts)
	if err != nil {
		t.Fatal(err)
	}
	op, err := eng.StartDeploy(ctx, "hello", cs.ID, "test")
	if err != nil {
		t.Fatal(err)
	}
	claimed, _ := store.ClaimOperation(ctx, "w", time.Minute)
	if err := eng.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	final, _ := store.GetOperation(ctx, op.ID)
	return final
}

// TestDemoScenario runs the same three moments as scripts/demo-gcp.sh
// against the fake cloud: deploy, failed update with rollback, redeploy.
func TestDemoScenario(t *testing.T) {
	store := memory.New()
	cloud := fake.NewCloud()
	eng := &engine.Engine{
		Store: store, Registry: provider.NewRegistry(fake.Mirror(cloud, gcp.Schemas())...),
		Project: "demo", Region: "us-central1", Concurrency: 4, ActionTimeout: time.Minute,
	}

	if op := deploy(t, eng, store, nil); op.Status != state.OpSucceeded {
		t.Fatalf("deploy: %s", op.Error)
	}
	st, _ := store.GetStack(context.Background(), "hello")
	if len(st.Resources) != 8 || st.Outputs["Url"] == nil {
		t.Fatalf("resources=%d outputs=%v", len(st.Resources), st.Outputs)
	}
	web := st.Resources["Web"].PhysicalID

	cloud.Faults.Set("update:Web", 1) // stands in for Cloud Run rejecting a bad image
	op := deploy(t, eng, store, map[string]any{"Image": "us-docker.pkg.dev/cloudrun/container/does-not-exist"})
	if op.Result != state.StackRollbackComplete {
		t.Fatalf("expected rollback, got %s", op.Result)
	}
	obj, _ := cloud.Get("gcp:run:Service", web)
	if obj["image"] != "us-docker.pkg.dev/cloudrun/container/hello" {
		t.Fatalf("image not restored: %v", obj["image"])
	}
	if op := deploy(t, eng, store, nil); op.Status != state.OpSucceeded {
		t.Fatalf("redeploy: %s", op.Error)
	}

	// Step 4 of the demo, as it ran on real GCP: versioning is turned off by
	// hand, drift reports it, and a redeploy (with no template change) must
	// restore it. Before the plan-time refresh the redeploy was a no-op.
	ctx := context.Background()
	bucket := st.Resources["Assets"].PhysicalID
	cloud.Mutate("gcp:storage:Bucket", bucket, "versioning", false)
	cloud.Mutate("gcp:iam:ServiceAccount", st.Resources["WebServiceAccount49B3496C"].PhysicalID, "description", "")

	rep, err := eng.DetectDrift(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	var drifted []string
	for _, r := range rep.Resources {
		if r.Status != engine.DriftInSync {
			drifted = append(drifted, r.LogicalID)
		}
	}
	// The empty description is how GCP reports "never set": not drift.
	if len(drifted) != 1 || drifted[0] != "Assets" {
		t.Fatalf("drifted = %v, want [Assets]", drifted)
	}

	tmpl, _ := Build().Stacks()[0].Template()
	cs, err := eng.PlanWith(ctx, "hello", tmpl, nil, "test", engine.PlanOptions{Refresh: true})
	if err != nil {
		t.Fatal(err)
	}
	var assets *state.Change
	for i := range cs.Changes {
		if cs.Changes[i].Action != state.ActionNoOp {
			if cs.Changes[i].LogicalID != "Assets" {
				t.Fatalf("unexpected change %s %s", cs.Changes[i].Action, cs.Changes[i].LogicalID)
			}
			assets = &cs.Changes[i]
		}
	}
	if assets == nil || assets.Action != state.ActionUpdate || len(assets.Diffs) != 1 ||
		!assets.Diffs[0].Drift || assets.Diffs[0].Old != false || assets.Diffs[0].New != true {
		t.Fatalf("plan did not restore versioning: %+v", assets)
	}

	if op := deploy(t, eng, store, nil); op.Status != state.OpSucceeded {
		t.Fatalf("restore deploy: %s", op.Error)
	}
	if obj, _ := cloud.Get("gcp:storage:Bucket", bucket); obj["versioning"] != true {
		t.Fatalf("versioning not restored: %v", obj["versioning"])
	}
	if rep, _ := eng.DetectDrift(ctx, "hello"); rep.Drifted {
		t.Fatalf("still drifted after redeploy: %+v", rep.Resources)
	}
}

// Without refresh the plan compares only against recorded state, which is
// what produced the no-op redeploy in the first live demo.
func TestNoRefreshIgnoresDrift(t *testing.T) {
	store := memory.New()
	cloud := fake.NewCloud()
	eng := &engine.Engine{
		Store: store, Registry: provider.NewRegistry(fake.Mirror(cloud, gcp.Schemas())...),
		Project: "demo", Region: "us-central1", Concurrency: 4, ActionTimeout: time.Minute,
	}
	deploy(t, eng, store, nil)
	st, _ := store.GetStack(context.Background(), "hello")
	cloud.Mutate("gcp:storage:Bucket", st.Resources["Assets"].PhysicalID, "versioning", false)
	tmpl, _ := Build().Stacks()[0].Template()
	cs, err := eng.PlanWith(context.Background(), "hello", tmpl, nil, "test", engine.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range cs.Changes {
		if ch.Action != state.ActionNoOp {
			t.Fatalf("expected no changes without refresh, got %s %s", ch.Action, ch.LogicalID)
		}
	}
}

func TestCommittedTemplateIsCurrent(t *testing.T) {
	want, err := Build().Stacks()[0].Synth()
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("hello.template.json")
	if err != nil {
		t.Skip("hello.template.json not generated yet")
	}
	if string(got) != string(want) {
		t.Fatal("hello.template.json is stale: go run ./examples/hello-gcp -out examples/hello-gcp")
	}
}

// recorder wraps a provider and keeps the last Update request, so a test can
// check what a real (mask-based) provider would have been asked to patch.
type recorder struct {
	provider.Provider
	last provider.Request
}

func (r *recorder) Update(ctx context.Context, req provider.Request) (provider.Result, error) {
	r.last = req
	return r.Provider.Update(ctx, req)
}

// A drift-restoring update must present the live value as the old one;
// providers that patch only changed fields (Pub/Sub, labels) would otherwise
// compute an empty update mask and silently leave the drift in place.
func TestDriftRestoreSendsLiveOldProperties(t *testing.T) {
	store := memory.New()
	cloud := fake.NewCloud()
	var rec *recorder
	var ps []provider.Provider
	for _, p := range fake.Mirror(cloud, gcp.Schemas()) {
		if p.Schema().Type == "gcp:storage:Bucket" {
			rec = &recorder{Provider: p}
			p = rec
		}
		ps = append(ps, p)
	}
	eng := &engine.Engine{
		Store: store, Registry: provider.NewRegistry(ps...),
		Project: "demo", Region: "us-central1", Concurrency: 4, ActionTimeout: time.Minute,
	}
	deploy(t, eng, store, nil)
	st, _ := store.GetStack(context.Background(), "hello")
	cloud.Mutate("gcp:storage:Bucket", st.Resources["Assets"].PhysicalID, "versioning", false)

	if op := deploy(t, eng, store, nil); op.Status != state.OpSucceeded {
		t.Fatalf("restore deploy: %s", op.Error)
	}
	if rec.last.OldProperties["versioning"] != false || rec.last.Properties["versioning"] != true {
		t.Fatalf("update old=%v new=%v, want old=false new=true",
			rec.last.OldProperties["versioning"], rec.last.Properties["versioning"])
	}
}
