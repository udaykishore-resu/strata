package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/udaykishore-resu/strata/internal/engine"
	"github.com/udaykishore-resu/strata/internal/provider"
	"github.com/udaykishore-resu/strata/internal/provider/fake"
	"github.com/udaykishore-resu/strata/internal/provider/gcp"
	"github.com/udaykishore-resu/strata/internal/state"
	"github.com/udaykishore-resu/strata/internal/store/memory"
)

// TestExampleDeploys synthesizes the app and deploys it through the real
// engine with fake providers that use the real GCP schemas, proving the
// construct library emits properties every provider accepts.
func TestExampleDeploys(t *testing.T) {
	app := Build()
	tmpl, err := app.Stacks()[0].Template()
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	cloud := fake.NewCloud()
	eng := &engine.Engine{
		Store: store, Registry: provider.NewRegistry(fake.Mirror(cloud, gcp.Schemas())...),
		Project: "demo", Region: "us-central1", Concurrency: 8, ActionTimeout: time.Minute,
	}
	ctx := context.Background()
	cs, err := eng.Plan(ctx, "orders", tmpl, map[string]any{"Env": "prod"}, "test")
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	op, err := eng.StartDeploy(ctx, "orders", cs.ID, "test")
	if err != nil {
		t.Fatal(err)
	}
	claimed, _ := store.ClaimOperation(ctx, "w", time.Minute)
	if err := eng.Execute(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	final, _ := store.GetOperation(ctx, op.ID)
	if final.Status != state.OpSucceeded {
		t.Fatalf("deploy failed: %s", final.Error)
	}
	st, _ := store.GetStack(ctx, "orders")
	for _, out := range []string{"ApiUrl", "UploadBucket", "EventsTopic", "WorkerServiceAccount"} {
		if st.Outputs[out] == nil {
			t.Errorf("missing output %s", out)
		}
	}
	if len(st.Resources) != len(tmpl.Resources) {
		t.Fatalf("deployed %d of %d resources", len(st.Resources), len(tmpl.Resources))
	}
}

// TestCommittedTemplateIsCurrent keeps examples/orders-platform/orders.template.json in sync.
func TestCommittedTemplateIsCurrent(t *testing.T) {
	want, err := Build().Stacks()[0].Synth()
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join("orders.template.json"))
	if err != nil {
		t.Skip("orders.template.json not generated yet")
	}
	if string(got) != string(want) {
		t.Fatal("orders.template.json is stale: run `make synth`")
	}
}
