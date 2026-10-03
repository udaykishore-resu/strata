package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/udaykishore-resu/strata/internal/auth"
	"github.com/udaykishore-resu/strata/internal/engine"
	"github.com/udaykishore-resu/strata/internal/obs"
	"github.com/udaykishore-resu/strata/internal/provider"
	"github.com/udaykishore-resu/strata/internal/provider/fake"
	"github.com/udaykishore-resu/strata/internal/provider/gcp"
	"github.com/udaykishore-resu/strata/internal/store/memory"
	"github.com/udaykishore-resu/strata/internal/worker"
	"github.com/udaykishore-resu/strata/pkg/client"
)

const ordersTemplate = `{
  "formatVersion": "2026-10-01",
  "parameters": {"Env": {"type": "string", "default": "dev", "allowed": ["dev", "prod"]}},
  "resources": {
    "RunApi":  {"type": "gcp:serviceusage:Service", "properties": {"service": "run.googleapis.com"}},
    "Uploads": {"type": "gcp:storage:Bucket", "properties": {"versioning": true, "labels": {"env": {"param": "Env"}}}},
    "Events":  {"type": "gcp:pubsub:Topic"},
    "ApiSa":   {"type": "gcp:iam:ServiceAccount", "properties": {"displayName": "orders api"}},
    "Api":     {"type": "gcp:run:Service", "dependsOn": ["RunApi"], "properties": {
      "image": "us-docker.pkg.dev/cloudrun/container/hello",
      "serviceAccount": {"getAtt": ["ApiSa", "email"]},
      "env": {"BUCKET": {"ref": "Uploads"}, "TOPIC": {"ref": "Events"}}}},
    "Publish": {"type": "gcp:pubsub:TopicIamMember", "properties": {
      "topic": {"ref": "Events"}, "role": "roles/pubsub.publisher", "member": {"getAtt": ["ApiSa", "member"]}}}
  },
  "outputs": {"Url": {"value": {"getAtt": ["Api", "uri"]}}, "Bucket": {"value": {"ref": "Uploads"}}}
}`

type testEnv struct {
	srv   *httptest.Server
	cl    *client.Client
	cloud *fake.Cloud
	stop  func()
}

func newEnv(t *testing.T, authn auth.Authenticator) *testEnv {
	t.Helper()
	store := memory.New()
	cloud := fake.NewCloud()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	eng := &engine.Engine{
		Store: store, Registry: provider.NewRegistry(fake.Mirror(cloud, gcp.Schemas())...),
		Project: "proj", Region: "us-central1", Concurrency: 4, ActionTimeout: time.Minute, Log: log,
	}
	metrics := obs.NewMetrics()
	srv := httptest.NewServer((&Server{Engine: eng, Auth: authn, Log: log, Metrics: metrics, Version: "test"}).Handler())
	ctx, cancel := context.WithCancel(context.Background())
	w := &worker.Worker{Store: store, Engine: eng, ID: "w1", Lease: 10 * time.Second, PollInterval: 20 * time.Millisecond, Log: log, Metrics: metrics}
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	env := &testEnv{srv: srv, cl: client.New(srv.URL), cloud: cloud, stop: func() { cancel(); <-done; srv.Close() }}
	t.Cleanup(env.stop)
	return env
}

func (e *testEnv) deploy(t *testing.T, stack, tmpl string, params map[string]any) (*client.ChangeSet, *client.Operation) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cs, err := e.cl.CreateChangeSet(ctx, stack, json.RawMessage(tmpl), params)
	if err != nil {
		t.Fatalf("create change set: %v", err)
	}
	op, err := e.cl.ExecuteChangeSet(ctx, stack, cs.ID)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	final, err := e.cl.Wait(ctx, op, 0, nil)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	return cs, final
}

func TestEndToEndLifecycle(t *testing.T) {
	e := newEnv(t, auth.None{})
	ctx := context.Background()

	cs, op := e.deploy(t, "orders", ordersTemplate, map[string]any{"Env": "prod"})
	if len(cs.Changes) != 6 || op.Status != "SUCCEEDED" || op.Result != "READY" {
		t.Fatalf("deploy: %d changes, op %s/%s %s", len(cs.Changes), op.Status, op.Result, op.Error)
	}
	st, err := e.cl.GetStack(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	if st.Outputs["Url"] == nil || !strings.HasPrefix(st.Outputs["Url"].(string), "https://") || st.Parameters["Env"] != "prod" {
		t.Fatalf("stack outputs/params = %v %v", st.Outputs, st.Parameters)
	}
	run, _ := e.cloud.Get("gcp:run:Service", st.Resources["Api"].PhysicalID)
	envVars := run["env"].(map[string]any)
	if envVars["BUCKET"] != st.Resources["Uploads"].PhysicalID || run["serviceAccount"] != st.Resources["ApiSa"].Attributes["email"] {
		t.Fatalf("references not wired: %v", run)
	}

	events, _ := e.cl.Events(ctx, "orders", 0, 1000)
	statuses := map[string]bool{}
	for _, ev := range events {
		statuses[ev.Status] = true
	}
	for _, want := range []string{"DEPLOY_IN_PROGRESS", "CREATE_IN_PROGRESS", "CREATE_COMPLETE", "DEPLOY_COMPLETE"} {
		if !statuses[want] {
			t.Errorf("missing event %s", want)
		}
	}

	// Plan with no changes.
	cs2, err := e.cl.CreateChangeSet(ctx, "orders", json.RawMessage(ordersTemplate), map[string]any{"Env": "prod"})
	if err != nil || cs2.HasChanges() {
		t.Fatalf("expected empty plan: %+v %v", cs2, err)
	}

	rep, err := e.cl.DetectDrift(ctx, "orders")
	if err != nil || rep.Drifted {
		t.Fatalf("drift = %+v %v", rep, err)
	}

	stacks, _ := e.cl.ListStacks(ctx)
	if len(stacks) != 1 || stacks[0].Resources != 6 {
		t.Fatalf("list = %+v", stacks)
	}

	dop, err := e.cl.DeleteStack(ctx, "orders")
	if err != nil {
		t.Fatal(err)
	}
	final, err := e.cl.Wait(ctx, dop, 0, nil)
	if err != nil || final.Status != "SUCCEEDED" {
		t.Fatalf("delete = %+v %v", final, err)
	}
	if n := e.cloud.Count(); n != 0 {
		t.Fatalf("expected an empty cloud, got %d: %v", n, e.cloud.Types())
	}
	if _, err := e.cl.GetStack(ctx, "orders"); !isStatus(err, 404) {
		t.Fatalf("expected 404, got %v", err)
	}
}

func TestFailedDeployRollsBackViaAPI(t *testing.T) {
	e := newEnv(t, auth.None{})
	e.cloud.Faults.Set("create:Publish", -1)
	_, op := e.deploy(t, "orders", ordersTemplate, nil)
	if op.Status != "FAILED" || op.Result != "ROLLBACK_COMPLETE" || !strings.Contains(op.Error, "injected fault") {
		t.Fatalf("op = %+v", op)
	}
	if n := e.cloud.Count(); n != 0 {
		t.Fatalf("leftovers after rollback: %v", e.cloud.Types())
	}
}

func TestAPIErrors(t *testing.T) {
	e := newEnv(t, auth.None{})
	ctx := context.Background()
	_, err := e.cl.CreateChangeSet(ctx, "orders", json.RawMessage(`{"formatVersion":"2026-10-01","resources":{"A":{"type":"gcp:storage:Bucket","properties":{"colour":"blue"}}}}`), nil)
	if !isStatus(err, 400) || !strings.Contains(err.Error(), "unknown property") {
		t.Fatalf("expected 400 validation error, got %v", err)
	}
	_, err = e.cl.CreateChangeSet(ctx, "Orders!", json.RawMessage(ordersTemplate), nil)
	if !isStatus(err, 400) {
		t.Fatalf("expected 400 for bad stack name, got %v", err)
	}
	if _, err := e.cl.ExecuteChangeSet(ctx, "orders", "cs-missing"); !isStatus(err, 404) {
		t.Fatalf("expected 404, got %v", err)
	}

	cs, _ := e.cl.CreateChangeSet(ctx, "orders", json.RawMessage(ordersTemplate), nil)
	op, err := e.cl.ExecuteChangeSet(ctx, "orders", cs.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.cl.ExecuteChangeSet(ctx, "orders", cs.ID); !isStatus(err, 409) {
		t.Fatalf("re-executing a change set should be 409, got %v", err)
	}
	if _, err := e.cl.Wait(ctx, op, 0, nil); err != nil {
		t.Fatal(err)
	}

	resp, _ := http.Get(e.srv.URL + "/healthz")
	if resp.StatusCode != 200 {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}
	resp, _ = http.Get(e.srv.URL + "/metrics")
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `strata_http_requests_total{method="POST",route="POST /v1/stacks/{stack}/changesets",code="201"}`) ||
		!strings.Contains(string(body), "strata_operations_total") {
		t.Fatalf("metrics missing series:\n%s", body)
	}
}

type staticAuth struct{}

func (staticAuth) Authenticate(r *http.Request) (*auth.Principal, error) {
	switch r.Header.Get("Authorization") {
	case "Bearer good":
		return &auth.Principal{Email: "dev@example.com"}, nil
	case "Bearer outsider":
		return nil, auth.ErrForbidden
	}
	return nil, auth.ErrUnauthenticated
}

func TestAuthentication(t *testing.T) {
	e := newEnv(t, staticAuth{})
	ctx := context.Background()
	if _, err := e.cl.ListStacks(ctx); !isStatus(err, 401) {
		t.Fatalf("expected 401, got %v", err)
	}
	e.cl.Token = func(context.Context) (string, error) { return "outsider", nil }
	if _, err := e.cl.ListStacks(ctx); !isStatus(err, 403) {
		t.Fatalf("expected 403, got %v", err)
	}
	e.cl.Token = func(context.Context) (string, error) { return "good", nil }
	cs, err := e.cl.CreateChangeSet(ctx, "orders", json.RawMessage(ordersTemplate), nil)
	if err != nil || cs.CreatedBy != "dev@example.com" {
		t.Fatalf("change set = %+v %v", cs, err)
	}
	// Health checks never require auth.
	if resp, _ := http.Get(e.srv.URL + "/readyz"); resp.StatusCode != 200 {
		t.Fatalf("readyz = %d", resp.StatusCode)
	}
}

func isStatus(err error, code int) bool {
	var ae *client.Error
	return errors.As(err, &ae) && ae.Status == code
}
