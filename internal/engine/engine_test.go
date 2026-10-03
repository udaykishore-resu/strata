package engine

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/udaykishore-resu/strata/internal/provider"
	"github.com/udaykishore-resu/strata/internal/provider/fake"
	"github.com/udaykishore-resu/strata/internal/state"
	"github.com/udaykishore-resu/strata/internal/store/memory"
	"github.com/udaykishore-resu/strata/internal/store/postgres"
	"github.com/udaykishore-resu/strata/pkg/template"
)

var testSchemas = []provider.Schema{
	{
		Type: "test:core:Bucket", NameProperty: "name", Labels: true, Name: provider.NameConstraints{MaxLen: 63},
		Properties: map[string]provider.PropertySpec{
			"name":       {Type: provider.TypeString, ForceNew: true},
			"location":   {Type: provider.TypeString, ForceNew: true, Default: "US"},
			"versioning": {Type: provider.TypeBool, Default: false},
			"labels":     {Type: provider.TypeMap},
		},
		Attributes: map[string]string{"name": "", "url": ""},
	},
	{
		Type: "test:core:Topic", NameProperty: "name", Labels: true, Name: provider.NameConstraints{MaxLen: 255},
		Properties: map[string]provider.PropertySpec{
			"name":   {Type: provider.TypeString, ForceNew: true},
			"labels": {Type: provider.TypeMap},
		},
		Attributes: map[string]string{"name": "", "id": ""},
	},
	{
		Type: "test:core:Sub", NameProperty: "name", Labels: true, Name: provider.NameConstraints{MaxLen: 255},
		Properties: map[string]provider.PropertySpec{
			"name":  {Type: provider.TypeString, ForceNew: true},
			"topic": {Type: provider.TypeString, Required: true, ForceNew: true},
			"ack":   {Type: provider.TypeInt, Default: 10},
		},
		Attributes: map[string]string{"name": ""},
	},
	{
		Type: "test:core:Account", NameProperty: "accountId", Name: provider.NameConstraints{MinLen: 6, MaxLen: 30},
		Properties: map[string]provider.PropertySpec{
			"accountId":   {Type: provider.TypeString, ForceNew: true},
			"displayName": {Type: provider.TypeString},
		},
		Attributes: map[string]string{"email": "", "member": ""},
	},
	{
		Type: "test:core:Binding",
		Properties: map[string]provider.PropertySpec{
			"bucket": {Type: provider.TypeString, Required: true, ForceNew: true},
			"role":   {Type: provider.TypeString, Required: true, ForceNew: true},
			"member": {Type: provider.TypeString, Required: true, ForceNew: true},
		},
		Attributes: map[string]string{},
	},
}

type harness struct {
	t     *testing.T
	store state.Store
	mem   *memory.Store // nil when running against Postgres
	cloud *fake.Cloud
	eng   *Engine
	clock time.Time
	mu    sync.Mutex
}

func newHarness(t *testing.T, store state.Store) *harness {
	h := &harness{t: t, store: store, cloud: fake.NewCloud(), clock: time.Now()}
	if m, ok := store.(*memory.Store); ok {
		h.mem = m
		m.SetClock(func() time.Time { h.mu.Lock(); defer h.mu.Unlock(); return h.clock })
	}
	h.eng = &Engine{
		Store: store, Registry: provider.NewRegistry(fake.Mirror(h.cloud, testSchemas)...),
		Project: "proj", Region: "us-central1", Concurrency: 4, ActionTimeout: time.Minute,
	}
	return h
}

// eachStore runs a test against the memory store and, when
// STRATA_TEST_DATABASE_URL is set, against Postgres too.
func eachStore(t *testing.T, fn func(t *testing.T, h *harness)) {
	t.Run("memory", func(t *testing.T) { fn(t, newHarness(t, memory.New())) })
	dsn := os.Getenv("STRATA_TEST_DATABASE_URL")
	if dsn == "" {
		return
	}
	t.Run("postgres", func(t *testing.T) {
		pg, err := postgres.Open(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { pg.Close() })
		if err := pg.Truncate(context.Background()); err != nil {
			t.Fatal(err)
		}
		fn(t, newHarness(t, pg))
	})
}

// expireLease releases an operation's lease so another worker can claim it,
// as a crashed worker's lease would eventually expire.
func (h *harness) expireLease(opID string) {
	h.t.Helper()
	op, err := h.store.GetOperation(context.Background(), opID)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.store.RenewLease(context.Background(), opID, op.LeaseOwner, 0); err != nil {
		h.t.Fatal(err)
	}
	h.mu.Lock()
	h.clock = h.clock.Add(time.Second)
	h.mu.Unlock()
	time.Sleep(15 * time.Millisecond)
}

func parse(t *testing.T, doc string) *template.Template {
	t.Helper()
	tmpl, err := template.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	return tmpl
}

func (h *harness) plan(stack, doc string) *state.ChangeSet {
	h.t.Helper()
	cs, err := h.eng.Plan(context.Background(), stack, parse(h.t, doc), nil, "test")
	if err != nil {
		h.t.Fatalf("plan: %v", err)
	}
	return cs
}

// deploy plans and executes doc, returning the finished operation.
func (h *harness) deploy(stack, doc string) *state.Operation {
	h.t.Helper()
	cs := h.plan(stack, doc)
	op, err := h.eng.StartDeploy(context.Background(), stack, cs.ID, "test")
	if err != nil {
		h.t.Fatalf("start deploy: %v", err)
	}
	return h.runToEnd(op.ID)
}

func (h *harness) runToEnd(opID string) *state.Operation {
	h.t.Helper()
	ctx := context.Background()
	claimed, err := h.store.ClaimOperation(ctx, "worker-1", time.Minute)
	if err != nil || claimed == nil || claimed.ID != opID {
		h.t.Fatalf("claim: %v %v", claimed, err)
	}
	if err := h.eng.Execute(ctx, claimed); err != nil {
		h.t.Fatalf("execute: %v", err)
	}
	op, _ := h.store.GetOperation(ctx, opID)
	return op
}

func (h *harness) stack(name string) *state.Stack {
	h.t.Helper()
	st, err := h.store.GetStack(context.Background(), name)
	if err != nil {
		h.t.Fatalf("get stack: %v", err)
	}
	return st
}

const baseTemplate = `{
  "formatVersion": "2026-10-01",
  "resources": {
    "Uploads": {"type": "test:core:Bucket", "properties": {"labels": {"team": "orders"}}},
    "Events":  {"type": "test:core:Topic"},
    "Worker":  {"type": "test:core:Sub", "properties": {"topic": {"ref": "Events"}}},
    "Sa":      {"type": "test:core:Account", "properties": {"displayName": "api"}},
    "Grant":   {"type": "test:core:Binding", "properties": {
      "bucket": {"ref": "Uploads"}, "role": "roles/storage.objectViewer", "member": {"getAtt": ["Sa", "member"]}}}
  },
  "outputs": {"Bucket": {"value": {"ref": "Uploads"}}, "Url": {"value": {"getAtt": ["Uploads", "url"]}}}
}`

func TestDeployNoOpUpdateDelete(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		op := h.deploy("orders", baseTemplate)
		if op.Status != state.OpSucceeded || op.Result != state.StackReady {
			t.Fatalf("op = %s/%s %s", op.Status, op.Result, op.Error)
		}
		st := h.stack("orders")
		if st.Status != state.StackReady || len(st.Resources) != 5 || st.CurrentOperation != "" {
			t.Fatalf("stack = %+v", st)
		}
		bucket := st.Resources["Uploads"]
		if !strings.HasPrefix(bucket.PhysicalID, "orders-uploads-") {
			t.Fatalf("generated name = %q", bucket.PhysicalID)
		}
		if _, stored := bucket.Properties["name"]; stored {
			t.Fatal("generated name must not be stored as a template property")
		}
		if st.Outputs["Bucket"] != bucket.PhysicalID || st.Outputs["Url"] != "gs://"+bucket.PhysicalID {
			t.Fatalf("outputs = %v", st.Outputs)
		}
		obj, _ := h.cloud.Get("test:core:Bucket", bucket.PhysicalID)
		labels := obj["labels"].(map[string]any)
		if labels["strata-stack"] != "orders" || labels["strata-lid"] != "uploads" || labels["team"] != "orders" {
			t.Fatalf("labels = %v", labels)
		}
		if sub, _ := h.cloud.Get("test:core:Sub", st.Resources["Worker"].PhysicalID); sub["topic"] != st.Resources["Events"].PhysicalID {
			t.Fatalf("sub topic not resolved: %v", sub)
		}
		acct := st.Resources["Sa"].PhysicalID
		if len(acct) < 6 || len(acct) > 30 {
			t.Fatalf("account id %q violates length constraints", acct)
		}

		// Redeploying the same template is a no-op.
		cs := h.plan("orders", baseTemplate)
		if cs.HasChanges() {
			t.Fatalf("expected no changes, got %+v", cs.Changes)
		}

		// An in-place change updates only that resource.
		updated := strings.Replace(baseTemplate, `{"labels": {"team": "orders"}}`, `{"labels": {"team": "orders"}, "versioning": true}`, 1)
		cs = h.plan("orders", updated)
		for _, ch := range cs.Changes {
			want := state.ActionNoOp
			if ch.LogicalID == "Uploads" {
				want = state.ActionUpdate
			}
			if ch.Action != want {
				t.Fatalf("%s: action %s, want %s", ch.LogicalID, ch.Action, want)
			}
		}
		op = h.deploy("orders", updated)
		if op.Status != state.OpSucceeded {
			t.Fatalf("update failed: %s", op.Error)
		}
		obj, _ = h.cloud.Get("test:core:Bucket", bucket.PhysicalID)
		if obj["versioning"] != true {
			t.Fatalf("versioning not applied: %v", obj)
		}

		// Delete removes everything.
		dop, err := h.eng.StartDelete(context.Background(), "orders", "test")
		if err != nil {
			t.Fatal(err)
		}
		if got := h.runToEnd(dop.ID); got.Status != state.OpSucceeded || got.Result != state.StackDeleted {
			t.Fatalf("delete = %s %s", got.Status, got.Error)
		}
		if n := h.cloud.Count(); n != 0 {
			t.Fatalf("cloud still has %d objects: %v", n, h.cloud.Types())
		}
		if _, err := h.store.GetStack(context.Background(), "orders"); !errors.Is(err, state.ErrNotFound) {
			t.Fatalf("stack should be gone, err=%v", err)
		}
	})
}

func TestReplacementCascadesAndCleansUp(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		h.deploy("orders", baseTemplate)
		old := h.stack("orders").Resources["Uploads"].PhysicalID

		moved := strings.Replace(baseTemplate, `{"labels": {"team": "orders"}}`, `{"labels": {"team": "orders"}, "location": "EU"}`, 1)
		cs := h.plan("orders", moved)
		actions := map[string]state.Action{}
		for _, ch := range cs.Changes {
			actions[ch.LogicalID] = ch.Action
		}
		if actions["Uploads"] != state.ActionReplace || actions["Grant"] != state.ActionReplace || actions["Worker"] != state.ActionNoOp {
			t.Fatalf("actions = %v", actions)
		}
		op := h.deploy("orders", moved)
		if op.Status != state.OpSucceeded {
			t.Fatalf("deploy: %s", op.Error)
		}
		st := h.stack("orders")
		if st.Resources["Uploads"].PhysicalID == old {
			t.Fatal("bucket was not replaced")
		}
		if _, ok := h.cloud.Get("test:core:Bucket", old); ok {
			t.Fatal("old bucket was not cleaned up")
		}
		if len(st.PendingCleanup) != 0 {
			t.Fatalf("pending cleanup = %v", st.PendingCleanup)
		}
		calls := strings.Join(h.cloud.Calls(), "\n")
		// Create-before-delete: the new bucket exists before the old one goes.
		if strings.Index(calls, "create:test:core:Bucket:"+st.Resources["Uploads"].PhysicalID) > strings.Index(calls, "delete:test:core:Bucket:"+old) {
			t.Fatalf("expected create before delete:\n%s", calls)
		}
	})
}

func TestCustomNameReplacementRejected(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		named := strings.Replace(baseTemplate, `{"labels": {"team": "orders"}}`, `{"name": "orders-uploads"}`, 1)
		h.deploy("orders", named)
		moved := strings.Replace(named, `{"name": "orders-uploads"}`, `{"name": "orders-uploads", "location": "EU"}`, 1)
		_, err := h.eng.Plan(context.Background(), "orders", parse(t, moved), nil, "")
		var ve *ValidationError
		if !errors.As(err, &ve) || !strings.Contains(err.Error(), "custom name") {
			t.Fatalf("expected custom-name replacement error, got %v", err)
		}
	})
}

func TestRollbackRestoresPreviousState(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		h.deploy("orders", baseTemplate)
		before := h.stack("orders")
		bucket := before.Resources["Uploads"].PhysicalID

		broken := strings.Replace(baseTemplate, `"Events":  {"type": "test:core:Topic"},`,
			`"Events":  {"type": "test:core:Topic"}, "Audit": {"type": "test:core:Topic", "dependsOn": ["Uploads"]},`, 1)
		broken = strings.Replace(broken, `{"labels": {"team": "orders"}}`, `{"labels": {"team": "orders"}, "versioning": true}`, 1)
		h.cloud.Faults.Set("create:Audit", -1)

		op := h.deploy("orders", broken)
		if op.Status != state.OpFailed || op.Result != state.StackRollbackComplete {
			t.Fatalf("op = %s/%s", op.Status, op.Result)
		}
		st := h.stack("orders")
		if st.Status != state.StackRollbackComplete || st.CurrentOperation != "" {
			t.Fatalf("stack status = %s", st.Status)
		}
		if _, ok := st.Resources["Audit"]; ok {
			t.Fatal("failed resource must not remain in state")
		}
		if len(st.Template.Resources) != len(before.Template.Resources) {
			t.Fatal("template not restored")
		}
		obj, _ := h.cloud.Get("test:core:Bucket", bucket)
		if obj["versioning"] != false {
			t.Fatalf("bucket update was not reverted: %v", obj)
		}
		if h.cloud.Count() != 5 {
			t.Fatalf("cloud objects = %d", h.cloud.Count())
		}
		// The stack is deployable again after the fault clears.
		h.cloud.Faults.Clear()
		if op := h.deploy("orders", broken); op.Status != state.OpSucceeded {
			t.Fatalf("redeploy: %s", op.Error)
		}
	})
}

func TestRollbackOfNewStackLeavesNothing(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		h.cloud.Faults.Set("create:Grant", -1)
		op := h.deploy("orders", baseTemplate)
		if op.Result != state.StackRollbackComplete {
			t.Fatalf("result = %s (%s)", op.Result, op.Error)
		}
		if n := h.cloud.Count(); n != 0 {
			t.Fatalf("cloud has %d leftovers: %v", n, h.cloud.Types())
		}
		if st := h.stack("orders"); len(st.Resources) != 0 || st.Template != nil {
			t.Fatalf("stack = %+v", st)
		}
	})
}

func TestRemovedResourcesDeletedDuringCleanup(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		h.deploy("orders", baseTemplate)
		sub := h.stack("orders").Resources["Worker"].PhysicalID
		smaller := strings.Replace(baseTemplate, `"Worker":  {"type": "test:core:Sub", "properties": {"topic": {"ref": "Events"}}},`, "", 1)
		cs := h.plan("orders", smaller)
		var deleted []string
		for _, ch := range cs.Changes {
			if ch.Action == state.ActionDelete {
				deleted = append(deleted, ch.LogicalID)
			}
		}
		if strings.Join(deleted, ",") != "Worker" {
			t.Fatalf("deletes = %v", deleted)
		}
		if op := h.deploy("orders", smaller); op.Status != state.OpSucceeded {
			t.Fatal(op.Error)
		}
		if _, ok := h.cloud.Get("test:core:Sub", sub); ok {
			t.Fatal("removed subscription still exists")
		}
	})
}

func TestRetainPolicyOnDelete(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		retained := strings.Replace(baseTemplate, `"Events":  {"type": "test:core:Topic"},`, `"Events":  {"type": "test:core:Topic", "deletionPolicy": "Retain"},`, 1)
		h.deploy("orders", retained)
		topic := h.stack("orders").Resources["Events"].PhysicalID
		dop, _ := h.eng.StartDelete(context.Background(), "orders", "")
		if op := h.runToEnd(dop.ID); op.Status != state.OpSucceeded {
			t.Fatal(op.Error)
		}
		if _, ok := h.cloud.Get("test:core:Topic", topic); !ok {
			t.Fatal("retained topic was deleted")
		}
	})
}

func TestResumeAfterInterruption(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		ctx, cancel := context.WithCancel(context.Background())
		creates := 0
		var mu sync.Mutex
		h.cloud.Hook = func(action, lid string) {
			mu.Lock()
			defer mu.Unlock()
			if action == "create" {
				creates++
				if creates == 2 {
					cancel() // worker shutting down mid-deploy
				}
			}
		}
		h.eng.Concurrency = 1
		cs := h.plan("orders", baseTemplate)
		op, _ := h.eng.StartDeploy(context.Background(), "orders", cs.ID, "")
		claimed, _ := h.store.ClaimOperation(context.Background(), "worker-1", time.Minute)
		if err := h.eng.Execute(ctx, claimed); !errors.Is(err, errInterrupted) {
			t.Fatalf("expected interruption, got %v", err)
		}
		if st := h.stack("orders"); st.CurrentOperation != op.ID {
			t.Fatal("stack lock must be held across interruption")
		}

		// Another worker takes over after the lease expires.
		h.cloud.Hook = nil
		h.expireLease(op.ID)
		reclaimed, err := h.store.ClaimOperation(context.Background(), "worker-2", time.Minute)
		if err != nil || reclaimed == nil || reclaimed.ID != op.ID || reclaimed.Attempts != 2 {
			t.Fatalf("reclaim = %+v, %v", reclaimed, err)
		}
		if err := h.eng.Execute(context.Background(), reclaimed); err != nil {
			t.Fatal(err)
		}
		final, _ := h.store.GetOperation(context.Background(), op.ID)
		if final.Status != state.OpSucceeded {
			t.Fatalf("final = %s %s", final.Status, final.Error)
		}
		if n := h.cloud.Count(); n != 5 {
			t.Fatalf("expected 5 objects (no duplicates), got %d", n)
		}
	})
}

// flakyStore fails one SaveProgress call on demand, simulating a worker that
// dies after a provider call succeeds but before progress is recorded.
type flakyStore struct {
	state.Store
	mu       sync.Mutex
	failNext bool
}

func (f *flakyStore) SaveProgress(ctx context.Context, s *state.Stack, op *state.Operation) error {
	f.mu.Lock()
	fail := f.failNext
	f.failNext = false
	f.mu.Unlock()
	if fail {
		return errors.New("simulated crash")
	}
	return f.Store.SaveProgress(ctx, s, op)
}

func TestCrashAfterCreateIsReconciled(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		fs := &flakyStore{Store: h.store}
		h.eng.Store = fs
		h.eng.Concurrency = 1
		armed := true
		h.cloud.Hook = func(action, lid string) {
			if action == "create" && lid == "Events" && armed {
				armed = false
				fs.mu.Lock()
				fs.failNext = true // the save recording the completed create will fail
				fs.mu.Unlock()
			}
		}
		cs := h.plan("orders", baseTemplate)
		op, _ := h.eng.StartDeploy(context.Background(), "orders", cs.ID, "")
		claimed, _ := h.store.ClaimOperation(context.Background(), "worker-1", time.Minute)
		if err := h.eng.Execute(context.Background(), claimed); err == nil {
			t.Fatal("expected execution to fail on simulated crash")
		}
		h.expireLease(op.ID)
		reclaimed, _ := h.store.ClaimOperation(context.Background(), "worker-2", time.Minute)
		if err := h.eng.Execute(context.Background(), reclaimed); err != nil {
			t.Fatal(err)
		}
		final, _ := h.store.GetOperation(context.Background(), op.ID)
		if final.Status != state.OpSucceeded {
			t.Fatalf("final = %s %s", final.Status, final.Error)
		}
		topicCreates := 0
		for _, c := range h.cloud.Calls() {
			if strings.HasPrefix(c, "create:test:core:Topic:") {
				topicCreates++
			}
		}
		if topicCreates != 1 || h.cloud.Count() != 5 {
			t.Fatalf("topic creates = %d, objects = %d", topicCreates, h.cloud.Count())
		}
	})
}

func TestDriftDetection(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		h.deploy("orders", baseTemplate)
		st := h.stack("orders")
		rep, err := h.eng.DetectDrift(context.Background(), "orders")
		if err != nil || rep.Drifted {
			t.Fatalf("fresh stack drifted: %+v %v", rep, err)
		}
		h.cloud.Mutate("test:core:Bucket", st.Resources["Uploads"].PhysicalID, "versioning", true)
		h.cloud.Remove("test:core:Topic", st.Resources["Events"].PhysicalID)
		rep, _ = h.eng.DetectDrift(context.Background(), "orders")
		got := map[string]string{}
		for _, r := range rep.Resources {
			got[r.LogicalID] = r.Status
		}
		if !rep.Drifted || got["Uploads"] != DriftModified || got["Events"] != DriftDeleted || got["Worker"] != DriftInSync {
			t.Fatalf("drift = %v", got)
		}
	})
}

func TestConcurrencyGuards(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		h.deploy("orders", baseTemplate)
		cs1 := h.plan("orders", baseTemplate)
		cs2 := h.plan("orders", baseTemplate)
		if _, err := h.eng.StartDeploy(context.Background(), "orders", cs1.ID, ""); err != nil {
			t.Fatal(err)
		}
		if _, err := h.eng.StartDeploy(context.Background(), "orders", cs2.ID, ""); !errors.Is(err, ErrBusy) {
			t.Fatalf("expected busy, got %v", err)
		}
		if _, err := h.eng.StartDelete(context.Background(), "orders", ""); !errors.Is(err, ErrBusy) {
			t.Fatalf("expected busy, got %v", err)
		}
	})
}

func TestStaleChangeSetRejected(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		h.deploy("orders", baseTemplate)
		stale := h.plan("orders", baseTemplate)
		h.deploy("orders", strings.Replace(baseTemplate, `"displayName": "api"`, `"displayName": "api2"`, 1))
		if _, err := h.eng.StartDeploy(context.Background(), "orders", stale.ID, ""); !errors.Is(err, ErrStale) {
			t.Fatalf("expected stale, got %v", err)
		}
	})
}

func TestPlanValidation(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		bad := `{"formatVersion":"2026-10-01","resources":{
	  "A":{"type":"test:core:Bucket","properties":{"nope":1}},
	  "B":{"type":"test:core:Sub"},
	  "C":{"type":"test:core:Topic","properties":{"labels":{"x":{"getAtt":["A","missing"]}}}},
	  "D":{"type":"test:core:Unknown"}}}`
		_, err := h.eng.Plan(context.Background(), "orders", parse(t, bad), nil, "")
		for _, want := range []string{`unknown property "nope"`, `missing required property "topic"`, `no attribute "missing"`, `unsupported resource type`} {
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("expected %q in %v", want, err)
			}
		}
		if _, err := h.eng.Plan(context.Background(), "Bad_Name", parse(t, baseTemplate), nil, ""); err == nil {
			t.Error("expected invalid stack name error")
		}
	})
}

func TestDenyPublicMembers(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		h.eng.DenyPublicMembers = true
		pub := strings.Replace(baseTemplate, `{"getAtt": ["Sa", "member"]}`, `"allUsers"`, 1)
		if _, err := h.eng.Plan(context.Background(), "orders", parse(t, pub), nil, ""); err == nil || !strings.Contains(err.Error(), "public IAM member") {
			t.Fatalf("expected policy error, got %v", err)
		}
	})
}

// --- Regression tests for the independent review findings ---

// A custom name that collides with a resource owned by another stack must
// fail the deploy without the rollback deleting the foreign resource.
func TestCollisionNeverDeletesForeignResource(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		h.cloud.Put("test:core:Bucket", "shared-data", map[string]any{
			"name": "shared-data", "labels": map[string]any{"strata-stack": "other", "strata-lid": "data"},
		})
		h.cloud.Put("test:core:Account", "shared-sa", map[string]any{"accountId": "shared-sa"})
		for _, doc := range []string{
			strings.Replace(baseTemplate, `{"labels": {"team": "orders"}}`, `{"name": "shared-data"}`, 1),
			strings.Replace(baseTemplate, `{"displayName": "api"}`, `{"accountId": "shared-sa"}`, 1),
		} {
			op := h.deploy("orders", doc)
			if op.Result != state.StackRollbackComplete || !strings.Contains(op.Error, "already exists and is not managed") {
				t.Fatalf("op = %s %s", op.Result, op.Error)
			}
		}
		b, ok := h.cloud.Get("test:core:Bucket", "shared-data")
		if !ok || b["labels"].(map[string]any)["strata-stack"] != "other" {
			t.Fatalf("foreign bucket was deleted or modified: %v %v", ok, b)
		}
		if _, ok := h.cloud.Get("test:core:Account", "shared-sa"); !ok {
			t.Fatal("foreign service account was deleted")
		}
	})
}

// A resource that appears between the pre-check and an interrupted create
// must not be adopted on resume if it belongs to someone else.
func TestResumeDoesNotAdoptForeignResource(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		named := strings.Replace(baseTemplate, `{"labels": {"team": "orders"}}`, `{"name": "shared-data"}`, 1)
		ctx, cancel := context.WithCancel(context.Background())
		h.cloud.Hook = func(action, lid string) {
			if action == "create" && lid == "Uploads" {
				h.cloud.Put("test:core:Bucket", "shared-data", map[string]any{
					"name": "shared-data", "labels": map[string]any{"strata-stack": "other", "strata-lid": "data"},
				})
				cancel()
			}
		}
		cs := h.plan("orders", named)
		op, _ := h.eng.StartDeploy(context.Background(), "orders", cs.ID, "")
		claimed, _ := h.store.ClaimOperation(context.Background(), "worker-1", time.Minute)
		if err := h.eng.Execute(ctx, claimed); !errors.Is(err, errInterrupted) {
			t.Fatalf("expected interruption, got %v", err)
		}
		h.cloud.Hook = nil
		h.expireLease(op.ID)
		reclaimed, _ := h.store.ClaimOperation(context.Background(), "worker-2", time.Minute)
		if err := h.eng.Execute(context.Background(), reclaimed); err != nil {
			t.Fatal(err)
		}
		final, _ := h.store.GetOperation(context.Background(), op.ID)
		if final.Result != state.StackRollbackComplete {
			t.Fatalf("expected rollback, got %s %s", final.Result, final.Error)
		}
		b, ok := h.cloud.Get("test:core:Bucket", "shared-data")
		if !ok || b["labels"].(map[string]any)["strata-stack"] != "other" {
			t.Fatalf("foreign bucket adopted or deleted: %v %v", ok, b)
		}
	})
}

// A transient store error while resuming must not end the operation.
type changeSetFlake struct {
	state.Store
	fail bool
}

func (c *changeSetFlake) GetChangeSet(ctx context.Context, id string) (*state.ChangeSet, error) {
	if c.fail {
		c.fail = false
		return nil, errors.New("connection reset")
	}
	return c.Store.GetChangeSet(ctx, id)
}

func TestTransientErrorOnResumeKeepsOperation(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		cs := h.plan("orders", baseTemplate)
		op, _ := h.eng.StartDeploy(context.Background(), "orders", cs.ID, "")
		flaky := &changeSetFlake{Store: h.store, fail: true}
		h.eng.Store = flaky
		claimed, _ := h.store.ClaimOperation(context.Background(), "worker-1", time.Minute)
		if err := h.eng.Execute(context.Background(), claimed); err == nil {
			t.Fatal("expected a retryable error")
		}
		mid, _ := h.store.GetOperation(context.Background(), op.ID)
		if mid.Status.Terminal() || h.stack("orders").CurrentOperation != op.ID {
			t.Fatalf("operation must stay open: %s", mid.Status)
		}
		h.expireLease(op.ID)
		reclaimed, _ := h.store.ClaimOperation(context.Background(), "worker-2", time.Minute)
		if err := h.eng.Execute(context.Background(), reclaimed); err != nil {
			t.Fatal(err)
		}
		if final, _ := h.store.GetOperation(context.Background(), op.ID); final.Status != state.OpSucceeded {
			t.Fatalf("final = %s %s", final.Status, final.Error)
		}
	})
}

// A step failure followed by a crash before the rollback transition is saved
// must still roll back on resume (not retry and leave partial resources).
type failRollbackTransition struct {
	state.Store
	armed bool
}

func (f *failRollbackTransition) SaveProgress(ctx context.Context, s *state.Stack, op *state.Operation) error {
	if f.armed && op.Checkpoint.Phase == state.PhaseRollback {
		f.armed = false
		return errors.New("simulated crash")
	}
	return f.Store.SaveProgress(ctx, s, op)
}

func TestFailureThenCrashStillRollsBack(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		h.deploy("orders", baseTemplate)
		h.eng.Store = &failRollbackTransition{Store: h.store, armed: true}
		h.cloud.Faults.Set("create:Audit", 1) // fails once; a retry would succeed
		doc := strings.Replace(baseTemplate, `"Events":  {"type": "test:core:Topic"},`,
			`"Events":  {"type": "test:core:Topic"}, "Audit": {"type": "test:core:Topic"},`, 1)
		cs := h.plan("orders", doc)
		op, _ := h.eng.StartDeploy(context.Background(), "orders", cs.ID, "")
		claimed, _ := h.store.ClaimOperation(context.Background(), "worker-1", time.Minute)
		if err := h.eng.Execute(context.Background(), claimed); err == nil {
			t.Fatal("expected simulated crash")
		}
		h.expireLease(op.ID)
		reclaimed, _ := h.store.ClaimOperation(context.Background(), "worker-2", time.Minute)
		if err := h.eng.Execute(context.Background(), reclaimed); err != nil {
			t.Fatal(err)
		}
		final, _ := h.store.GetOperation(context.Background(), op.ID)
		if final.Result != state.StackRollbackComplete {
			t.Fatalf("expected rollback after resume, got %s", final.Result)
		}
		if h.cloud.Count() != 5 {
			t.Fatalf("leftover resources: %d", h.cloud.Count())
		}
	})
}

// deletionPolicy changes are planned, applied without a cloud call, and
// reverted by rollback.
func TestDeletionPolicyChanges(t *testing.T) {
	eachStore(t, func(t *testing.T, h *harness) {
		retain := strings.Replace(baseTemplate, `"Events":  {"type": "test:core:Topic"},`, `"Events":  {"type": "test:core:Topic", "deletionPolicy": "Retain"},`, 1)
		h.deploy("orders", retain)

		cs := h.plan("orders", baseTemplate) // Retain -> Delete only
		var ch state.Change
		for _, c := range cs.Changes {
			if c.LogicalID == "Events" {
				ch = c
			}
		}
		if ch.Action != state.ActionUpdate || len(ch.Diffs) != 1 || ch.Diffs[0].Name != "(deletionPolicy)" {
			t.Fatalf("change = %+v", ch)
		}

		// Combined with a failure, rollback must restore Retain.
		h.cloud.Faults.Set("create:Audit", -1)
		broken := strings.Replace(baseTemplate, `"Events":  {"type": "test:core:Topic"},`,
			`"Events":  {"type": "test:core:Topic"}, "Audit": {"type": "test:core:Topic", "dependsOn": ["Events"]},`, 1)
		if op := h.deploy("orders", broken); op.Result != state.StackRollbackComplete {
			t.Fatalf("op = %s", op.Result)
		}
		if got := h.stack("orders").Resources["Events"].DeletionPolicy; got != template.DeletionPolicyRetain {
			t.Fatalf("deletionPolicy after rollback = %q", got)
		}

		h.cloud.Faults.Clear()
		before := len(h.cloud.Calls())
		if op := h.deploy("orders", baseTemplate); op.Status != state.OpSucceeded {
			t.Fatal(op.Error)
		}
		if after := h.cloud.Calls(); len(after) != before {
			t.Fatalf("metadata-only change made cloud calls: %v", after[before:])
		}
		if got := h.stack("orders").Resources["Events"].DeletionPolicy; got != template.DeletionPolicyDelete {
			t.Fatalf("deletionPolicy = %q", got)
		}
	})
}
