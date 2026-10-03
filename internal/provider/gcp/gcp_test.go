package gcp

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/udaykishore-resu/strata/internal/provider"
)

// fakeAPI is a scriptable stand-in for Google APIs.
type fakeAPI struct {
	t        *testing.T
	mu       sync.Mutex
	handlers map[string]func(w http.ResponseWriter, r *http.Request, body map[string]any)
}

func newFakeAPI(t *testing.T) (*fakeAPI, *Client) {
	f := &fakeAPI{t: t, handlers: map[string]func(http.ResponseWriter, *http.Request, map[string]any){}}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	c := NewClient(StaticToken("test-token"))
	for k := range c.Endpoints {
		c.Endpoints[k] = srv.URL
	}
	c.sleep = func(ctx context.Context, d time.Duration) error { return ctx.Err() }
	return f, c
}

func (f *fakeAPI) on(methodPath string, h func(w http.ResponseWriter, r *http.Request, body map[string]any)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers[methodPath] = h
}

func (f *fakeAPI) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer test-token" {
		w.WriteHeader(401)
		return
	}
	key := r.Method + " " + r.URL.EscapedPath()
	f.mu.Lock()
	h, ok := f.handlers[key]
	f.mu.Unlock()
	var body map[string]any
	if b, _ := io.ReadAll(r.Body); len(b) > 0 {
		_ = json.Unmarshal(b, &body)
	}
	if !ok {
		writeErr(w, 404, "NOT_FOUND", "no handler for "+key)
		return
	}
	h(w, r, body)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, status, msg string) {
	w.WriteHeader(code)
	writeJSON(w, map[string]any{"error": map[string]any{"code": code, "status": status, "message": msg}})
}

var env = provider.Env{Project: "proj", Region: "us-central1", Stack: "orders"}

func TestClientRetriesAndErrorMapping(t *testing.T) {
	f, c := newFakeAPI(t)
	attempts := 0
	f.on("GET /flaky", func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		attempts++
		if attempts < 3 {
			writeErr(w, 503, "UNAVAILABLE", "try again")
			return
		}
		writeJSON(w, map[string]any{"ok": true})
	})
	f.on("POST /bad", func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		writeErr(w, 400, "INVALID_ARGUMENT", "nope")
	})
	f.on("PUT /exists", func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		writeErr(w, 409, "ALREADY_EXISTS", "Resource already exists")
	})

	var out map[string]any
	if err := c.Do(context.Background(), http.MethodGet, c.endpoint("storage")+"/flaky", nil, &out); err != nil || out["ok"] != true {
		t.Fatalf("retry: %v %v", out, err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d", attempts)
	}
	err := c.Do(context.Background(), http.MethodPost, c.endpoint("storage")+"/bad", map[string]any{}, nil)
	var ae *APIError
	if !errors.As(err, &ae) || ae.Code != 400 || ae.Message != "nope" {
		t.Fatalf("bad request err = %v", err)
	}
	if err := c.Do(context.Background(), http.MethodGet, c.endpoint("storage")+"/missing", nil, nil); !errors.Is(err, provider.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if err := c.Do(context.Background(), http.MethodPut, c.endpoint("storage")+"/exists", nil, nil); !errors.Is(err, provider.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestWaitOperation(t *testing.T) {
	f, c := newFakeAPI(t)
	polls := 0
	f.on("GET /v2/projects/proj/locations/us-central1/operations/op1", func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		polls++
		writeJSON(w, map[string]any{"name": "projects/proj/locations/us-central1/operations/op1", "done": polls >= 2})
	})
	f.on("GET /v2/projects/proj/locations/us-central1/operations/op2", func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		writeJSON(w, map[string]any{"name": "op2", "done": true, "error": map[string]any{"code": 9, "message": "revision failed to start"}})
	})
	if _, err := c.WaitOperation(context.Background(), "run", "/v2/", operation{Name: "projects/proj/locations/us-central1/operations/op1"}); err != nil {
		t.Fatal(err)
	}
	if polls != 2 {
		t.Fatalf("polls = %d", polls)
	}
	_, err := c.WaitOperation(context.Background(), "run", "/v2/", operation{Name: "projects/proj/locations/us-central1/operations/op2"})
	if err == nil || !strings.Contains(err.Error(), "revision failed to start") {
		t.Fatalf("expected op error, got %v", err)
	}
}

func TestIAMMemberReadModifyWrite(t *testing.T) {
	f, c := newFakeAPI(t)
	var mu sync.Mutex
	pol := map[string]any{"etag": "e1", "bindings": []any{
		map[string]any{"role": "roles/viewer", "members": []any{"user:a@x.com"}},
		map[string]any{"role": "roles/pubsub.publisher", "members": []any{"user:keep@x.com"}, "condition": map[string]any{"expression": "true"}},
	}}
	conflicted := false
	path := "/v1/projects/proj/topics/orders:getIamPolicy"
	f.on("GET "+path, func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		writeJSON(w, pol)
	})
	f.on("POST /v1/projects/proj/topics/orders:setIamPolicy", func(w http.ResponseWriter, r *http.Request, body map[string]any) {
		mu.Lock()
		defer mu.Unlock()
		if !conflicted {
			conflicted = true
			writeErr(w, 409, "ABORTED", "etag mismatch")
			return
		}
		pol = body["policy"].(map[string]any)
		writeJSON(w, pol)
	})

	var p provider.Provider
	for _, cand := range iamProviders(c) {
		if cand.Schema().Type == "gcp:pubsub:TopicIamMember" {
			p = cand
		}
	}
	req := provider.Request{Env: env, LogicalID: "Grant", Properties: map[string]any{
		"topic": "orders", "role": "roles/pubsub.publisher", "member": "serviceAccount:api@proj.iam.gserviceaccount.com",
	}}
	res, err := p.Create(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !conflicted {
		t.Fatal("expected a conflict retry")
	}
	if res.PhysicalID != "projects/proj/topics/orders|roles/pubsub.publisher|serviceAccount:api@proj.iam.gserviceaccount.com" {
		t.Fatalf("physical id = %s", res.PhysicalID)
	}
	mu.Lock()
	pp := policy(pol)
	if !pp.hasMember("roles/pubsub.publisher", "serviceAccount:api@proj.iam.gserviceaccount.com") ||
		!pp.hasMember("roles/viewer", "user:a@x.com") || pp["version"] != float64(3) {
		t.Fatalf("policy = %v", pol)
	}
	// The conditional binding must be untouched.
	if len(pp.bindings()) != 3 {
		t.Fatalf("bindings = %v", pp.bindings())
	}
	mu.Unlock()

	if _, err := p.Read(context.Background(), provider.Request{Env: env, PhysicalID: res.PhysicalID}); err != nil {
		t.Fatal(err)
	}
	if err := p.Delete(context.Background(), provider.Request{Env: env, PhysicalID: res.PhysicalID}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Read(context.Background(), provider.Request{Env: env, PhysicalID: res.PhysicalID}); !errors.Is(err, provider.ErrNotFound) {
		t.Fatalf("expected not found after delete, got %v", err)
	}
}

func TestPolicyHelpers(t *testing.T) {
	p := policy{}
	if !p.add("roles/a", "user:x") || p.add("roles/a", "user:x") {
		t.Fatal("add should be idempotent")
	}
	p.add("roles/a", "user:y")
	if !p.remove("roles/a", "user:x") || !p.hasMember("roles/a", "user:y") {
		t.Fatal("remove failed")
	}
	p.remove("roles/a", "user:y")
	if len(p.bindings()) != 0 {
		t.Fatalf("empty binding should be dropped: %v", p)
	}
}

func TestBucketLifecycle(t *testing.T) {
	f, c := newFakeAPI(t)
	var created, patched map[string]any
	f.on("POST /storage/v1/b", func(w http.ResponseWriter, r *http.Request, body map[string]any) {
		if r.URL.Query().Get("project") != "proj" {
			t.Errorf("project query = %s", r.URL.RawQuery)
		}
		created = body
		writeJSON(w, map[string]any{"name": body["name"], "selfLink": "https://x/b/" + body["name"].(string)})
	})
	f.on("PATCH /storage/v1/b/orders-uploads", func(w http.ResponseWriter, r *http.Request, body map[string]any) {
		patched = body
		writeJSON(w, map[string]any{"name": "orders-uploads"})
	})
	f.on("GET /storage/v1/b/orders-uploads/o", func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		if r.URL.Query().Get("pageToken") == "" {
			writeJSON(w, map[string]any{"items": []any{map[string]any{"name": "a/b.txt", "generation": "1"}}, "nextPageToken": "p2"})
			return
		}
		writeJSON(w, map[string]any{"items": []any{map[string]any{"name": "c.txt", "generation": "7"}}})
	})
	deleted := []string{}
	f.on("DELETE /storage/v1/b/orders-uploads/o/a%2Fb.txt", func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		deleted = append(deleted, "a/b.txt@"+r.URL.Query().Get("generation"))
	})
	f.on("DELETE /storage/v1/b/orders-uploads/o/c.txt", func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		deleted = append(deleted, "c.txt@"+r.URL.Query().Get("generation"))
	})
	f.on("DELETE /storage/v1/b/orders-uploads", func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		deleted = append(deleted, "bucket")
	})

	b := &bucket{c}
	props := provider.ApplyDefaults(b.Schema(), map[string]any{
		"name": "orders-uploads", "labels": map[string]any{"team": "a", "old": "x"}, "lifecycleDeleteAfterDays": float64(30),
	})
	res, err := b.Create(context.Background(), provider.Request{Env: env, Properties: props})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attributes["url"] != "gs://orders-uploads" || created["location"] != "US" {
		t.Fatalf("create: %v %v", res.Attributes, created)
	}
	iam := created["iamConfiguration"].(map[string]any)
	if iam["publicAccessPrevention"] != "enforced" {
		t.Fatalf("secure defaults missing: %v", iam)
	}

	newProps := provider.ApplyDefaults(b.Schema(), map[string]any{"name": "orders-uploads", "labels": map[string]any{"team": "b"}, "forceDestroy": true})
	if _, err := b.Update(context.Background(), provider.Request{Env: env, PhysicalID: "orders-uploads", Properties: newProps, OldProperties: props}); err != nil {
		t.Fatal(err)
	}
	labels := patched["labels"].(map[string]any)
	if labels["team"] != "b" || labels["old"] != nil {
		t.Fatalf("label patch = %v", labels)
	}
	if _, has := labels["old"]; !has {
		t.Fatal("removed label must be sent as null")
	}
	if v, has := patched["lifecycle"]; !has || v != nil {
		t.Fatalf("lifecycle should be cleared, got %v", patched["lifecycle"])
	}
	if err := b.Delete(context.Background(), provider.Request{Env: env, PhysicalID: "orders-uploads", Properties: newProps}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(deleted, ",") != "a/b.txt@1,c.txt@7,bucket" {
		t.Fatalf("deleted = %v", deleted)
	}
}

func TestRunServiceCreateWaitsForOperation(t *testing.T) {
	f, c := newFakeAPI(t)
	var body map[string]any
	f.on("POST /v2/projects/proj/locations/us-central1/services", func(w http.ResponseWriter, r *http.Request, b map[string]any) {
		if r.URL.Query().Get("serviceId") != "api" {
			t.Errorf("serviceId = %s", r.URL.RawQuery)
		}
		body = b
		writeJSON(w, map[string]any{"name": "projects/proj/locations/us-central1/operations/o1", "done": false})
	})
	f.on("GET /v2/projects/proj/locations/us-central1/operations/o1", func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		writeJSON(w, map[string]any{"name": "projects/proj/locations/us-central1/operations/o1", "done": true})
	})
	f.on("GET /v2/projects/proj/locations/us-central1/services/api", func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		writeJSON(w, map[string]any{"name": "projects/proj/locations/us-central1/services/api", "uri": "https://api-xyz.a.run.app", "latestReadyRevision": "api-00001"})
	})
	rs := &runService{c}
	props := provider.ApplyDefaults(rs.Schema(), map[string]any{
		"name": "api", "image": "gcr.io/p/api:1",
		"env":       map[string]any{"BUCKET": "orders-uploads"},
		"secretEnv": map[string]any{"DB_PASSWORD": "db-password"},
	})
	res, err := rs.Create(context.Background(), provider.Request{Env: env, Properties: props})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attributes["uri"] != "https://api-xyz.a.run.app" || res.Attributes["name"] != "api" {
		t.Fatalf("attrs = %v", res.Attributes)
	}
	container := body["template"].(map[string]any)["containers"].([]any)[0].(map[string]any)
	envs, _ := json.Marshal(container["env"])
	if !strings.Contains(string(envs), `"secretKeyRef":{"secret":"db-password","version":"latest"}`) ||
		!strings.Contains(string(envs), `{"name":"BUCKET","value":"orders-uploads"}`) {
		t.Fatalf("env = %s", envs)
	}
}

func TestServiceAccountAndTopic(t *testing.T) {
	f, c := newFakeAPI(t)
	f.on("POST /v1/projects/proj/serviceAccounts", func(w http.ResponseWriter, r *http.Request, b map[string]any) {
		id := b["accountId"].(string)
		writeJSON(w, map[string]any{"name": "projects/proj/serviceAccounts/" + id + "@proj.iam.gserviceaccount.com", "email": id + "@proj.iam.gserviceaccount.com", "uniqueId": "123"})
	})
	f.on("PUT /v1/projects/proj/topics/events", func(w http.ResponseWriter, r *http.Request, b map[string]any) {
		writeErr(w, 409, "ALREADY_EXISTS", "Resource already exists in the project")
	})
	sa := &serviceAccount{c}
	res, err := sa.Create(context.Background(), provider.Request{Env: env, Properties: map[string]any{"accountId": "orders-api-abc123"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attributes["member"] != "serviceAccount:orders-api-abc123@proj.iam.gserviceaccount.com" {
		t.Fatalf("attrs = %v", res.Attributes)
	}
	tp := &topic{c}
	if _, err := tp.Create(context.Background(), provider.Request{Env: env, Properties: map[string]any{"name": "events"}}); !errors.Is(err, provider.ErrAlreadyExists) {
		t.Fatalf("expected already exists, got %v", err)
	}
}

func TestSchemasAreConsistent(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range Schemas() {
		if seen[s.Type] {
			t.Errorf("duplicate type %s", s.Type)
		}
		seen[s.Type] = true
		if s.NameProperty != "" {
			if _, ok := s.Properties[s.NameProperty]; !ok {
				t.Errorf("%s: name property %q not declared", s.Type, s.NameProperty)
			}
		}
		if s.Labels {
			if _, ok := s.Properties["labels"]; !ok {
				t.Errorf("%s: labeled type must declare labels", s.Type)
			}
		}
		for name, spec := range s.Properties {
			if spec.Required && spec.Default != nil {
				t.Errorf("%s.%s: required properties cannot have defaults", s.Type, name)
			}
		}
	}
	if len(seen) != 13 {
		t.Errorf("expected 13 resource types, got %d", len(seen))
	}
}

func TestServiceAccountKeyTokenFlow(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	der, _ := x509.MarshalPKCS8PrivateKey(key)
	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	var gotAssertion string
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		gotAssertion = r.Form.Get("assertion")
		writeJSON(w, map[string]any{"access_token": "ya29.sa", "expires_in": 3600})
	}))
	defer tokenSrv.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "sa.json")
	cred, _ := json.Marshal(map[string]any{"type": "service_account", "client_email": "deployer@proj.iam.gserviceaccount.com",
		"private_key": pemKey, "private_key_id": "k1", "token_uri": tokenSrv.URL})
	_ = os.WriteFile(path, cred, 0o600)

	ts, err := tokenSourceFromFile(http.DefaultClient, path)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := ts.Token(context.Background())
	if err != nil || tok != "ya29.sa" {
		t.Fatalf("token = %q %v", tok, err)
	}
	if parts := strings.Split(gotAssertion, "."); len(parts) != 3 {
		t.Fatalf("assertion is not a JWT: %q", gotAssertion)
	}
	// Cached: no second request.
	gotAssertion = ""
	if tok2, _ := ts.Token(context.Background()); tok2 != tok || gotAssertion != "" {
		t.Fatal("token should be cached")
	}
}

func TestMetadataTokenFlow(t *testing.T) {
	md := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			w.WriteHeader(403)
			return
		}
		w.Header().Set("Metadata-Flavor", "Google")
		switch r.URL.Path {
		case "/computeMetadata/v1/instance/service-accounts/default/token":
			writeJSON(w, map[string]any{"access_token": "ya29.md", "expires_in": 3599})
		case "/computeMetadata/v1/project/project-id":
			fmt.Fprint(w, "my-project")
		default:
			w.WriteHeader(200)
		}
	}))
	defer md.Close()
	t.Setenv("GCE_METADATA_HOST", strings.TrimPrefix(md.URL, "http://"))
	t.Setenv("STRATA_GCP_ACCESS_TOKEN", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_CONFIG", t.TempDir())
	ts, src, err := DefaultTokenSource(context.Background(), http.DefaultClient)
	if err != nil || src != "metadata server" {
		t.Fatalf("source = %q %v", src, err)
	}
	if tok, err := ts.Token(context.Background()); err != nil || tok != "ya29.md" {
		t.Fatalf("token = %q %v", tok, err)
	}
	if p, err := MetadataProject(context.Background(), http.DefaultClient); err != nil || p != "my-project" {
		t.Fatalf("project = %q %v", p, err)
	}
}

func TestRunServiceRetriesNewServiceAccount(t *testing.T) {
	f, c := newFakeAPI(t)
	attempts := 0
	f.on("POST /v2/projects/proj/locations/us-central1/services", func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		attempts++
		switch attempts {
		case 1:
			writeErr(w, 400, "INVALID_ARGUMENT", "Service account web-sa@proj.iam.gserviceaccount.com does not exist.")
			return
		case 2: // the exact error Cloud Run returned in a live demo run
			writeErr(w, 403, "PERMISSION_DENIED", "Permission 'iam.serviceaccounts.actAs' denied on service account web-sa@proj.iam.gserviceaccount.com (or it may not exist).")
			return
		}
		writeJSON(w, map[string]any{"name": "op", "done": true})
	})
	f.on("GET /v2/projects/proj/locations/us-central1/services/web", func(w http.ResponseWriter, r *http.Request, _ map[string]any) {
		writeJSON(w, map[string]any{"name": "projects/proj/locations/us-central1/services/web", "uri": "https://web.a.run.app"})
	})
	rs := &runService{c}
	props := provider.ApplyDefaults(rs.Schema(), map[string]any{"name": "web", "image": "img", "serviceAccount": "web-sa@proj.iam.gserviceaccount.com"})
	if _, err := rs.Create(context.Background(), provider.Request{Env: env, Properties: props}); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d", attempts)
	}
	// A bad image is a real error and must not be retried.
	if isServiceAccountPropagation(&APIError{Code: 400, Message: "Image 'gcr.io/x/missing' not found."}) {
		t.Fatal("image errors must not be treated as propagation delay")
	}
	if isServiceAccountPropagation(&APIError{Code: 403, Message: "Permission 'run.services.create' denied on resource 'projects/proj'."}) {
		t.Fatal("permission errors unrelated to a service account must not be retried")
	}
}
