package template

import (
	"errors"
	"strings"
	"testing"
)

const sample = `{
  "formatVersion": "2026-10-01",
  "parameters": {"Env": {"type": "string", "default": "dev", "allowed": ["dev", "prod"]}},
  "resources": {
    "Topic": {"type": "gcp:pubsub:Topic", "properties": {"labels": {"env": {"param": "Env"}}}},
    "Sub": {"type": "gcp:pubsub:Subscription", "properties": {"topic": {"ref": "Topic"}}},
    "Sa": {"type": "gcp:iam:ServiceAccount"},
    "Grant": {"type": "gcp:pubsub:TopicIamMember", "properties": {
      "topic": {"ref": "Topic"}, "role": "roles/pubsub.publisher",
      "member": {"join": ["", ["serviceAccount:", {"getAtt": ["Sa", "email"]}]]}}}
  },
  "outputs": {"TopicName": {"value": {"ref": "Topic"}}}
}`

func TestParseValidateAndGraph(t *testing.T) {
	tmpl, err := Parse([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	if err := tmpl.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	g, err := BuildGraph(tmpl)
	if err != nil {
		t.Fatal(err)
	}
	order := strings.Join(g.Order(), ",")
	if order != "Sa,Topic,Grant,Sub" {
		t.Fatalf("order = %s", order)
	}
	if got := strings.Join(g.Deps["Grant"], ","); got != "Sa,Topic" {
		t.Fatalf("Grant deps = %s", got)
	}
}

func TestValidateErrors(t *testing.T) {
	cases := map[string]string{
		"unknown ref":   `{"formatVersion":"2026-10-01","resources":{"A":{"type":"gcp:x:Y","properties":{"p":{"ref":"Nope"}}}}}`,
		"bad version":   `{"formatVersion":"1","resources":{"A":{"type":"gcp:x:Y"}}}`,
		"bad type":      `{"formatVersion":"2026-10-01","resources":{"A":{"type":"bucket"}}}`,
		"cycle":         `{"formatVersion":"2026-10-01","resources":{"A":{"type":"gcp:x:Y","properties":{"p":{"ref":"B"}}},"B":{"type":"gcp:x:Y","dependsOn":["A"]}}}`,
		"unknown param": `{"formatVersion":"2026-10-01","resources":{"A":{"type":"gcp:x:Y","properties":{"p":{"param":"Missing"}}}}}`,
		"bad lid":       `{"formatVersion":"2026-10-01","resources":{"a-b":{"type":"gcp:x:Y"}}}`,
		"self ref":      `{"formatVersion":"2026-10-01","resources":{"A":{"type":"gcp:x:Y","properties":{"p":{"ref":"A"}}}}}`,
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			tmpl, err := Parse([]byte(doc))
			if err != nil {
				t.Fatal(err)
			}
			if err := tmpl.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestParseRejectsUnknownFields(t *testing.T) {
	if _, err := Parse([]byte(`{"formatVersion":"2026-10-01","resource":{}}`)); err == nil {
		t.Fatal("expected error for unknown field")
	}
}

type mapResolver map[string]any

func (m mapResolver) Ref(id string) (any, error) {
	if v, ok := m["ref:"+id]; ok {
		return v, nil
	}
	return nil, errors.New("no ref " + id)
}
func (m mapResolver) GetAtt(id, a string) (any, error) {
	if v, ok := m["att:"+id+"."+a]; ok {
		return v, nil
	}
	return Unknown{}, nil
}
func (m mapResolver) Param(n string) (any, error) { return m["param:"+n], nil }

func TestResolve(t *testing.T) {
	r := mapResolver{"ref:Topic": "orders", "param:Env": "prod", "att:Sa.email": "sa@p.iam.gserviceaccount.com"}
	v, err := Resolve(map[string]any{
		"topic":  map[string]any{"ref": "Topic"},
		"member": map[string]any{"join": []any{"", []any{"serviceAccount:", map[string]any{"getAtt": []any{"Sa", "email"}}}}},
		"env":    map[string]any{"param": "Env"},
		"list":   []any{map[string]any{"ref": "Topic"}, "x"},
		"later":  map[string]any{"join": []any{"/", []any{"a", map[string]any{"getAtt": []any{"Run", "uri"}}}}},
	}, r)
	if err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if m["topic"] != "orders" || m["env"] != "prod" || m["member"] != "serviceAccount:sa@p.iam.gserviceaccount.com" {
		t.Fatalf("resolved = %#v", m)
	}
	if m["list"].([]any)[0] != "orders" {
		t.Fatalf("list = %#v", m["list"])
	}
	if _, ok := m["later"].(Unknown); !ok || !ContainsUnknown(m) {
		t.Fatalf("expected unknown join, got %#v", m["later"])
	}
}

func TestResolveParameters(t *testing.T) {
	tmpl, _ := Parse([]byte(sample))
	got, err := tmpl.ResolveParameters(nil, "p", "us-central1", "s")
	if err != nil || got["Env"] != "dev" || got[PseudoProject] != "p" {
		t.Fatalf("got %v %v", got, err)
	}
	if _, err := tmpl.ResolveParameters(map[string]any{"Env": "qa"}, "p", "r", "s"); err == nil {
		t.Fatal("expected allowed-values error")
	}
	if _, err := tmpl.ResolveParameters(map[string]any{"Nope": 1}, "p", "r", "s"); err == nil {
		t.Fatal("expected undeclared parameter error")
	}
}

func TestEqualNormalizes(t *testing.T) {
	if !Equal(map[string]string{"a": "b"}, map[string]any{"a": "b"}) {
		t.Fatal("typed vs untyped map should be equal")
	}
	if !Equal(3, 3.0) {
		t.Fatal("int vs float should be equal")
	}
}
