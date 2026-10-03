package template

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// Intrinsic function names. An intrinsic is a JSON object with exactly one
// key, e.g. {"ref": "Uploads"} or {"getAtt": ["Api", "uri"]}.
const (
	KindRef    = "ref"    // physical ID of a resource
	KindGetAtt = "getAtt" // [logicalID, attribute]
	KindParam  = "param"  // parameter or pseudo parameter value
	KindJoin   = "join"   // [separator, [parts...]]
)

// Unknown marks a value that cannot be known until deploy time (for example
// the ID of a resource that has not been created yet). It only appears in
// plans, never in applied state.
type Unknown struct{}

func (Unknown) String() string { return "(known after apply)" }

// MarshalJSON renders Unknown as a readable placeholder in plan output.
func (Unknown) MarshalJSON() ([]byte, error) { return []byte(`"(known after apply)"`), nil }

// AsIntrinsic reports whether v is an intrinsic and returns its kind and argument.
func AsIntrinsic(v any) (kind string, arg any, ok bool) {
	m, isMap := v.(map[string]any)
	if !isMap || len(m) != 1 {
		return "", nil, false
	}
	for k, a := range m {
		switch k {
		case KindRef, KindGetAtt, KindParam, KindJoin:
			return k, a, true
		}
	}
	return "", nil, false
}

func getAttArgs(arg any) (id, attr string, err error) {
	list, ok := arg.([]any)
	if !ok || len(list) != 2 {
		return "", "", fmt.Errorf("getAtt takes [logicalID, attribute]")
	}
	id, ok1 := list[0].(string)
	attr, ok2 := list[1].(string)
	if !ok1 || !ok2 || id == "" || attr == "" {
		return "", "", fmt.Errorf("getAtt takes [logicalID, attribute] strings")
	}
	return id, attr, nil
}

func joinArgs(arg any) (sep string, parts []any, err error) {
	list, ok := arg.([]any)
	if !ok || len(list) != 2 {
		return "", nil, fmt.Errorf("join takes [separator, [parts...]]")
	}
	sep, ok1 := list[0].(string)
	parts, ok2 := list[1].([]any)
	if !ok1 || !ok2 {
		return "", nil, fmt.Errorf("join takes [separator string, [parts...]]")
	}
	return sep, parts, nil
}

// Walk visits every intrinsic in v (depth first) and validates its shape.
// Intrinsics nested inside join arguments are visited too.
func Walk(v any, fn func(kind string, arg any) error) error {
	if kind, arg, ok := AsIntrinsic(v); ok {
		switch kind {
		case KindRef, KindParam:
			if s, ok := arg.(string); !ok || s == "" {
				return fmt.Errorf("%s takes a non-empty string", kind)
			}
		case KindGetAtt:
			if _, _, err := getAttArgs(arg); err != nil {
				return err
			}
		case KindJoin:
			_, parts, err := joinArgs(arg)
			if err != nil {
				return err
			}
			if err := fn(kind, arg); err != nil {
				return err
			}
			for _, p := range parts {
				if err := Walk(p, fn); err != nil {
					return err
				}
			}
			return nil
		}
		return fn(kind, arg)
	}
	switch t := v.(type) {
	case map[string]any:
		for _, k := range sortedKeys(t) {
			if err := Walk(t[k], fn); err != nil {
				return err
			}
		}
	case []any:
		for _, e := range t {
			if err := Walk(e, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

// References returns the logical IDs referenced (via ref/getAtt) by v, sorted.
func References(v any) []string {
	seen := map[string]bool{}
	_ = Walk(v, func(kind string, arg any) error {
		switch kind {
		case KindRef:
			if s, ok := arg.(string); ok {
				seen[s] = true
			}
		case KindGetAtt:
			if id, _, err := getAttArgs(arg); err == nil {
				seen[id] = true
			}
		}
		return nil
	})
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Resolver supplies values for intrinsics.
type Resolver interface {
	Ref(logicalID string) (any, error)
	GetAtt(logicalID, attr string) (any, error)
	Param(name string) (any, error)
}

// Resolve returns a copy of v with every intrinsic replaced by its value.
// A resolver may return Unknown{}; joins containing unknowns become Unknown.
func Resolve(v any, r Resolver) (any, error) {
	if kind, arg, ok := AsIntrinsic(v); ok {
		switch kind {
		case KindRef:
			return r.Ref(arg.(string))
		case KindGetAtt:
			id, attr, err := getAttArgs(arg)
			if err != nil {
				return nil, err
			}
			return r.GetAtt(id, attr)
		case KindParam:
			return r.Param(arg.(string))
		case KindJoin:
			sep, parts, err := joinArgs(arg)
			if err != nil {
				return nil, err
			}
			strs := make([]string, 0, len(parts))
			for _, p := range parts {
				rv, err := Resolve(p, r)
				if err != nil {
					return nil, err
				}
				if _, unk := rv.(Unknown); unk {
					return Unknown{}, nil
				}
				strs = append(strs, fmt.Sprint(rv))
			}
			return strings.Join(strs, sep), nil
		}
	}
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			rv, err := Resolve(e, r)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
			out[k] = rv
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			rv, err := Resolve(e, r)
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", i, err)
			}
			out[i] = rv
		}
		return out, nil
	}
	return v, nil
}

// ResolveMap resolves a property map.
func ResolveMap(props map[string]any, r Resolver) (map[string]any, error) {
	if props == nil {
		return map[string]any{}, nil
	}
	v, err := Resolve(props, r)
	if err != nil {
		return nil, err
	}
	return v.(map[string]any), nil
}

// ContainsUnknown reports whether v contains an Unknown anywhere.
func ContainsUnknown(v any) bool {
	switch t := v.(type) {
	case Unknown:
		return true
	case map[string]any:
		for _, e := range t {
			if ContainsUnknown(e) {
				return true
			}
		}
	case []any:
		for _, e := range t {
			if ContainsUnknown(e) {
				return true
			}
		}
	}
	return false
}

// Normalize converts a value to its canonical JSON form (numbers become
// float64, typed maps and slices become map[string]any and []any) so that
// values from different sources compare reliably.
func Normalize(v any) any {
	if v == nil {
		return nil
	}
	switch v.(type) {
	case string, bool, float64:
		return v
	}
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		return v
	}
	return out
}

// Equal compares two values after normalization.
func Equal(a, b any) bool {
	return reflect.DeepEqual(Normalize(a), Normalize(b))
}
