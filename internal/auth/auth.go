// Package auth authenticates API callers with Google-signed ID tokens.
//
// Modes:
//   - none:         no authentication (local development only)
//   - google:       verify the ID token signature against Google's public
//     keys, plus issuer, audience and expiry
//   - cloudrun-iam: Cloud Run IAM (roles/run.invoker) has already verified
//     the token at the edge; decode it for the caller identity only
//
// In every mode except none an optional allowlist restricts callers by
// email or domain.
package auth

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Principal is an authenticated caller.
type Principal struct {
	Email   string `json:"email"`
	Subject string `json:"subject"`
}

// ErrUnauthenticated means no valid credentials were presented (HTTP 401).
var ErrUnauthenticated = errors.New("unauthenticated")

// ErrForbidden means the caller is authenticated but not allowed (HTTP 403).
var ErrForbidden = errors.New("caller is not allowed")

// Authenticator authenticates requests.
type Authenticator interface {
	Authenticate(r *http.Request) (*Principal, error)
}

type ctxKey struct{}

// WithPrincipal stores p in ctx.
func WithPrincipal(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// FromContext returns the caller, or nil.
func FromContext(ctx context.Context) *Principal {
	p, _ := ctx.Value(ctxKey{}).(*Principal)
	return p
}

// None accepts every request (development).
type None struct{}

func (None) Authenticate(*http.Request) (*Principal, error) {
	return &Principal{Email: "anonymous", Subject: "anonymous"}, nil
}

// Allowlist restricts principals by exact email or "domain:example.com".
// An empty allowlist allows every authenticated principal.
type Allowlist struct {
	emails  map[string]bool
	domains map[string]bool
}

// ParseAllowlist parses a comma separated list.
func ParseAllowlist(spec string) Allowlist {
	a := Allowlist{emails: map[string]bool{}, domains: map[string]bool{}}
	for _, e := range strings.Split(spec, ",") {
		e = strings.ToLower(strings.TrimSpace(e))
		switch {
		case e == "":
		case strings.HasPrefix(e, "domain:"):
			a.domains[strings.TrimPrefix(e, "domain:")] = true
		default:
			a.emails[strings.TrimPrefix(strings.TrimPrefix(e, "user:"), "serviceaccount:")] = true
		}
	}
	return a
}

// Allows reports whether email may call the API.
func (a Allowlist) Allows(email string) bool {
	if len(a.emails) == 0 && len(a.domains) == 0 {
		return true
	}
	email = strings.ToLower(email)
	if a.emails[email] {
		return true
	}
	if i := strings.LastIndex(email, "@"); i >= 0 && a.domains[email[i+1:]] {
		return true
	}
	return false
}

// GoogleIDToken authenticates Bearer ID tokens issued by Google.
type GoogleIDToken struct {
	// Audiences the token's aud claim must match (at least one required
	// when VerifySignature is true).
	Audiences []string
	Allow     Allowlist
	// VerifySignature is false in cloudrun-iam mode.
	VerifySignature bool
	Keys            *KeySet
	Now             func() time.Time
}

type claims struct {
	Iss           string `json:"iss"`
	Aud           any    `json:"aud"`
	Sub           string `json:"sub"`
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	Exp           int64  `json:"exp"`
	Iat           int64  `json:"iat"`
}

func (g *GoogleIDToken) Authenticate(r *http.Request) (*Principal, error) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return nil, ErrUnauthenticated
	}
	tok := strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
	parts := strings.Split(tok, ".")
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: malformed token", ErrUnauthenticated)
	}
	var c claims
	if err := decodeSegment(parts[1], &c); err != nil {
		return nil, fmt.Errorf("%w: malformed claims", ErrUnauthenticated)
	}
	if g.VerifySignature {
		if len(parts) != 3 {
			return nil, fmt.Errorf("%w: unsigned token", ErrUnauthenticated)
		}
		if err := g.verify(r.Context(), parts, &c); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
		}
	}
	if c.Email == "" {
		return nil, fmt.Errorf("%w: token has no email claim (request it with --include-email or an audience)", ErrUnauthenticated)
	}
	if !g.Allow.Allows(c.Email) {
		return nil, fmt.Errorf("%w: %s", ErrForbidden, c.Email)
	}
	return &Principal{Email: c.Email, Subject: c.Sub}, nil
}

func (g *GoogleIDToken) verify(ctx context.Context, parts []string, c *claims) error {
	var hdr struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := decodeSegment(parts[0], &hdr); err != nil {
		return errors.New("malformed header")
	}
	if hdr.Alg != "RS256" {
		return fmt.Errorf("unsupported alg %q", hdr.Alg)
	}
	key, err := g.Keys.Get(ctx, hdr.Kid)
	if err != nil {
		return err
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return errors.New("malformed signature")
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig); err != nil {
		return errors.New("invalid signature")
	}
	if c.Iss != "accounts.google.com" && c.Iss != "https://accounts.google.com" {
		return fmt.Errorf("unexpected issuer %q", c.Iss)
	}
	now := time.Now
	if g.Now != nil {
		now = g.Now
	}
	const skew = 60
	if t := now().Unix(); c.Exp+skew < t || c.Iat-skew > t {
		return errors.New("token expired or not yet valid")
	}
	if !g.audienceOK(c.Aud) {
		return fmt.Errorf("audience not accepted")
	}
	if c.Email != "" && !c.EmailVerified {
		return errors.New("email not verified")
	}
	return nil
}

func (g *GoogleIDToken) audienceOK(aud any) bool {
	var auds []string
	switch v := aud.(type) {
	case string:
		auds = []string{v}
	case []any:
		for _, a := range v {
			if s, ok := a.(string); ok {
				auds = append(auds, s)
			}
		}
	}
	for _, want := range g.Audiences {
		for _, got := range auds {
			if strings.TrimRight(want, "/") == strings.TrimRight(got, "/") {
				return true
			}
		}
	}
	return false
}

func decodeSegment(seg string, v any) error {
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(seg, "="))
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// KeySet caches Google's token signing keys (JWKS).
type KeySet struct {
	URL    string
	Client *http.Client
	mu     sync.Mutex
	keys   map[string]*rsa.PublicKey
	expiry time.Time
	last   time.Time
}

// GoogleKeys returns a key set for Google's OAuth2 certificates.
func GoogleKeys() *KeySet {
	return &KeySet{URL: "https://www.googleapis.com/oauth2/v3/certs", Client: &http.Client{Timeout: 10 * time.Second}}
}

// Get returns the key with id kid, refreshing the set when it is stale or
// the kid is unknown (at most once every 30 seconds).
func (k *KeySet) Get(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if key, ok := k.keys[kid]; ok && time.Now().Before(k.expiry) {
		return key, nil
	}
	if time.Since(k.last) > 30*time.Second || k.keys == nil {
		if err := k.refresh(ctx); err != nil {
			return nil, err
		}
	}
	if key, ok := k.keys[kid]; ok {
		return key, nil
	}
	return nil, fmt.Errorf("unknown signing key %q", kid)
}

func (k *KeySet) refresh(ctx context.Context) error {
	k.last = time.Now()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, k.URL, nil)
	resp, err := k.Client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch signing keys: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch signing keys: status %d", resp.StatusCode)
	}
	var set struct {
		Keys []struct {
			Kid string `json:"kid"`
			Kty string `json:"kty"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return fmt.Errorf("decode signing keys: %w", err)
	}
	keys := map[string]*rsa.PublicKey{}
	for _, jk := range set.Keys {
		if jk.Kty != "RSA" {
			continue
		}
		n, err1 := base64.RawURLEncoding.DecodeString(jk.N)
		e, err2 := base64.RawURLEncoding.DecodeString(jk.E)
		if err1 != nil || err2 != nil {
			continue
		}
		keys[jk.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
	}
	k.keys = keys
	k.expiry = time.Now().Add(time.Hour)
	if cc := resp.Header.Get("Cache-Control"); cc != "" {
		for _, part := range strings.Split(cc, ",") {
			var age int
			if _, err := fmt.Sscanf(strings.TrimSpace(part), "max-age=%d", &age); err == nil && age > 0 {
				k.expiry = time.Now().Add(time.Duration(age) * time.Second)
			}
		}
	}
	return nil
}
