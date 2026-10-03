package auth

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func sign(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	enc := base64.RawURLEncoding
	h, _ := json.Marshal(map[string]string{"alg": "RS256", "kid": kid, "typ": "JWT"})
	c, _ := json.Marshal(claims)
	signing := enc.EncodeToString(h) + "." + enc.EncodeToString(c)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return signing + "." + enc.EncodeToString(sig)
}

func TestGoogleIDTokenVerification(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=600")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{
			"kid": "k1", "kty": "RSA",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	}))
	defer jwks.Close()

	g := &GoogleIDToken{
		Audiences:       []string{"https://strata.example.run.app"},
		Allow:           ParseAllowlist("domain:example.com, ops@partner.io"),
		VerifySignature: true,
		Keys:            &KeySet{URL: jwks.URL, Client: http.DefaultClient},
	}
	now := time.Now().Unix()
	good := map[string]any{"iss": "https://accounts.google.com", "aud": "https://strata.example.run.app", "sub": "1",
		"email": "dev@example.com", "email_verified": true, "iat": now, "exp": now + 3600}
	with := func(k string, v any) map[string]any {
		c := map[string]any{}
		for kk, vv := range good {
			c[kk] = vv
		}
		c[k] = v
		return c
	}
	req := func(tok string) *http.Request {
		r := httptest.NewRequest("GET", "/v1/stacks", nil)
		if tok != "" {
			r.Header.Set("Authorization", "Bearer "+tok)
		}
		return r
	}

	p, err := g.Authenticate(req(sign(t, key, "k1", good)))
	if err != nil || p.Email != "dev@example.com" {
		t.Fatalf("valid token rejected: %v", err)
	}
	cases := map[string]struct {
		tok  string
		want error
	}{
		"missing":      {"", ErrUnauthenticated},
		"bad sig":      {sign(t, other, "k1", good), ErrUnauthenticated},
		"unknown kid":  {sign(t, key, "k2", good), ErrUnauthenticated},
		"expired":      {sign(t, key, "k1", with("exp", now-3600)), ErrUnauthenticated},
		"wrong aud":    {sign(t, key, "k1", with("aud", "https://evil.example")), ErrUnauthenticated},
		"wrong iss":    {sign(t, key, "k1", with("iss", "https://evil.example")), ErrUnauthenticated},
		"not allowed":  {sign(t, key, "k1", with("email", "intruder@gmail.com")), ErrForbidden},
		"unverified":   {sign(t, key, "k1", with("email_verified", false)), ErrUnauthenticated},
		"alg none":     {"eyJhbGciOiJub25lIn0." + base64.RawURLEncoding.EncodeToString([]byte(`{"email":"dev@example.com"}`)) + ".", ErrUnauthenticated},
		"partner user": {sign(t, key, "k1", with("email", "ops@partner.io")), nil},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := g.Authenticate(req(tc.tok))
			if tc.want == nil && err != nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestCloudRunIAMModeDecodesWithoutSignature(t *testing.T) {
	g := &GoogleIDToken{Allow: ParseAllowlist("")}
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"email":"ci@proj.iam.gserviceaccount.com","sub":"9"}`))
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer eyJhbGciOiJSUzI1NiJ9."+payload+".SIGNATURE_REMOVED")
	p, err := g.Authenticate(r)
	if err != nil || p.Email != "ci@proj.iam.gserviceaccount.com" {
		t.Fatalf("got %v %v", p, err)
	}
}

func TestAllowlist(t *testing.T) {
	a := ParseAllowlist("user:Alice@Example.com, domain:corp.io, serviceAccount:ci@p.iam.gserviceaccount.com")
	for email, want := range map[string]bool{
		"alice@example.com": true, "bob@corp.io": true, "ci@p.iam.gserviceaccount.com": true,
		"bob@example.com": false, "x@evilcorp.io": false,
	} {
		if a.Allows(email) != want {
			t.Errorf("Allows(%s) = %v", email, !want)
		}
	}
	if !ParseAllowlist("").Allows("anyone@x.com") {
		t.Error("empty allowlist should allow all")
	}
}
