package gcp

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

// TokenSource returns OAuth2 access tokens for Google APIs.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// StaticToken is a fixed access token (tests, or STRATA_GCP_ACCESS_TOKEN).
type StaticToken string

func (s StaticToken) Token(context.Context) (string, error) { return string(s), nil }

type fetchFunc func(ctx context.Context) (token string, expiry time.Time, err error)

// cachingSource caches a token until five minutes before it expires.
type cachingSource struct {
	mu     sync.Mutex
	fetch  fetchFunc
	token  string
	expiry time.Time
}

func (c *cachingSource) Token(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && time.Until(c.expiry) > 5*time.Minute {
		return c.token, nil
	}
	tok, exp, err := c.fetch(ctx)
	if err != nil {
		return "", err
	}
	c.token, c.expiry = tok, exp
	return tok, nil
}

// DefaultTokenSource finds credentials the way Google client libraries do:
//  1. STRATA_GCP_ACCESS_TOKEN (static token)
//  2. GOOGLE_APPLICATION_CREDENTIALS (service account, authorized user or
//     impersonated service account JSON)
//  3. gcloud application-default credentials
//  4. the GCE/Cloud Run metadata server
func DefaultTokenSource(ctx context.Context, hc *http.Client) (TokenSource, string, error) {
	if hc == nil {
		hc = http.DefaultClient
	}
	if tok := os.Getenv("STRATA_GCP_ACCESS_TOKEN"); tok != "" {
		return StaticToken(tok), "static token", nil
	}
	if path := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); path != "" {
		ts, err := tokenSourceFromFile(hc, path)
		return ts, "credentials file " + path, err
	}
	if path := wellKnownADC(); path != "" {
		if _, err := os.Stat(path); err == nil {
			ts, err := tokenSourceFromFile(hc, path)
			return ts, "application default credentials", err
		}
	}
	if onGCE(ctx, hc) {
		return &cachingSource{fetch: metadataFetch(hc)}, "metadata server", nil
	}
	return nil, "", errors.New("no Google credentials found: set GOOGLE_APPLICATION_CREDENTIALS, run `gcloud auth application-default login`, or run on GCP")
}

func wellKnownADC() string {
	if d := os.Getenv("CLOUDSDK_CONFIG"); d != "" {
		return filepath.Join(d, "application_default_credentials.json")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "gcloud", "application_default_credentials.json")
}

func metadataHost() string {
	if h := os.Getenv("GCE_METADATA_HOST"); h != "" {
		return h
	}
	return "metadata.google.internal"
}

func onGCE(ctx context.Context, hc *http.Client) bool {
	if os.Getenv("K_SERVICE") != "" || os.Getenv("GCE_METADATA_HOST") != "" {
		return true
	}
	cctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(cctx, http.MethodGet, "http://"+metadataHost()+"/computeMetadata/v1/", nil)
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := hc.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.Header.Get("Metadata-Flavor") == "Google"
}

func metadataFetch(hc *http.Client) fetchFunc {
	return func(ctx context.Context) (string, time.Time, error) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
			"http://"+metadataHost()+"/computeMetadata/v1/instance/service-accounts/default/token", nil)
		req.Header.Set("Metadata-Flavor", "Google")
		return doTokenRequest(hc, req)
	}
}

// MetadataProject returns the project ID from the metadata server.
func MetadataProject(ctx context.Context, hc *http.Client) (string, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+metadataHost()+"/computeMetadata/v1/project/project-id", nil)
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil || resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata project-id: status %d", resp.StatusCode)
	}
	return strings.TrimSpace(string(b)), nil
}

type credentialsFile struct {
	Type string `json:"type"`
	// service_account
	ClientEmail  string `json:"client_email"`
	PrivateKey   string `json:"private_key"`
	PrivateKeyID string `json:"private_key_id"`
	TokenURI     string `json:"token_uri"`
	// authorized_user
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	RefreshToken string `json:"refresh_token"`
	// impersonated_service_account
	ServiceAccountImpersonationURL string           `json:"service_account_impersonation_url"`
	SourceCredentials              *credentialsFile `json:"source_credentials"`
}

func tokenSourceFromFile(hc *http.Client, path string) (TokenSource, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read credentials: %w", err)
	}
	var cf credentialsFile
	if err := json.Unmarshal(b, &cf); err != nil {
		return nil, fmt.Errorf("parse credentials %s: %w", path, err)
	}
	fetch, err := fetchFor(hc, &cf)
	if err != nil {
		return nil, err
	}
	return &cachingSource{fetch: fetch}, nil
}

func fetchFor(hc *http.Client, cf *credentialsFile) (fetchFunc, error) {
	switch cf.Type {
	case "service_account":
		key, err := parseRSAKey(cf.PrivateKey)
		if err != nil {
			return nil, err
		}
		tokenURI := cf.TokenURI
		if tokenURI == "" {
			tokenURI = "https://oauth2.googleapis.com/token"
		}
		return func(ctx context.Context) (string, time.Time, error) {
			assertion, err := signJWT(key, cf.PrivateKeyID, map[string]any{
				"iss": cf.ClientEmail, "scope": cloudPlatformScope, "aud": tokenURI,
				"iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix(),
			})
			if err != nil {
				return "", time.Time{}, err
			}
			form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}, "assertion": {assertion}}
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, tokenURI, strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			return doTokenRequest(hc, req)
		}, nil
	case "authorized_user":
		return func(ctx context.Context) (string, time.Time, error) {
			form := url.Values{
				"grant_type": {"refresh_token"}, "client_id": {cf.ClientID},
				"client_secret": {cf.ClientSecret}, "refresh_token": {cf.RefreshToken},
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://oauth2.googleapis.com/token", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			return doTokenRequest(hc, req)
		}, nil
	case "impersonated_service_account":
		if cf.SourceCredentials == nil || cf.ServiceAccountImpersonationURL == "" {
			return nil, errors.New("impersonated credentials missing source_credentials or impersonation URL")
		}
		source, err := fetchFor(hc, cf.SourceCredentials)
		if err != nil {
			return nil, err
		}
		src := &cachingSource{fetch: source}
		return func(ctx context.Context) (string, time.Time, error) {
			tok, err := src.Token(ctx)
			if err != nil {
				return "", time.Time{}, err
			}
			body := strings.NewReader(`{"scope":["` + cloudPlatformScope + `"],"lifetime":"3600s"}`)
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, cf.ServiceAccountImpersonationURL, body)
			req.Header.Set("Authorization", "Bearer "+tok)
			req.Header.Set("Content-Type", "application/json")
			resp, err := hc.Do(req)
			if err != nil {
				return "", time.Time{}, err
			}
			defer resp.Body.Close()
			var out struct {
				AccessToken string `json:"accessToken"`
				ExpireTime  string `json:"expireTime"`
			}
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			if resp.StatusCode != http.StatusOK {
				return "", time.Time{}, fmt.Errorf("impersonation: status %d: %s", resp.StatusCode, b)
			}
			if err := json.Unmarshal(b, &out); err != nil {
				return "", time.Time{}, err
			}
			exp, _ := time.Parse(time.RFC3339, out.ExpireTime)
			return out.AccessToken, exp, nil
		}, nil
	}
	return nil, fmt.Errorf("unsupported credentials type %q", cf.Type)
}

func doTokenRequest(hc *http.Client, req *http.Request) (string, time.Time, error) {
	resp, err := hc.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("token request: status %d: %s", resp.StatusCode, b)
	}
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(b, &out); err != nil || out.AccessToken == "" {
		return "", time.Time{}, fmt.Errorf("token response: invalid body")
	}
	return out.AccessToken, time.Now().Add(time.Duration(out.ExpiresIn) * time.Second), nil
}

func parseRSAKey(pemKey string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemKey))
	if block == nil {
		return nil, errors.New("service account private_key is not PEM")
	}
	if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		rk, ok := k.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("service account key is not RSA")
		}
		return rk, nil
	}
	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

func signJWT(key *rsa.PrivateKey, kid string, claims map[string]any) (string, error) {
	header := map[string]string{"alg": "RS256", "typ": "JWT"}
	if kid != "" {
		header["kid"] = kid
	}
	hb, _ := json.Marshal(header)
	cb, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding
	signing := enc.EncodeToString(hb) + "." + enc.EncodeToString(cb)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	return signing + "." + enc.EncodeToString(sig), nil
}
