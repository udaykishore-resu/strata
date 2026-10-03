// Package config loads server configuration from environment variables
// (twelve-factor style, matching how Cloud Run injects settings).
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the server configuration.
type Config struct {
	Addr string // listen address; PORT is honored for Cloud Run
	Mode string // all | api | worker

	Store    string // memory | postgres
	Provider string // gcp | fake

	Project string
	Region  string

	Auth           string // none | google | cloudrun-iam
	Audiences      []string
	AllowedCallers string

	WorkerConcurrency   int
	ResourceConcurrency int
	Lease               time.Duration
	ActionTimeout       time.Duration
	ShutdownGrace       time.Duration

	DenyPublicMembers bool
	FakeFaults        string
	FakeLatency       time.Duration
	LogLevel          string
	LogFormat         string
}

// Dev returns a configuration for local development: in-memory store,
// fake cloud, no authentication.
func Dev() Config {
	c := defaults()
	c.Store, c.Provider, c.Auth = "memory", "fake", "none"
	c.Project, c.Region = "dev-project", "us-central1"
	c.LogFormat = "text"
	c.FakeLatency = 300 * time.Millisecond
	return c
}

func defaults() Config {
	return Config{
		Addr: ":8080", Mode: "all", Store: "postgres", Provider: "gcp", Region: "us-central1",
		Auth: "google", WorkerConcurrency: 4, ResourceConcurrency: 8,
		Lease: 60 * time.Second, ActionTimeout: 30 * time.Minute, ShutdownGrace: 8 * time.Second,
		DenyPublicMembers: false, LogLevel: "info", LogFormat: "json",
	}
}

// Load reads configuration from the environment on top of base.
func Load(base *Config) (Config, error) {
	c := defaults()
	if base != nil {
		c = *base
	}
	var errs []error
	str := func(key string, dst *string) {
		if v := os.Getenv(key); v != "" {
			*dst = v
		}
	}
	num := func(key string, dst *int) {
		if v := os.Getenv(key); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				errs = append(errs, fmt.Errorf("%s must be a positive integer", key))
				return
			}
			*dst = n
		}
	}
	dur := func(key string, dst *time.Duration) {
		if v := os.Getenv(key); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil || d < 0 {
				errs = append(errs, fmt.Errorf("%s must be a duration like 30s", key))
				return
			}
			*dst = d
		}
	}
	boolean := func(key string, dst *bool) {
		if v := os.Getenv(key); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s must be true or false", key))
				return
			}
			*dst = b
		}
	}

	if p := os.Getenv("PORT"); p != "" {
		c.Addr = ":" + p
	}
	str("STRATA_ADDR", &c.Addr)
	str("STRATA_MODE", &c.Mode)
	str("STRATA_STORE", &c.Store)
	str("STRATA_PROVIDER", &c.Provider)
	str("GOOGLE_CLOUD_PROJECT", &c.Project)
	str("STRATA_PROJECT", &c.Project)
	str("STRATA_REGION", &c.Region)
	str("STRATA_AUTH", &c.Auth)
	if v := os.Getenv("STRATA_AUTH_AUDIENCES"); v != "" {
		c.Audiences = splitList(v)
	}
	str("STRATA_ALLOWED_CALLERS", &c.AllowedCallers)
	num("STRATA_WORKER_CONCURRENCY", &c.WorkerConcurrency)
	num("STRATA_RESOURCE_CONCURRENCY", &c.ResourceConcurrency)
	dur("STRATA_LEASE", &c.Lease)
	dur("STRATA_ACTION_TIMEOUT", &c.ActionTimeout)
	dur("STRATA_SHUTDOWN_GRACE", &c.ShutdownGrace)
	boolean("STRATA_DENY_PUBLIC_MEMBERS", &c.DenyPublicMembers)
	str("STRATA_FAKE_FAULTS", &c.FakeFaults)
	dur("STRATA_FAKE_LATENCY", &c.FakeLatency)
	str("STRATA_LOG_LEVEL", &c.LogLevel)
	str("STRATA_LOG_FORMAT", &c.LogFormat)

	errs = append(errs, c.validate()...)
	return c, errors.Join(errs...)
}

func (c Config) validate() []error {
	var errs []error
	oneOf := func(name, v string, allowed ...string) {
		for _, a := range allowed {
			if v == a {
				return
			}
		}
		errs = append(errs, fmt.Errorf("%s must be one of %s (got %q)", name, strings.Join(allowed, ", "), v))
	}
	oneOf("STRATA_MODE", c.Mode, "all", "api", "worker")
	oneOf("STRATA_STORE", c.Store, "memory", "postgres")
	oneOf("STRATA_PROVIDER", c.Provider, "gcp", "fake")
	oneOf("STRATA_AUTH", c.Auth, "none", "google", "cloudrun-iam")
	if c.Auth == "google" && len(c.Audiences) == 0 {
		errs = append(errs, errors.New("STRATA_AUTH=google requires STRATA_AUTH_AUDIENCES (the service URL, plus 32555940559.apps.googleusercontent.com to accept gcloud user tokens)"))
	}
	if c.Store == "memory" && c.Mode != "all" {
		errs = append(errs, errors.New("the memory store only works with STRATA_MODE=all"))
	}
	if c.Lease < 10*time.Second && c.Provider == "gcp" {
		errs = append(errs, errors.New("STRATA_LEASE must be at least 10s"))
	}
	return errs
}

// RunsAPI and RunsWorker report which roles this process serves.
func (c Config) RunsAPI() bool    { return c.Mode == "all" || c.Mode == "api" }
func (c Config) RunsWorker() bool { return c.Mode == "all" || c.Mode == "worker" }

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
