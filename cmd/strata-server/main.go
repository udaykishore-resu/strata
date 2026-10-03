// Command strata-server runs the Strata control plane: the HTTP API and/or
// the operation workers, depending on STRATA_MODE.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/udaykishore-resu/strata/internal/api"
	"github.com/udaykishore-resu/strata/internal/auth"
	"github.com/udaykishore-resu/strata/internal/config"
	"github.com/udaykishore-resu/strata/internal/engine"
	"github.com/udaykishore-resu/strata/internal/obs"
	"github.com/udaykishore-resu/strata/internal/provider"
	"github.com/udaykishore-resu/strata/internal/provider/fake"
	"github.com/udaykishore-resu/strata/internal/provider/gcp"
	"github.com/udaykishore-resu/strata/internal/state"
	"github.com/udaykishore-resu/strata/internal/store/memory"
	"github.com/udaykishore-resu/strata/internal/store/postgres"
	"github.com/udaykishore-resu/strata/internal/worker"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	dev := flag.Bool("dev", false, "development mode: in-memory store, fake cloud, no auth")
	flag.Parse()
	if err := run(*dev); err != nil {
		fmt.Fprintln(os.Stderr, "strata-server:", err)
		os.Exit(1)
	}
}

func run(dev bool) error {
	var base *config.Config
	if dev {
		d := config.Dev()
		base = &d
	}
	cfg, err := config.Load(base)
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}
	log := obs.NewLogger(os.Stderr, cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cfg.Project == "" && cfg.Provider == "gcp" {
		if p, err := gcp.MetadataProject(ctx, http.DefaultClient); err == nil {
			cfg.Project = p
		} else {
			return errors.New("STRATA_PROJECT is required (could not read it from the metadata server)")
		}
	}

	store, closeStore, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeStore()

	registry, err := buildRegistry(ctx, cfg, log)
	if err != nil {
		return err
	}

	metrics := obs.NewMetrics()
	eng := &engine.Engine{
		Store: store, Registry: registry, Project: cfg.Project, Region: cfg.Region,
		Concurrency: cfg.ResourceConcurrency, ActionTimeout: cfg.ActionTimeout,
		DenyPublicMembers: cfg.DenyPublicMembers, Log: log,
		Observe: func(typ, action string, err error, d time.Duration) {
			result := "ok"
			switch {
			case err == nil:
			case errors.Is(err, provider.ErrNotFound):
				result = "not_found"
			default:
				result = "error"
			}
			metrics.ProviderCalls.Inc(typ, action, result)
			metrics.ProviderDuration.Observe(d.Seconds(), typ, action)
		},
	}

	authn, err := buildAuth(cfg)
	if err != nil {
		return err
	}

	host, _ := os.Hostname()
	w := &worker.Worker{
		Store: store, Engine: eng, ID: fmt.Sprintf("%s-%d", host, os.Getpid()),
		Concurrency: cfg.WorkerConcurrency, Lease: cfg.Lease, Log: log, Metrics: metrics,
	}
	metrics.Registry.NewGaugeFunc("strata_operations_running", "Operations executing in this process.", func() float64 { return float64(w.Running()) })

	srv := &http.Server{
		Addr: cfg.Addr,
		Handler: (&api.Server{
			Engine: eng, Auth: authn, Log: log, Metrics: metrics, Version: version,
			HealthOnly: !cfg.RunsAPI(),
		}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      10 * time.Minute, // drift detection reads every resource
		IdleTimeout:       120 * time.Second,
	}

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	if cfg.RunsWorker() {
		go func() { defer close(workerDone); w.Run(workerCtx) }()
	} else {
		close(workerDone)
	}

	serverErr := make(chan error, 1)
	go func() {
		// Worker-only processes still serve health and metrics endpoints.
		log.Info("strata-server listening", "addr", cfg.Addr, "mode", cfg.Mode, "store", cfg.Store,
			"provider", cfg.Provider, "auth", cfg.Auth, "project", cfg.Project, "region", cfg.Region, "version", version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-serverErr:
		cancelWorker()
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	cancelWorker() // running operations checkpoint and hand back their leases
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	select {
	case <-workerDone:
	case <-shutdownCtx.Done():
		log.Warn("worker did not stop within grace period; leases will expire and operations resume elsewhere")
	}
	log.Info("stopped")
	return nil
}

func openStore(ctx context.Context, cfg config.Config) (state.Store, func(), error) {
	if cfg.Store == "memory" {
		return memory.New(), func() {}, nil
	}
	dsn, err := postgres.DSNFromEnv()
	if err != nil {
		return nil, nil, err
	}
	s, err := postgres.Open(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}
	return s, func() { s.Close() }, nil
}

func buildRegistry(ctx context.Context, cfg config.Config, log *slog.Logger) (*provider.Registry, error) {
	if cfg.Provider == "fake" {
		cloud := fake.NewCloud()
		cloud.Faults = fake.NewFaults(cfg.FakeFaults)
		cloud.Latency = cfg.FakeLatency
		log.Warn("using the fake cloud provider; no real resources will be created")
		return provider.NewRegistry(fake.Mirror(cloud, gcp.Schemas())...), nil
	}
	ts, source, err := gcp.DefaultTokenSource(ctx, http.DefaultClient)
	if err != nil {
		return nil, err
	}
	log.Info("google credentials", "source", source)
	client := gcp.NewClient(ts)
	client.UserAgent = "strata-engine/" + version
	if qp := os.Getenv("STRATA_QUOTA_PROJECT"); qp != "" {
		client.QuotaProject = qp
	}
	return provider.NewRegistry(gcp.Providers(client)...), nil
}

func buildAuth(cfg config.Config) (auth.Authenticator, error) {
	allow := auth.ParseAllowlist(cfg.AllowedCallers)
	switch cfg.Auth {
	case "none":
		return auth.None{}, nil
	case "google":
		return &auth.GoogleIDToken{Audiences: cfg.Audiences, Allow: allow, VerifySignature: true, Keys: auth.GoogleKeys()}, nil
	case "cloudrun-iam":
		return &auth.GoogleIDToken{Allow: allow}, nil
	}
	return nil, fmt.Errorf("unknown auth mode %q", cfg.Auth)
}
