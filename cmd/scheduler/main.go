// Command scheduler runs the leader-elected scheduling loop: it creates queued runs for
// due, dependency-satisfied jobs, advances due runs through scheduling, and assigns them
// to live workers with matching resources. Multiple replicas may run for availability,
// but only the elected leader promotes and dispatches work at any moment (see internal/lock).
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/G1lollipop/atlas/internal/config"
	"github.com/G1lollipop/atlas/internal/lock"
	"github.com/G1lollipop/atlas/internal/logger"
	"github.com/G1lollipop/atlas/internal/metrics"
	"github.com/G1lollipop/atlas/internal/scheduler"
	"github.com/G1lollipop/atlas/internal/store"
	"github.com/G1lollipop/atlas/internal/tracing"
)

// promotionLockKey is an arbitrary fixed key identifying the "promoter leader" slot in
// pg_try_advisory_lock's global keyspace. It only needs to be distinct from any other
// advisory lock this system might introduce later.
const promotionLockKey = 727433001

func main() {
	log := logger.New("scheduler")

	cfg, err := config.Load()
	if err != nil {
		log.Error("load config", "error", err)
		os.Exit(1)
	}
	policy, err := scheduler.ParseSchedulingPolicy(os.Getenv("SCHEDULING_POLICY"))
	if err != nil {
		log.Error("load scheduling policy", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// fatal runs the deferred stop() before exiting (a bare os.Exit here would skip it),
	// since we're past the point where stop is registered.
	fatal := func(msg string, err error) {
		log.Error(msg, "error", err)
		stop()
		os.Exit(1)
	}

	shutdownTracing, err := tracing.Init(ctx, "scheduler", cfg.OTLPEndpoint)
	if err != nil {
		fatal("init tracing", err)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		fatal("connect to database", err)
	}
	defer st.Close()
	// API and scheduler replicas share queue state, so deploy them with identical
	// MAX_QUEUE_DEPTH/PER_QUEUE_LIMIT/PER_TENANT_LIMIT values.
	if err := st.SetQueueLimits(store.QueueLimits{
		MaxQueueDepth: cfg.MaxQueueDepth, PerQueueLimit: cfg.PerQueueLimit, PerTenantLimit: cfg.PerTenantLimit,
	}); err != nil {
		fatal("configure queue backpressure", err)
	}

	if err := store.RunMigrations(ctx, st.Pool(), "migrations"); err != nil {
		fatal("run migrations", err)
	}
	if err := prometheus.DefaultRegisterer.Register(metrics.NewObservabilityCollector(
		st, metrics.DefaultObservabilityTimeout, scheduler.HeartbeatTTL,
	)); err != nil {
		fatal("register scheduler observability collector", err)
	}

	elector := lock.NewPostgresElector(st.Pool(), promotionLockKey)
	promoter := scheduler.NewPromoter(st, elector, log, cfg.PollInterval)
	promoter.Policy = policy

	metricsServer := &http.Server{Addr: cfg.MetricsAddr, Handler: metrics.Handler()}
	go func() {
		log.Info("metrics server listening", "addr", cfg.MetricsAddr)
		_ = metricsServer.ListenAndServe()
	}()

	log.Info("scheduler starting", "interval", cfg.PollInterval, "policy", scheduler.SchedulingPolicyName(policy))
	runErr := promoter.Run(ctx)
	log.Info("scheduler stopped", "reason", runErr)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = metricsServer.Shutdown(shutdownCtx)
}
