// Command worker leases job runs assigned to its worker from Postgres and executes
// them via a small built-in handler registry (echo/sleep/http_call). A real deployment
// would register domain-specific handlers here instead of (or in addition to) the examples.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/G1lollipop/atlas/internal/artifact"
	"github.com/G1lollipop/atlas/internal/config"
	"github.com/G1lollipop/atlas/internal/logger"
	"github.com/G1lollipop/atlas/internal/metrics"
	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
	"github.com/G1lollipop/atlas/internal/tracing"
	"github.com/G1lollipop/atlas/internal/worker"
)

func main() {
	log := logger.New("worker")

	cfg, err := config.Load()
	if err != nil {
		log.Error("load config", "error", err)
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
	if cfg.Concurrency < 0 {
		fatal("invalid worker concurrency", fmt.Errorf("WORKER_CONCURRENCY must be zero or greater, got %d", cfg.Concurrency))
	}
	retryPolicy, err := loadRetryPolicy()
	if err != nil {
		fatal("invalid worker retry policy", err)
	}

	shutdownTracing, err := tracing.Init(ctx, "worker", cfg.OTLPEndpoint)
	if err != nil {
		fatal("init tracing", err)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		fatal("connect to database", err)
	}
	defer st.Close()

	if err := store.RunMigrations(ctx, st.Pool(), "migrations"); err != nil {
		fatal("run migrations", err)
	}
	objects, maxArtifactBytes, closeArtifacts, err := loadArtifactStoreFromEnv()
	if err != nil {
		fatal("configure artifact store", err)
	}
	if closeArtifacts != nil {
		defer func() { _ = closeArtifacts() }()
	}

	pool := worker.NewPool(st, cfg.WorkerID, cfg.Concurrency, cfg.LeaseDuration, cfg.PollInterval, log)
	if err := pool.SetRetryPolicy(retryPolicy); err != nil {
		fatal("configure worker retry policy", err)
	}
	pool.SetCapabilities(model.Worker{
		CPUCapacity:      cfg.WorkerCPUCapacityMillis,
		MemoryCapacityMB: cfg.WorkerMemoryCapacityMB,
		GPUCount:         cfg.WorkerGPUCount,
		GPUType:          cfg.WorkerGPUType,
		GPUMemoryMB:      cfg.WorkerGPUMemoryMB,
		Labels:           cfg.WorkerLabels,
	})
	pool.RegisterHandler("echo", worker.EchoHandler)
	pool.RegisterHandler("sleep", worker.SleepHandler)
	pool.RegisterHandler("http_call", worker.HTTPCallHandler)
	pool.RegisterHandler("idempotent_record", worker.NewIdempotentRecordHandler(st))
	pool.RegisterHandler("embedding", worker.NewEmbeddingHandler(objects, maxArtifactBytes))
	pool.RegisterHandler("inference", worker.NewInferenceHandler(objects, maxArtifactBytes))
	pool.RegisterHandler("batch_transform", worker.NewBatchTransformHandler(objects, maxArtifactBytes))
	artifactBackend := strings.ToLower(strings.TrimSpace(os.Getenv("ARTIFACT_STORE")))
	if artifactBackend == "" {
		artifactBackend = "local"
	}

	metricsServer := &http.Server{Addr: cfg.MetricsAddr, Handler: metrics.Handler()}
	go func() {
		log.Info("metrics server listening", "addr", cfg.MetricsAddr)
		_ = metricsServer.ListenAndServe()
	}()

	log.Info("worker starting", "worker_id", cfg.WorkerID, "concurrency", cfg.Concurrency,
		"cpu_capacity_millis", cfg.WorkerCPUCapacityMillis,
		"memory_capacity_mb", cfg.WorkerMemoryCapacityMB,
		"gpu_count", cfg.WorkerGPUCount,
		"gpu_type", cfg.WorkerGPUType,
		"gpu_memory_mb", cfg.WorkerGPUMemoryMB,
		"labels", cfg.WorkerLabels,
		"artifact_store", artifactBackend,
		"artifact_max_bytes", maxArtifactBytes)
	runErr := pool.Run(ctx)
	log.Info("worker stopped", "reason", runErr)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = metricsServer.Shutdown(shutdownCtx)
}

func loadArtifactStoreFromEnv() (artifact.ArtifactStore, int64, func() error, error) {
	maxBytes := artifact.DefaultMaxObjectBytes
	if value := os.Getenv("ARTIFACT_MAX_BYTES"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed <= 0 || parsed > 1<<30 {
			return nil, 0, nil, fmt.Errorf("ARTIFACT_MAX_BYTES must be an integer from 1 through 1073741824")
		}
		maxBytes = parsed
	}
	backend := strings.ToLower(strings.TrimSpace(os.Getenv("ARTIFACT_STORE")))
	if backend == "" {
		backend = "local"
	}
	switch backend {
	case "local":
		root := os.Getenv("ARTIFACT_LOCAL_DIR")
		if root == "" {
			root = "./artifacts"
		}
		objects, err := artifact.NewLocalArtifactStore(root, maxBytes)
		if err != nil {
			return nil, 0, nil, err
		}
		return objects, maxBytes, objects.Close, nil
	case "s3":
		objects, err := artifact.NewS3ArtifactStore(artifact.S3Config{
			Bucket:       os.Getenv("ARTIFACT_S3_BUCKET"),
			Region:       os.Getenv("AWS_REGION"),
			Endpoint:     os.Getenv("ARTIFACT_S3_ENDPOINT"),
			AccessKey:    os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretKey:    os.Getenv("AWS_SECRET_ACCESS_KEY"),
			SessionToken: os.Getenv("AWS_SESSION_TOKEN"),
		}, maxBytes)
		if err != nil {
			return nil, 0, nil, err
		}
		return objects, maxBytes, nil, nil
	default:
		return nil, 0, nil, fmt.Errorf("ARTIFACT_STORE must be local or s3, got %q", backend)
	}
}

func loadRetryPolicy() (worker.RetryPolicy, error) {
	policy := worker.DefaultRetryPolicy()
	if value := os.Getenv("WORKER_RETRY_BASE_DELAY"); value != "" {
		delay, err := time.ParseDuration(value)
		if err != nil {
			return policy, fmt.Errorf("WORKER_RETRY_BASE_DELAY: %w", err)
		}
		policy.BaseDelay = delay
	}
	if value := os.Getenv("WORKER_RETRY_MULTIPLIER"); value != "" {
		multiplier, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return policy, fmt.Errorf("WORKER_RETRY_MULTIPLIER: %w", err)
		}
		policy.Multiplier = multiplier
	}
	if value := os.Getenv("WORKER_RETRY_MAX_DELAY"); value != "" {
		delay, err := time.ParseDuration(value)
		if err != nil {
			return policy, fmt.Errorf("WORKER_RETRY_MAX_DELAY: %w", err)
		}
		policy.MaxDelay = delay
	}
	if value := os.Getenv("WORKER_RETRY_JITTER"); value != "" {
		jitter, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return policy, fmt.Errorf("WORKER_RETRY_JITTER: %w", err)
		}
		policy.Jitter = jitter
	}
	if value := os.Getenv("WORKER_RETRY_MAX_ATTEMPTS"); value != "" {
		maxAttempts, err := strconv.ParseInt(value, 10, 16)
		if err != nil {
			return policy, fmt.Errorf("WORKER_RETRY_MAX_ATTEMPTS: %w", err)
		}
		policy.MaxAttempts = int16(maxAttempts)
	}
	if err := policy.Validate(); err != nil {
		return policy, err
	}
	return policy, nil
}
