// Command experiment-worker runs Atlas's real lease pool with deterministic
// experiment-only handlers. It is built separately from the production worker.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/G1lollipop/atlas/internal/config"
	"github.com/G1lollipop/atlas/internal/logger"
	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
	"github.com/G1lollipop/atlas/internal/worker"
	"github.com/jackc/pgx/v5"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "experiment worker:", err)
		os.Exit(1)
	}
}

func run() error {
	log := logger.New("experiment-worker")
	cfg, err := config.Load()
	if err != nil {
		log.Error("load config", "error", err)
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("connect to database", "error", err)
		return err
	}
	defer st.Close()
	migrations := os.Getenv("EXPERIMENT_MIGRATIONS_DIR")
	if migrations == "" {
		migrations = "migrations"
	}
	if err := store.RunMigrations(ctx, st.Pool(), migrations); err != nil {
		log.Error("run migrations", "error", err)
		return err
	}
	if cfg.Concurrency < 1 {
		return fmt.Errorf("worker concurrency must be positive")
	}
	pool := worker.NewPool(st, cfg.WorkerID, cfg.Concurrency, cfg.LeaseDuration, cfg.PollInterval, log)
	if grace := os.Getenv("WORKER_SHUTDOWN_GRACE_PERIOD"); grace != "" {
		d, err := time.ParseDuration(grace)
		if err != nil {
			log.Error("invalid shutdown grace period", "error", err)
			return err
		}
		if err := pool.SetShutdownGracePeriod(d); err != nil {
			log.Error("configure shutdown grace period", "error", err)
			return err
		}
	}
	pool.SetCapabilities(model.Worker{CPUCapacity: cfg.WorkerCPUCapacityMillis, MemoryCapacityMB: cfg.WorkerMemoryCapacityMB, GPUCount: cfg.WorkerGPUCount, GPUType: cfg.WorkerGPUType, GPUMemoryMB: cfg.WorkerGPUMemoryMB, Labels: cfg.WorkerLabels})
	pool.RegisterHandler("simulate_cpu", simulateHandler(false))
	pool.RegisterHandler("simulate_gpu", simulateHandler(true))
	pool.RegisterHandler("stale_completion_probe", staleCompletionProbe)
	pool.RegisterHandler("sideeffect_barrier", sideEffectBarrier(st))
	pool.RegisterHandler("idempotent_record", worker.NewIdempotentRecordHandler(st))
	pool.RegisterHandler("echo", worker.EchoHandler)
	err = pool.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("worker stopped", "error", err)
		return err
	}
	return nil
}

func simulateHandler(gpu bool) worker.Handler {
	return func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		if gpu && job.Payload["simulate_gpu"] != true {
			return nil, fmt.Errorf("simulate_gpu handler requires explicit simulate_gpu=true payload")
		}
		duration, err := durationFrom(job.Payload, "duration_ms", 50*time.Millisecond)
		if err != nil {
			return nil, err
		}
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
		return map[string]any{"simulated_gpu": gpu, "duration_ms": duration.Milliseconds(), "attempt": run.Attempt}, nil
	}
}

func staleCompletionProbe(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
	key := "first_attempt_duration_ms"
	if run.Attempt > 1 {
		key = "later_attempt_duration_ms"
	}
	duration, err := durationFrom(job.Payload, key, 2*time.Second)
	if err != nil {
		return nil, err
	}
	if run.Attempt == 1 {
		time.Sleep(duration)
	} else {
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return map[string]any{"attempt": run.Attempt, "returned_after_ms": duration.Milliseconds()}, nil
}

func sideEffectBarrier(st *store.PostgresStore) worker.Handler {
	return func(ctx context.Context, job *model.Job, run *model.JobRun) (map[string]any, error) {
		if run == nil || run.ExecutionKey == "" {
			return nil, fmt.Errorf("run has no stable execution key")
		}
		barrier, ok := job.Payload["barrier_path"].(string)
		if !ok || barrier == "" {
			return nil, fmt.Errorf("barrier_path is required")
		}
		release, _ := job.Payload["release_path"].(string)
		payload, err := json.Marshal(job.Payload)
		if err != nil {
			return nil, err
		}
		result, executed, err := st.RunOnce(ctx, run.ExecutionKey, func(ctx context.Context, tx pgx.Tx) (map[string]any, error) {
			if _, err := tx.Exec(ctx, `INSERT INTO idempotent_handler_effects(execution_key,payload) VALUES($1,$2)`, run.ExecutionKey, payload); err != nil {
				return nil, err
			}
			return map[string]any{"execution_key": run.ExecutionKey, "effect_persisted": true}, nil
		})
		if err != nil {
			return nil, err
		}
		if executed {
			if err := os.MkdirAll(filepath.Dir(barrier), 0755); err != nil {
				return nil, err
			}
			if err := os.WriteFile(barrier, []byte(run.ExecutionKey), 0600); err != nil {
				return nil, err
			}
			if err := waitForRelease(ctx, release); err != nil {
				return nil, err
			}
		}
		return result, nil
	}
}

func waitForRelease(ctx context.Context, path string) error {
	if path == "" {
		return fmt.Errorf("release_path is required")
	}
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func durationFrom(payload map[string]any, key string, fallback time.Duration) (time.Duration, error) {
	v, ok := payload[key]
	if !ok {
		return fallback, nil
	}
	var ms int64
	switch n := v.(type) {
	case float64:
		ms = int64(n)
	case int64:
		ms = n
	case int:
		ms = int64(n)
	case json.Number:
		x, e := n.Int64()
		if e != nil {
			return 0, e
		}
		ms = x
	case string:
		x, e := strconv.ParseInt(n, 10, 64)
		if e != nil {
			return 0, e
		}
		ms = x
	default:
		return 0, fmt.Errorf("%s must be integer milliseconds", key)
	}
	if ms < 0 {
		return 0, fmt.Errorf("%s must not be negative", key)
	}
	return time.Duration(ms) * time.Millisecond, nil
}
