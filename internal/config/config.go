// Package config loads process configuration from the environment. All services
// (api, scheduler, worker) share this loader so deployment (docker-compose/k8s) only
// has one set of env vars to reason about. MAX_QUEUE_DEPTH, PER_QUEUE_LIMIT, and
// PER_TENANT_LIMIT must match on every API and scheduler replica sharing a database.
package config

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL   string
	HTTPAddr      string
	JWTSecret     string
	LeaseDuration time.Duration
	PollInterval  time.Duration
	WorkerShutdownGracePeriod time.Duration
	WorkerID      string
	// Worker resource capacities are advertised at registration and on each
	// heartbeat. CPU is measured in millicores; memory and GPU memory are MB.
	WorkerCPUCapacityMillis int32
	WorkerMemoryCapacityMB  int32
	WorkerGPUCount          int32
	WorkerGPUType           string
	WorkerGPUMemoryMB       int32
	WorkerLabels            map[string]string
	MetricsAddr             string
	RateLimitRPS            float64
	RateLimitBurst          int
	MaxQueueDepth           int
	PerQueueLimit           int
	PerTenantLimit          int
	Concurrency             int
	OTLPEndpoint            string
	RedisAddr               string
	ReplicaURL              string
}

func Load() (Config, error) {
	cfg := Config{
		DatabaseURL:  getEnv("DATABASE_URL", "postgres://atlas:atlas@localhost:5432/atlas?sslmode=disable"),
		HTTPAddr:     getEnv("HTTP_ADDR", ":8080"),
		JWTSecret:    getEnv("JWT_SECRET", ""),
		MetricsAddr:  getEnv("METRICS_ADDR", ":9090"),
		WorkerID:     getEnv("WORKER_ID", ""),
		OTLPEndpoint: getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
		RedisAddr:    getEnv("REDIS_ADDR", ""),
		ReplicaURL:   getEnv("REPLICA_DATABASE_URL", ""),
	}

	var err error
	if cfg.LeaseDuration, err = getEnvDuration("LEASE_DURATION", 30*time.Second); err != nil {
		return cfg, err
	}
	if cfg.PollInterval, err = getEnvDuration("POLL_INTERVAL", 1*time.Second); err != nil {
		return cfg, err
	}
	if cfg.WorkerShutdownGracePeriod, err = getEnvDuration("WORKER_SHUTDOWN_GRACE_PERIOD", 20*time.Second); err != nil {
		return cfg, fmt.Errorf("WORKER_SHUTDOWN_GRACE_PERIOD: %w", err)
	}
	if cfg.WorkerShutdownGracePeriod < 0 {
		return cfg, fmt.Errorf("WORKER_SHUTDOWN_GRACE_PERIOD must not be negative")
	}
	if cfg.RateLimitRPS, err = getEnvFloat("RATE_LIMIT_RPS", 20); err != nil {
		return cfg, err
	}
	if cfg.RateLimitBurst, err = getEnvInt("RATE_LIMIT_BURST", 40); err != nil {
		return cfg, err
	}
	if cfg.MaxQueueDepth, err = getEnvInt("MAX_QUEUE_DEPTH", 10000); err != nil {
		return cfg, err
	}
	if cfg.PerQueueLimit, err = getEnvInt("PER_QUEUE_LIMIT", 2500); err != nil {
		return cfg, err
	}
	if cfg.PerTenantLimit, err = getEnvInt("PER_TENANT_LIMIT", 1000); err != nil {
		return cfg, err
	}
	if cfg.MaxQueueDepth < 0 || cfg.PerQueueLimit < 0 || cfg.PerTenantLimit < 0 {
		return cfg, fmt.Errorf("MAX_QUEUE_DEPTH, PER_QUEUE_LIMIT, and PER_TENANT_LIMIT must not be negative")
	}
	if math.IsNaN(cfg.RateLimitRPS) || math.IsInf(cfg.RateLimitRPS, 0) || cfg.RateLimitRPS <= 0 {
		return cfg, fmt.Errorf("RATE_LIMIT_RPS must be finite and greater than zero")
	}
	if cfg.RateLimitBurst <= 0 {
		return cfg, fmt.Errorf("RATE_LIMIT_BURST must be greater than zero")
	}
	if cfg.Concurrency, err = getEnvInt("WORKER_CONCURRENCY", 4); err != nil {
		return cfg, err
	}
	if cfg.WorkerCPUCapacityMillis, err = getEnvInt32("WORKER_CPU_CAPACITY_MILLIS", 4000); err != nil {
		return cfg, err
	}
	if cfg.WorkerMemoryCapacityMB, err = getEnvInt32("WORKER_MEMORY_CAPACITY_MB", 8192); err != nil {
		return cfg, err
	}
	if cfg.WorkerGPUCount, err = getEnvInt32("WORKER_GPU_COUNT", 0); err != nil {
		return cfg, err
	}
	if cfg.WorkerGPUMemoryMB, err = getEnvInt32("WORKER_GPU_MEMORY_MB", 0); err != nil {
		return cfg, err
	}
	cfg.WorkerGPUType = os.Getenv("WORKER_GPU_TYPE")
	if cfg.WorkerLabels, err = getEnvLabels("WORKER_LABELS"); err != nil {
		return cfg, err
	}
	if err := validateWorkerCapabilities(cfg); err != nil {
		return cfg, err
	}

	if cfg.WorkerID == "" {
		host, _ := os.Hostname()
		cfg.WorkerID = fmt.Sprintf("%s-%d", host, os.Getpid())
	}

	return cfg, nil
}

func validateWorkerCapabilities(cfg Config) error {
	if cfg.WorkerCPUCapacityMillis < 0 || cfg.WorkerMemoryCapacityMB < 0 || cfg.WorkerGPUCount < 0 || cfg.WorkerGPUMemoryMB < 0 {
		return fmt.Errorf("worker resource capacities must not be negative")
	}
	if cfg.WorkerGPUCount == 0 {
		if cfg.WorkerGPUType != "" || cfg.WorkerGPUMemoryMB != 0 {
			return fmt.Errorf("WORKER_GPU_TYPE and WORKER_GPU_MEMORY_MB require WORKER_GPU_COUNT greater than zero")
		}
		return nil
	}
	if cfg.WorkerGPUType == "" {
		return fmt.Errorf("WORKER_GPU_TYPE is required when WORKER_GPU_COUNT is greater than zero")
	}
	if cfg.WorkerGPUMemoryMB == 0 {
		return fmt.Errorf("WORKER_GPU_MEMORY_MB must be greater than zero when WORKER_GPU_COUNT is greater than zero")
	}
	return nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	return time.ParseDuration(v)
}

func getEnvFloat(key string, def float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	return strconv.ParseFloat(v, 64)
}

func getEnvInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	return strconv.Atoi(v)
}

func getEnvInt32(key string, def int32) (int32, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s must be a 32-bit integer: %w", key, err)
	}
	return int32(n), nil
}

func getEnvLabels(key string) (map[string]string, error) {
	v := os.Getenv(key)
	if v == "" {
		return map[string]string{}, nil
	}
	labels := make(map[string]string)
	if err := json.Unmarshal([]byte(v), &labels); err != nil {
		return nil, fmt.Errorf("%s must be a JSON object with string values: %w", key, err)
	}
	if labels == nil {
		return nil, fmt.Errorf("%s must be a JSON object, not null", key)
	}
	return labels, nil
}
