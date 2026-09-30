// Command experiments runs process-based Atlas load and recovery experiments.
// All reported measurements are collected from submitted jobs and PostgreSQL.
package main

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/G1lollipop/atlas/internal/api"
	"github.com/G1lollipop/atlas/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultJobs       = 100
	defaultWorkers    = "1,2,4,8,16"
	defaultPolicies   = "first-fit,best-fit"
	defaultRates      = "25,100,0"
	defaultDuration   = 50 * time.Millisecond
	defaultDeadline   = 10 * time.Minute
	defaultLease      = 3 * time.Second
	largeGPURequestMB = 16384
)

type options struct {
	mode, scenario, out, databaseURL, redisAddr, otelEndpoint                        string
	apiBin, schedulerBin, workerBin, migrationsDir, pgPIDFile                        string
	workerCounts, policies, submissionRates, workloadProfile                         string
	jobs, trials, submitConcurrency                                                  int
	seed                                                                             int64
	jobDuration, deadline, caseDeadline, leaseDuration, pollInterval, sampleInterval time.Duration
}

type report struct {
	FormatVersion  string            `json:"format_version"`
	RunID          string            `json:"run_id"`
	Mode           string            `json:"mode"`
	StartedAt      time.Time         `json:"started_at"`
	FinishedAt     time.Time         `json:"finished_at,omitempty"`
	Schema         string            `json:"isolated_schema"`
	Configuration  map[string]any    `json:"configuration"`
	Host           map[string]any    `json:"host_metadata"`
	Migrations     []string          `json:"migrations_applied"`
	Trials         []trialResult     `json:"trials"`
	Chaos          []chaosResult     `json:"chaos_scenarios"`
	Samples        []dbSample        `json:"database_samples"`
	SamplingErrors []string          `json:"sampling_errors,omitempty"`
	Errors         []string          `json:"errors,omitempty"`
	Cleanup        map[string]string `json:"cleanup,omitempty"`
}

type trialResult struct {
	ScenarioID                    string             `json:"scenario_id"`
	WorkloadProfile               string             `json:"workload_profile"`
	Policy                        string             `json:"policy"`
	WorkerCount                   int                `json:"worker_count"`
	Trial                         int                `json:"trial"`
	Seed                          int64              `json:"seed"`
	OfferedSubmissionRate         float64            `json:"offered_submission_rate_jobs_per_second"`
	JobsPlanned                   int                `json:"jobs_planned"`
	JobsAccepted                  int                `json:"jobs_accepted"`
	JobsSucceeded                 int                `json:"jobs_succeeded"`
	JobsDeadOrFailed              int                `json:"jobs_dead_or_failed"`
	UnfinishedAtDeadline          int                `json:"unfinished_at_deadline"`
	SubmissionDurationMS          float64            `json:"submission_duration_ms"`
	CompletionDurationMS          float64            `json:"completion_duration_ms"`
	TotalDurationMS               float64            `json:"total_batch_duration_ms"`
	ObservedSubmissionRate        float64            `json:"observed_submission_rate_jobs_per_second"`
	ExecutionThroughput           float64            `json:"whole_batch_execution_throughput_jobs_per_second"`
	P95SchedulerLatencyMS         *float64           `json:"p95_acceptance_to_assignment_ms,omitempty"`
	P95AcceptanceToStartMS        *float64           `json:"p95_acceptance_to_start_ms,omitempty"`
	P50ReadyQueueWaitMS           *float64           `json:"p50_ready_queue_wait_ms,omitempty"`
	P95ReadyQueueWaitMS           *float64           `json:"p95_ready_queue_wait_ms,omitempty"`
	MaxReadyQueueWaitMS           *float64           `json:"max_ready_queue_wait_ms,omitempty"`
	P95ReadyWaitByWorkload        map[string]float64 `json:"p95_ready_wait_ms_by_workload,omitempty"`
	UnfinishedByWorkload          map[string]int     `json:"unfinished_jobs_by_workload,omitempty"`
	MaxOldestReadyAgeMS           *float64           `json:"max_sampled_oldest_ready_age_ms,omitempty"`
	MeanDBCPUCoreEquivalents      *float64           `json:"mean_db_cpu_core_equivalents,omitempty"`
	MaxDBCPUCoreEquivalents       *float64           `json:"max_db_cpu_core_equivalents,omitempty"`
	MeanSampledLockWaitFraction   *float64           `json:"mean_sampled_lock_wait_fraction,omitempty"`
	MeanGPUReservedVRAMMB         *float64           `json:"mean_gpu_reserved_vram_mb,omitempty"`
	MeanGPUCapacityVRAMMB         *float64           `json:"mean_gpu_capacity_vram_mb,omitempty"`
	MeanGPUReservationRatio       *float64           `json:"mean_gpu_vram_reservation_ratio,omitempty"`
	MeanFreeGPUVRAMMB             *float64           `json:"mean_free_gpu_vram_mb,omitempty"`
	MeanLargeJobSlackMB           *float64           `json:"mean_subthreshold_available_vram_slack_for_16gb_job_mb,omitempty"`
	MeanWholeDeviceStrandedVRAMMB *float64           `json:"mean_whole_device_stranded_vram_mb,omitempty"`
	SmallGPUJobsAccepted          int                `json:"small_gpu_jobs_accepted"`
	SmallGPUJobsPlacedOnLargeGPU  int                `json:"small_gpu_jobs_placed_on_large_gpu"`
	Jobs                          []jobResult        `json:"jobs,omitempty"`
	Error                         string             `json:"error,omitempty"`
}

type jobResult struct {
	JobID                     string     `json:"job_id"`
	Workload                  string     `json:"workload"`
	Accepted                  bool       `json:"accepted"`
	SubmissionStatus          int        `json:"submission_http_status,omitempty"`
	SubmissionError           string     `json:"submission_error,omitempty"`
	SubmitStartedAt           *time.Time `json:"submit_started_at,omitempty"`
	SubmittedAt               *time.Time `json:"submitted_at,omitempty"`
	SubmissionDurationMS      *float64   `json:"submission_duration_ms,omitempty"`
	RunID                     string     `json:"run_id,omitempty"`
	ExecutionKey              string     `json:"execution_key,omitempty"`
	Status                    string     `json:"status,omitempty"`
	Attempt                   int16      `json:"attempt,omitempty"`
	AcceptedAtDB              *time.Time `json:"accepted_at_db,omitempty"`
	ScheduledAt               *time.Time `json:"scheduled_at,omitempty"`
	AssignedAt                *time.Time `json:"assigned_at,omitempty"`
	StartedAt                 *time.Time `json:"started_at,omitempty"`
	FinishedAt                *time.Time `json:"finished_at,omitempty"`
	AcceptanceToAssignmentMS  *float64   `json:"acceptance_to_assignment_ms,omitempty"`
	AcceptanceToStartMS       *float64   `json:"acceptance_to_start_ms,omitempty"`
	ReadyQueueWaitMS          *float64   `json:"ready_queue_wait_ms,omitempty"`
	ExecutionDurationMS       *float64   `json:"execution_duration_ms,omitempty"`
	AssignedWorkerID          string     `json:"assigned_worker_id,omitempty"`
	AssignedWorkerGPUMemoryMB int64      `json:"assigned_worker_gpu_memory_mb,omitempty"`
	Error                     string     `json:"error,omitempty"`
}

type chaosResult struct {
	ScenarioID string      `json:"scenario_id"`
	StartedAt  time.Time   `json:"started_at"`
	FinishedAt time.Time   `json:"finished_at,omitempty"`
	Passed     bool        `json:"passed"`
	Assertions []assertion `json:"assertions"`
	Error      string      `json:"error,omitempty"`
}

type assertion struct {
	Name     string `json:"name"`
	Passed   bool   `json:"passed"`
	Observed string `json:"observed"`
	Expected string `json:"expected"`
}
type connCounts struct {
	Active int `json:"active"`
	Idle   int `json:"idle"`
	Other  int `json:"other"`
}
type dbSample struct {
	At                        time.Time             `json:"at"`
	ScenarioID                string                `json:"scenario_id"`
	DBCPUCoreEquivalents      *float64              `json:"db_process_cpu_core_equivalents,omitempty"`
	ProcCPUError              string                `json:"proc_cpu_error,omitempty"`
	ActiveAppConnections      int                   `json:"active_app_connections"`
	IdleAppConnections        int                   `json:"idle_app_connections"`
	LockWaiters               int                   `json:"lock_waiters"`
	SampledLockWaitFraction   float64               `json:"sampled_lock_wait_fraction"`
	OldestReadyAgeMS          *float64              `json:"oldest_ready_age_ms,omitempty"`
	GPUReservedVRAMMB         int64                 `json:"gpu_reserved_vram_mb"`
	GPUCapacityVRAMMB         int64                 `json:"gpu_capacity_vram_mb"`
	GPUFreeVRAMMB             int64                 `json:"gpu_free_vram_mb"`
	GPUAvailableDevices       int64                 `json:"gpu_available_devices"`
	GPUReservedDevices        int64                 `json:"gpu_reserved_devices"`
	GPUReservationRatio       *float64              `json:"gpu_vram_reservation_ratio,omitempty"`
	LargeJobSlackMB           int64                 `json:"subthreshold_available_vram_slack_for_16gb_job_mb"`
	WholeDeviceStrandedVRAMMB int64                 `json:"whole_device_stranded_vram_mb"`
	ConnectionsByApp          map[string]connCounts `json:"connections_by_application"`
}
type apiSubmission struct {
	JobID, Workload, Name string
	Payload               map[string]any
	GPUCount              int32
	GPUMemMB              int32
}
type child struct {
	name, appName string
	cmd           *exec.Cmd
	logFile       *os.File
	done          chan error
	mu            sync.Mutex
	exited        bool
	waitErr       error
}
type workerSpec struct {
	id                         string
	cpu, mem, gpuCount, gpuMem int
	gpuType                    string
}
type leaderRow struct {
	app string
	pid int
}
type outcome struct {
	index          int
	id             string
	err            error
	code           int
	started, ended time.Time
}

type runner struct {
	opts                               options
	report                             *report
	baseURL                            *url.URL
	adminPool, dataPool                *pgxpool.Pool
	schema, apiAddr, secret, redisAddr string
	client                             *http.Client
	apiProcess                         *child
	schedulers, workers, processes     []*child
	pgProxy, redisProxy                *tcpProxy
	labelMu                            sync.RWMutex
	label                              string
	samplerStop                        chan struct{}
	samplerDone                        chan struct{}
	samplerStarted                     bool
	sampleMu                           sync.RWMutex
	procSampler                        *procCPUSampler
	closeOnce                          sync.Once
	finishErr                          error
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	o := parseOptions()
	ctx, cancel := context.WithTimeout(context.Background(), o.deadline)
	defer cancel()
	r, err := newRunner(ctx, o)
	if err != nil {
		if r != nil {
			r.report.Errors = append(r.report.Errors, err.Error())
			r.finish()
		}
		return fmt.Errorf("experiment setup failed: %w", err)
	}
	if o.mode == "bench" {
		err = r.runBench(ctx)
	} else {
		err = r.runChaos(ctx)
	}
	if err != nil {
		r.report.Errors = append(r.report.Errors, err.Error())
	}
	r.finish()
	if r.finishErr != nil {
		err = errors.Join(err, r.finishErr)
	}
	if err != nil {
		return fmt.Errorf("experiment ended with errors: %w (partial observations: %s)", err, filepath.Join(o.out, "report.json"))
	}
	fmt.Println("experiment observations:", filepath.Join(o.out, "report.json"))
	return nil
}

func parseOptions() options {
	var o options
	flag.StringVar(&o.mode, "mode", "bench", "mode: bench or chaos")
	flag.StringVar(&o.scenario, "scenario", "all", "chaos scenario: all, side-effect-crash, worker-loss, scheduler-failover, timeout, postgres-outage, redis-outage, graceful-shutdown")
	flag.StringVar(&o.out, "out", "artifacts/experiments/latest", "output directory")
	flag.StringVar(&o.databaseURL, "database-url", os.Getenv("DATABASE_URL"), "PostgreSQL URL")
	flag.StringVar(&o.redisAddr, "redis-addr", os.Getenv("REDIS_ADDR"), "optional Redis host:port")
	flag.StringVar(&o.otelEndpoint, "otel-endpoint", "", "optional OTLP/HTTP host:port")
	flag.StringVar(&o.apiBin, "api-bin", "", "built API executable")
	flag.StringVar(&o.schedulerBin, "scheduler-bin", "", "built scheduler executable")
	flag.StringVar(&o.workerBin, "worker-bin", "", "built experiment worker helper executable")
	flag.StringVar(&o.migrationsDir, "migrations-dir", "migrations", "SQL migration directory")
	flag.StringVar(&o.pgPIDFile, "pg-pid-file", "", "optional postmaster PID file for /proc CPU sampling")
	flag.StringVar(&o.workerCounts, "worker-counts", defaultWorkers, "worker count sweep")
	flag.StringVar(&o.policies, "policies", defaultPolicies, "scheduler policies to compare")
	flag.StringVar(&o.submissionRates, "submission-rates", defaultRates, "offered submissions/sec; 0 is unpaced")
	flag.StringVar(&o.workloadProfile, "workload-profile", "mixed", "load fixture: mixed or cpu (CPU-only scaling axis)")
	flag.IntVar(&o.jobs, "jobs", defaultJobs, "jobs per measured batch")
	flag.IntVar(&o.trials, "trials", 3, "repetitions per configuration")
	flag.IntVar(&o.submitConcurrency, "submit-concurrency", 16, "concurrent HTTP submitters")
	flag.Int64Var(&o.seed, "seed", 608, "deterministic workload seed")
	flag.DurationVar(&o.jobDuration, "job-duration", defaultDuration, "simulated handler duration")
	flag.DurationVar(&o.deadline, "deadline", defaultDeadline, "overall deadline")
	flag.DurationVar(&o.caseDeadline, "case-deadline", 90*time.Second, "per batch/recovery deadline")
	flag.DurationVar(&o.leaseDuration, "lease-duration", defaultLease, "worker lease duration")
	flag.DurationVar(&o.pollInterval, "poll-interval", 100*time.Millisecond, "scheduler polling interval")
	flag.DurationVar(&o.sampleInterval, "sample-interval", 250*time.Millisecond, "database/process sampling interval")
	flag.Parse()
	return o
}

func validateOptions(o options) error {
	if o.mode != "bench" && o.mode != "chaos" {
		return fmt.Errorf("--mode must be bench or chaos")
	}
	if o.databaseURL == "" {
		return fmt.Errorf("--database-url is required")
	}
	for label, path := range map[string]string{"api-bin": o.apiBin, "scheduler-bin": o.schedulerBin, "worker-bin": o.workerBin} {
		if path == "" {
			return fmt.Errorf("--%s is required", label)
		}
		info, err := os.Stat(path)
		if err != nil || info.IsDir() {
			return fmt.Errorf("--%s is not a file: %s", label, path)
		}
	}
	if o.jobs < 10 || o.trials < 1 || o.submitConcurrency < 1 {
		return fmt.Errorf("--jobs must be at least 10; --trials and --submit-concurrency must be positive")
	}
	if o.jobDuration <= 0 || o.deadline <= 0 || o.caseDeadline <= 0 || o.leaseDuration <= 0 || o.pollInterval <= 0 || o.sampleInterval <= 0 {
		return fmt.Errorf("duration flags must be positive")
	}
	if o.mode == "bench" {
		if _, err := parseInts(o.workerCounts); err != nil {
			return err
		}
		if _, err := parsePolicies(o.policies); err != nil {
			return err
		}
		if _, err := parseRates(o.submissionRates); err != nil {
			return err
		}
	}
	if o.workloadProfile != "cpu" && o.workloadProfile != "mixed" {
		return fmt.Errorf("--workload-profile must be cpu or mixed")
	}
	valid := map[string]bool{"all": true, "side-effect-crash": true, "worker-loss": true, "scheduler-failover": true, "timeout": true, "postgres-outage": true, "redis-outage": true, "graceful-shutdown": true}
	if o.mode == "chaos" && !valid[o.scenario] {
		return fmt.Errorf("unsupported --scenario %q", o.scenario)
	}
	if o.mode == "chaos" && (o.scenario == "all" || o.scenario == "redis-outage") && o.redisAddr == "" {
		return fmt.Errorf("--redis-addr is required for the Redis outage scenario")
	}
	if o.otelEndpoint != "" {
		if _, _, err := net.SplitHostPort(o.otelEndpoint); err != nil {
			return fmt.Errorf("--otel-endpoint must be host:port")
		}
	}
	return nil
}

func newRunner(ctx context.Context, o options) (*runner, error) {
	if err := validateOptions(o); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(o.out, "logs"), 0755); err != nil {
		return nil, err
	}
	u, err := url.Parse(o.databaseURL)
	if err != nil || u.Scheme == "" || u.Hostname() == "" {
		return nil, fmt.Errorf("--database-url must be a PostgreSQL URL with TCP host")
	}
	cfg, err := pgxpool.ParseConfig(o.databaseURL)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = "atlasexp-controller"
	cfg.MaxConns = 2
	admin, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := admin.Ping(ctx); err != nil {
		admin.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	random := make([]byte, 4)
	if _, err := cryptorand.Read(random); err != nil {
		admin.Close()
		return nil, err
	}
	runID := time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(random)
	schema := experimentSchemaName(runID)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoteIdentifier(schema)); err != nil {
		admin.Close()
		return nil, fmt.Errorf("create isolated schema: %w", err)
	}
	dataCfg, err := pgxpool.ParseConfig(setDBURL(u, schema, "atlasexp-controller", ""))
	if err != nil {
		return nil, errors.Join(err, cleanupSetupSchema(nil, admin, schema))
	}
	dataCfg.ConnConfig.RuntimeParams["application_name"] = "atlasexp-controller"
	dataCfg.MaxConns = 2
	data, err := pgxpool.NewWithConfig(ctx, dataCfg)
	if err != nil {
		return nil, errors.Join(err, cleanupSetupSchema(nil, admin, schema))
	}
	if err := data.Ping(ctx); err != nil {
		return nil, errors.Join(err, cleanupSetupSchema(data, admin, schema))
	}
	var selectedSchema string
	if err := data.QueryRow(ctx, `SELECT current_schema()`).Scan(&selectedSchema); err != nil {
		return nil, errors.Join(fmt.Errorf("read isolated current schema: %w", err), cleanupSetupSchema(data, admin, schema))
	}
	if err := validateExperimentSchemaOwnership(selectedSchema, schema); err != nil {
		return nil, errors.Join(err, cleanupSetupSchema(data, admin, schema))
	}
	if err := store.RunMigrations(ctx, data, o.migrationsDir); err != nil {
		return nil, errors.Join(fmt.Errorf("run isolated migrations: %w", err), cleanupSetupSchema(data, admin, schema))
	}
	versions, err := migrationVersions(ctx, data)
	if err != nil {
		return nil, errors.Join(err, cleanupSetupSchema(data, admin, schema))
	}
	key := make([]byte, 32)
	if _, err := cryptorand.Read(key); err != nil {
		return nil, errors.Join(err, cleanupSetupSchema(data, admin, schema))
	}
	r := &runner{opts: o, baseURL: u, adminPool: admin, dataPool: data, schema: schema, secret: hex.EncodeToString(key), client: &http.Client{Timeout: 8 * time.Second}, redisAddr: o.redisAddr, label: "startup", samplerStop: make(chan struct{}), samplerDone: make(chan struct{})}
	r.report = &report{FormatVersion: "1", RunID: runID, Mode: o.mode, StartedAt: time.Now().UTC(), Schema: schema, Migrations: versions, Cleanup: map[string]string{},
		Configuration: map[string]any{
			"jobs_per_batch": o.jobs, "trials": o.trials, "worker_counts": o.workerCounts, "policies": o.policies,
			"submission_rates_jobs_per_second": o.submissionRates, "submit_concurrency": o.submitConcurrency,
			"workload_profile": o.workloadProfile, "job_duration": o.jobDuration.String(), "deadline": o.deadline.String(),
			"case_deadline": o.caseDeadline.String(), "lease_duration": o.leaseDuration.String(),
			"poll_interval": o.pollInterval.String(), "sample_interval": o.sampleInterval.String(), "seed": o.seed,
			"database_url_host": u.Host, "redis_addr_configured": o.redisAddr != "", "postgres_pid_file": o.pgPIDFile,
			"service_pool_max_conns": 4, "controller_pool_max_conns": 2, "migrations_dir": o.migrationsDir,
			"history_reset_between_trials": true,
			"policy_order":                 strings.Split(o.policies, ","),
			"policy_order_bias_boundary":   "trials run in configured policy order; reset removes history bias but machine time/load drift may remain",
			"binaries":                     map[string]any{"api": binaryMetadata(o.apiBin), "scheduler": binaryMetadata(o.schedulerBin), "experiment_worker": binaryMetadata(o.workerBin)},
			"worker_id_order":              "CPU-only IDs first, then large-GPU IDs, then small-GPU IDs; stable lexicographic scheduler order",
			"gpu_metric_boundary":          "simulated GPU reservations only; free devices exclude every device with an active reservation; stranded VRAM and subthreshold slack are memory proxies, not physical utilization",
			"throughput_boundary":          "whole-batch succeeded count divided by wall time from first submission through last terminal run",
		},
		Host: map[string]any{"go_version": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "logical_cpu_count": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0)}}
	if hostname, e := os.Hostname(); e == nil {
		r.report.Host["hostname"] = hostname
	}
	if o.pgPIDFile != "" {
		r.procSampler = newProcCPUSampler(o.pgPIDFile)
		r.report.Configuration["postgres_proc_cpu_sampling_method"] = "per-process utime/stime deltas over postmaster descendants; a child exiting between samples may be undercounted"
		if r.procSampler.initErr == nil {
			r.report.Configuration["postgres_proc_clock_ticks_per_second"] = r.procSampler.ticksPerSecond
		} else {
			r.report.Configuration["postgres_proc_cpu_setup_error"] = r.procSampler.initErr.Error()
		}
	}
	if o.mode == "chaos" {
		target := net.JoinHostPort(u.Hostname(), defaultPort(u.Port(), "5432"))
		r.pgProxy, err = newTCPProxy(target)
		if err != nil {
			r.finish()
			return nil, fmt.Errorf("start scoped PostgreSQL proxy: %w", err)
		}
		if o.redisAddr != "" {
			r.redisProxy, err = newTCPProxy(o.redisAddr)
			if err != nil {
				r.finish()
				return nil, fmt.Errorf("start scoped Redis proxy: %w", err)
			}
			r.redisAddr = r.redisProxy.Addr()
		}
	}
	if err := r.startAPI(ctx); err != nil {
		r.finish()
		return nil, err
	}
	r.startSampler()
	return r, nil
}

func setDBURL(base *url.URL, schema, app, host string) string {
	u := *base
	q := u.Query()
	q.Set("search_path", schema)
	q.Set("application_name", app)
	q.Set("pool_max_conns", "4")
	if host != "" {
		u.Host = host
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func cleanupSetupSchema(data, admin *pgxpool.Pool, schema string) error {
	if data != nil {
		data.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+quoteIdentifier(schema)+" CASCADE")
	admin.Close()
	if err != nil {
		return fmt.Errorf("cleanup partially initialized schema %s: %w", schema, err)
	}
	return nil
}

func (r *runner) processDBURL(app string) string {
	host := ""
	if r.pgProxy != nil {
		host = r.pgProxy.Addr()
	}
	return setDBURL(r.baseURL, r.schema, app, host)
}

func (r *runner) startAPI(ctx context.Context) error {
	addr, err := freeAddress()
	if err != nil {
		return err
	}
	r.apiAddr = addr
	metrics, err := freeAddress()
	if err != nil {
		return err
	}
	app := "atlasexp-api-" + shortToken()
	env := map[string]string{"DATABASE_URL": r.processDBURL(app), "JWT_SECRET": r.secret, "HTTP_ADDR": addr, "METRICS_ADDR": metrics, "RATE_LIMIT_RPS": "100000", "RATE_LIMIT_BURST": "1000000", "MAX_QUEUE_DEPTH": "20000", "PER_QUEUE_LIMIT": "20000", "PER_TENANT_LIMIT": "20000"}
	if r.redisAddr != "" {
		env["REDIS_ADDR"] = r.redisAddr
	}
	if r.opts.otelEndpoint != "" {
		env["OTEL_EXPORTER_OTLP_ENDPOINT"] = r.opts.otelEndpoint
	}
	c, err := r.startProcess("api", r.opts.apiBin, app, env)
	if err != nil {
		return err
	}
	r.apiProcess = c
	url := "http://" + addr + "/healthz"
	ready := time.NewTimer(15 * time.Second)
	defer ready.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, e := r.client.Do(req)
		if e == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ready.C:
			return fmt.Errorf("API did not become ready at %s", url)
		case <-tick.C:
		}
	}
}

func (r *runner) startProcess(name, binary, app string, vars map[string]string) (*child, error) {
	path := filepath.Join(r.opts.out, "logs", safeFileName(name)+"-"+shortToken()+".log")
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(binary)
	cmd.Env = mergeEnvironment(vars)
	cmd.Stdout = f
	cmd.Stderr = f
	c := &child{name: name, appName: app, cmd: cmd, logFile: f, done: make(chan error, 1)}
	if err := cmd.Start(); err != nil {
		closeErr := f.Close()
		return nil, errors.Join(fmt.Errorf("start %s: %w", name, err), closeErr)
	}
	go func() {
		e := cmd.Wait()
		if closeErr := f.Close(); closeErr != nil {
			e = errors.Join(e, fmt.Errorf("close %s log: %w", name, closeErr))
		}
		c.mu.Lock()
		c.exited = true
		c.waitErr = e
		c.mu.Unlock()
		c.done <- e
	}()
	r.processes = append(r.processes, c)
	return c, nil
}

func (r *runner) startSampler() {
	r.samplerStarted = true
	go func() {
		defer close(r.samplerDone)
		ticker := time.NewTicker(r.opts.sampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.labelMu.RLock()
				label := r.label
				r.labelMu.RUnlock()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				s, err := r.takeSample(ctx)
				cancel()
				r.sampleMu.Lock()
				if err == nil {
					s.ScenarioID = label
					r.report.Samples = append(r.report.Samples, s)
				} else {
					r.report.SamplingErrors = append(r.report.SamplingErrors, label+" at "+time.Now().UTC().Format(time.RFC3339Nano)+": "+err.Error())
				}
				r.sampleMu.Unlock()
			case <-r.samplerStop:
				return
			}
		}
	}()
}
func (r *runner) setLabel(label string) { r.labelMu.Lock(); r.label = label; r.labelMu.Unlock() }

func (r *runner) takeSample(ctx context.Context) (dbSample, error) {
	s := dbSample{At: time.Now().UTC(), ConnectionsByApp: map[string]connCounts{}}
	rows, err := r.dataPool.Query(ctx, `SELECT application_name,count(*) FILTER(WHERE state='active')::int,count(*) FILTER(WHERE state='idle')::int,count(*) FILTER(WHERE state IS DISTINCT FROM 'active' AND state IS DISTINCT FROM 'idle')::int,count(*) FILTER(WHERE wait_event_type='Lock')::int FROM pg_stat_activity WHERE datname=current_database() AND application_name LIKE 'atlasexp-%' AND application_name<>'atlasexp-controller' GROUP BY application_name`)
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var app string
		var c connCounts
		var wait int
		if err := rows.Scan(&app, &c.Active, &c.Idle, &c.Other, &wait); err != nil {
			rows.Close()
			return s, err
		}
		s.ConnectionsByApp[app] = c
		s.ActiveAppConnections += c.Active
		s.IdleAppConnections += c.Idle
		s.LockWaiters += wait
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return s, err
	}
	rows.Close()
	if s.ActiveAppConnections > 0 {
		s.SampledLockWaitFraction = float64(s.LockWaiters) / float64(s.ActiveAppConnections)
	}
	var oldest *float64
	if err := r.dataPool.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM(now()-min(scheduled_at)))*1000.0 FROM job_runs WHERE status IN('queued','scheduled','assigned')`).Scan(&oldest); err != nil {
		return s, err
	}
	s.OldestReadyAgeMS = oldest
	gpu, err := r.dataPool.Query(ctx, `SELECT w.gpu_count::bigint,w.gpu_memory_mb::bigint,COALESCE(SUM(j.required_gpu_count),0)::bigint,COALESCE(SUM(j.required_gpu_count::bigint*j.required_gpu_memory_mb::bigint),0)::bigint FROM workers w LEFT JOIN job_runs r ON((r.status='assigned' AND r.assigned_worker_id=w.id) OR(r.status IN('leased','running') AND r.leased_by=w.id)) LEFT JOIN jobs j ON j.id=r.job_id WHERE w.status='alive' AND w.last_heartbeat_at>now()-INTERVAL '30 seconds' GROUP BY w.id,w.gpu_count,w.gpu_memory_mb`)
	if err != nil {
		return s, err
	}
	for gpu.Next() {
		var deviceCount, perDeviceMB, reservedDevices, reservedMB int64
		if err := gpu.Scan(&deviceCount, &perDeviceMB, &reservedDevices, &reservedMB); err != nil {
			gpu.Close()
			return s, err
		}
		if deviceCount <= 0 || perDeviceMB <= 0 {
			continue
		}
		reservedDevices = min(deviceCount, reservedDevices)
		availableDevices := max(int64(0), deviceCount-reservedDevices)
		capacity := deviceCount * perDeviceMB
		s.GPUCapacityVRAMMB += capacity
		s.GPUReservedVRAMMB += reservedMB
		s.GPUAvailableDevices += availableDevices
		s.GPUReservedDevices += reservedDevices
		s.GPUFreeVRAMMB += availableDevices * perDeviceMB
		s.WholeDeviceStrandedVRAMMB += max(int64(0), reservedDevices*perDeviceMB-reservedMB)
		if availableDevices > 0 && perDeviceMB < largeGPURequestMB {
			s.LargeJobSlackMB += availableDevices * perDeviceMB
		}
	}
	if err := gpu.Err(); err != nil {
		gpu.Close()
		return s, err
	}
	gpu.Close()
	if s.GPUCapacityVRAMMB > 0 {
		v := float64(s.GPUReservedVRAMMB) / float64(s.GPUCapacityVRAMMB)
		s.GPUReservationRatio = &v
	}
	if r.procSampler != nil {
		v, e := r.procSampler.Sample(s.At)
		if e != nil {
			s.ProcCPUError = e.Error()
		} else {
			s.DBCPUCoreEquivalents = &v
		}
	}
	return s, nil
}

func (r *runner) runBench(ctx context.Context) error {
	counts, _ := parseInts(r.opts.workerCounts)
	policies, _ := parsePolicies(r.opts.policies)
	rates, _ := parseRates(r.opts.submissionRates)
	var trialFailures []string
	for _, n := range counts {
		if err := r.assertNoActiveRuns(ctx); err != nil {
			return err
		}
		if err := r.stopWorkers(); err != nil {
			return err
		}
		if err := r.startFleet(n, r.opts.leaseDuration); err != nil {
			return err
		}
		if err := r.waitFleet(ctx, n); err != nil {
			return err
		}
		for _, policy := range policies {
			if err := r.stopSchedulers(); err != nil {
				return err
			}
			if err := r.startScheduler(policy); err != nil {
				return err
			}
			if err := r.waitLeader(ctx, 5*time.Second); err != nil {
				return err
			}
			for _, rate := range rates {
				for trial := 1; trial <= r.opts.trials; trial++ {
					if err := r.resetTrialHistory(ctx); err != nil {
						return err
					}
					tr := r.runTrial(ctx, policy, n, rate, trial)
					r.report.Trials = append(r.report.Trials, tr)
					if tr.Error != "" {
						failure := tr.ScenarioID + ": " + tr.Error
						trialFailures = append(trialFailures, failure)
						r.report.Errors = append(r.report.Errors, failure)
						if tr.UnfinishedAtDeadline > 0 {
							return fmt.Errorf("%s left %d unfinished runs; stopping to preserve isolation", tr.ScenarioID, tr.UnfinishedAtDeadline)
						}
					}
				}
			}
		}
	}
	if len(trialFailures) > 0 {
		return fmt.Errorf("one or more benchmark trials failed: %s", strings.Join(trialFailures, "; "))
	}
	return nil
}

func (r *runner) runTrial(ctx context.Context, policy string, workers int, rate float64, trial int) trialResult {
	seed := r.opts.seed + int64(trial-1)
	id := fmt.Sprintf("bench-%s-w%d-%s-r%s-t%d", r.opts.workloadProfile, workers, safeFileName(policy), rateLabel(rate), trial)
	tr := trialResult{ScenarioID: id, WorkloadProfile: r.opts.workloadProfile, Policy: policy, WorkerCount: workers, Trial: trial, Seed: seed, OfferedSubmissionRate: rate, JobsPlanned: r.opts.jobs}
	batch := workloadBatch(r.opts.jobs, seed, r.opts.jobDuration, r.opts.workloadProfile)
	tr.Jobs = make([]jobResult, len(batch))
	for i, j := range batch {
		tr.Jobs[i] = jobResult{JobID: j.JobID, Workload: j.Workload}
	}
	r.setLabel(id)
	started := time.Now()
	subStart := started
	accepted := r.submitBatch(ctx, batch, rate, tr.Jobs)
	subEnd := time.Now()
	tr.SubmissionDurationMS = float64(subEnd.Sub(subStart).Microseconds()) / 1000
	tr.JobsAccepted = len(accepted)
	tr.ObservedSubmissionRate = ratePerSecond(len(accepted), subEnd.Sub(subStart))
	completion := time.Now()
	waitCtx, cancel := context.WithTimeout(ctx, r.opts.caseDeadline)
	waitErr := r.waitTerminal(waitCtx, accepted)
	cancel()
	done := time.Now()
	tr.CompletionDurationMS = float64(done.Sub(completion).Microseconds()) / 1000
	tr.TotalDurationMS = float64(done.Sub(started).Microseconds()) / 1000
	readCtx, readCancel := context.WithTimeout(ctx, min(r.opts.caseDeadline, 5*time.Second))
	var readErr error
	tr.JobsSucceeded, tr.JobsDeadOrFailed, tr.UnfinishedAtDeadline, tr.Jobs, readErr = r.readJobResults(readCtx, tr.Jobs)
	readCancel()
	if readErr != nil {
		if tr.Error != "" {
			tr.Error += "; "
		}
		tr.Error += "read job results: " + readErr.Error()
	}
	tr.ExecutionThroughput = ratePerSecond(tr.JobsSucceeded, done.Sub(started))
	if waitErr != nil {
		tr.Error = waitErr.Error()
		tr.UnfinishedAtDeadline = countUnfinished(tr.Jobs)
	}
	if len(accepted) != len(batch) {
		x := fmt.Sprintf("API accepted %d/%d submissions", len(accepted), len(batch))
		if tr.Error != "" {
			tr.Error += "; "
		}
		tr.Error += x
	}
	if tr.JobsDeadOrFailed > 0 {
		if tr.Error != "" {
			tr.Error += "; "
		}
		tr.Error += fmt.Sprintf("%d runs ended failed/dead/canceled", tr.JobsDeadOrFailed)
	}
	waits, scheduledLatencies, endToStart := []float64{}, []float64{}, []float64{}
	perWorkload := map[string][]float64{}
	unfinishedBy := map[string]int{}
	for _, j := range tr.Jobs {
		if j.Workload == "gpu-small" && j.Accepted {
			tr.SmallGPUJobsAccepted++
			if j.AssignedWorkerGPUMemoryMB >= largeGPURequestMB {
				tr.SmallGPUJobsPlacedOnLargeGPU++
			}
		}
		if j.ReadyQueueWaitMS != nil {
			waits = append(waits, *j.ReadyQueueWaitMS)
			perWorkload[j.Workload] = append(perWorkload[j.Workload], *j.ReadyQueueWaitMS)
		}
		if j.AcceptanceToAssignmentMS != nil {
			scheduledLatencies = append(scheduledLatencies, *j.AcceptanceToAssignmentMS)
		}
		if j.AcceptanceToStartMS != nil {
			endToStart = append(endToStart, *j.AcceptanceToStartMS)
		}
		if !j.Accepted || (j.Status != "succeeded" && j.Status != "dead" && j.Status != "failed" && j.Status != "canceled") {
			unfinishedBy[j.Workload]++
		}
	}
	if len(waits) > 0 {
		sort.Float64s(waits)
		p50, p95, m := percentile(waits, .5), percentile(waits, .95), waits[len(waits)-1]
		tr.P50ReadyQueueWaitMS = &p50
		tr.P95ReadyQueueWaitMS = &p95
		tr.MaxReadyQueueWaitMS = &m
	}
	if len(scheduledLatencies) > 0 {
		sort.Float64s(scheduledLatencies)
		p := percentile(scheduledLatencies, .95)
		tr.P95SchedulerLatencyMS = &p
	}
	if len(endToStart) > 0 {
		sort.Float64s(endToStart)
		p := percentile(endToStart, .95)
		tr.P95AcceptanceToStartMS = &p
	}
	tr.P95ReadyWaitByWorkload = map[string]float64{}
	for workload, v := range perWorkload {
		sort.Float64s(v)
		tr.P95ReadyWaitByWorkload[workload] = percentile(v, .95)
	}
	tr.UnfinishedByWorkload = unfinishedBy
	r.setLabel("idle")
	r.summarizeSamples(&tr)
	return tr
}

func (r *runner) submitBatch(ctx context.Context, batch []apiSubmission, rate float64, results []jobResult) []string {
	jobs := make(chan int)
	out := make(chan outcome, len(batch))
	pacer := newPacer(rate)
	var wg sync.WaitGroup
	for w := 0; w < r.opts.submitConcurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if err := pacer.Wait(ctx); err != nil {
					out <- outcome{index: i, err: err}
					continue
				}
				j := batch[i]
				sentAt := time.Now().UTC()
				accel := ""
				if j.GPUCount > 0 {
					accel = "sim-gpu"
				}
				body, _ := json.Marshal(map[string]any{"name": j.Name, "payload": j.Payload, "workload_type": j.Workload, "required_cpu_millis": 1000, "required_memory_mb": 128, "required_gpu_count": j.GPUCount, "required_gpu_memory_mb": j.GPUMemMB, "required_accelerator": accel, "max_attempts": 2, "timeout_seconds": 30})
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+r.apiAddr+"/v1/jobs", strings.NewReader(string(body)))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+r.token())
				resp, err := r.client.Do(req)
				if err != nil {
					out <- outcome{index: i, err: err, started: sentAt, ended: time.Now().UTC()}
					continue
				}
				raw, e := io.ReadAll(io.LimitReader(resp.Body, 8192))
				_ = resp.Body.Close()
				if e != nil {
					out <- outcome{index: i, err: e, code: resp.StatusCode, started: sentAt, ended: time.Now().UTC()}
					continue
				}
				if resp.StatusCode != http.StatusCreated {
					out <- outcome{index: i, err: fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw))), code: resp.StatusCode, started: sentAt, ended: time.Now().UTC()}
					continue
				}
				var created struct {
					ID string `json:"id"`
				}
				if err := json.Unmarshal(raw, &created); err != nil {
					out <- outcome{index: i, err: err, code: resp.StatusCode, started: sentAt, ended: time.Now().UTC()}
					continue
				}
				if created.ID == "" {
					out <- outcome{index: i, err: fmt.Errorf("created job response has no id"), code: resp.StatusCode, started: sentAt, ended: time.Now().UTC()}
					continue
				}
				out <- outcome{index: i, id: created.ID, code: resp.StatusCode, started: sentAt, ended: time.Now().UTC()}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for i := range batch {
			select {
			case jobs <- i:
			case <-ctx.Done():
				for j := i; j < len(batch); j++ {
					out <- outcome{index: j, err: ctx.Err()}
				}
				return
			}
		}
	}()
	go func() { wg.Wait(); close(out) }()
	var accepted []string
	for v := range out {
		results[v.index].SubmissionStatus = v.code
		if !v.started.IsZero() {
			results[v.index].SubmitStartedAt = &v.started
		}
		if !v.ended.IsZero() {
			results[v.index].SubmittedAt = &v.ended
			ms := float64(v.ended.Sub(v.started).Microseconds()) / 1000
			results[v.index].SubmissionDurationMS = &ms
		}
		if v.err != nil {
			results[v.index].SubmissionError = v.err.Error()
			continue
		}
		results[v.index].Accepted = true
		results[v.index].JobID = v.id
		accepted = append(accepted, v.id)
	}
	return accepted
}

func (r *runner) token() string {
	v, _ := api.MintToken(r.secret, "atlas-experiment", time.Hour)
	return v
}

func (r *runner) waitTerminal(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		var total, finished int
		err := r.dataPool.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE status IN('succeeded','dead','failed','canceled')) FROM job_runs WHERE job_id::text=ANY($1::text[])`, ids).Scan(&total, &finished)
		if err == nil && total == len(ids) && finished == total {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for run completion: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

func (r *runner) readJobResults(ctx context.Context, entries []jobResult) (succeeded, failed, unfinished int, results []jobResult, retErr error) {
	results = entries
	ids := []string{}
	idx := map[string]int{}
	for i := range results {
		if results[i].Accepted {
			ids = append(ids, results[i].JobID)
			idx[results[i].JobID] = i
		}
	}
	if len(ids) == 0 {
		return 0, 0, len(entries), results, nil
	}
	rows, err := r.dataPool.Query(ctx, `SELECT j.id::text,r.id::text,r.execution_key::text,r.status,r.attempt,j.created_at,r.scheduled_at,r.assigned_at,COALESCE(r.assigned_worker_id,''),COALESCE(w.gpu_memory_mb,0)::bigint,r.started_at,r.finished_at,COALESCE(r.error,'') FROM job_runs r JOIN jobs j ON j.id=r.job_id LEFT JOIN workers w ON w.id=r.assigned_worker_id WHERE j.id::text=ANY($1::text[])`, ids)
	if err != nil {
		return 0, 0, len(entries), results, err
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var id, runID, key, status, message, workerID string
		var attempt int16
		var accepted, scheduled time.Time
		var assigned, started, finished *time.Time
		var workerGPUMem int64
		if err := rows.Scan(&id, &runID, &key, &status, &attempt, &accepted, &scheduled, &assigned, &workerID, &workerGPUMem, &started, &finished, &message); err != nil {
			return 0, 0, len(entries), results, err
		}
		i, ok := idx[id]
		if !ok {
			continue
		}
		seen[id] = true
		j := &results[i]
		j.RunID = runID
		j.ExecutionKey = key
		j.Status = status
		j.Attempt = attempt
		j.AcceptedAtDB = &accepted
		j.ScheduledAt = &scheduled
		j.AssignedAt = assigned
		j.AssignedWorkerID = workerID
		j.AssignedWorkerGPUMemoryMB = workerGPUMem
		j.StartedAt = started
		j.FinishedAt = finished
		j.Error = message
		if assigned != nil {
			v := float64(assigned.Sub(accepted).Microseconds()) / 1000
			j.AcceptanceToAssignmentMS = &v
		}
		if started != nil {
			v := float64(started.Sub(accepted).Microseconds()) / 1000
			j.AcceptanceToStartMS = &v
			ready := float64(started.Sub(scheduled).Microseconds()) / 1000
			j.ReadyQueueWaitMS = &ready
		}
		if started != nil && finished != nil {
			v := float64(finished.Sub(*started).Microseconds()) / 1000
			j.ExecutionDurationMS = &v
		}
		switch status {
		case "succeeded":
			succeeded++
		case "dead", "failed", "canceled":
			failed++
		default:
			unfinished++
		}
	}
	if err := rows.Err(); err != nil {
		return succeeded, failed, unfinished, results, err
	}
	for id := range idx {
		if !seen[id] {
			unfinished++
		}
	}
	unfinished += len(entries) - len(ids)
	return succeeded, failed, unfinished, results, nil
}

func (r *runner) summarizeSamples(t *trialResult) {
	r.sampleMu.RLock()
	samples := append([]dbSample(nil), r.report.Samples...)
	r.sampleMu.RUnlock()
	var cpu, locks, reserved, caps, ratios, free, slack, stranded, oldest []float64
	for _, s := range samples {
		if s.ScenarioID != t.ScenarioID {
			continue
		}
		if s.DBCPUCoreEquivalents != nil {
			cpu = append(cpu, *s.DBCPUCoreEquivalents)
		}
		locks = append(locks, s.SampledLockWaitFraction)
		if s.GPUCapacityVRAMMB > 0 {
			reserved = append(reserved, float64(s.GPUReservedVRAMMB))
			caps = append(caps, float64(s.GPUCapacityVRAMMB))
			if s.GPUReservationRatio != nil {
				ratios = append(ratios, *s.GPUReservationRatio)
			}
			free = append(free, float64(s.GPUFreeVRAMMB))
			slack = append(slack, float64(s.LargeJobSlackMB))
			stranded = append(stranded, float64(s.WholeDeviceStrandedVRAMMB))
		}
		if s.OldestReadyAgeMS != nil {
			oldest = append(oldest, *s.OldestReadyAgeMS)
		}
	}
	t.MeanDBCPUCoreEquivalents = meanPtr(cpu)
	t.MaxDBCPUCoreEquivalents = maxPtr(cpu)
	t.MeanSampledLockWaitFraction = meanPtr(locks)
	t.MeanGPUReservedVRAMMB = meanPtr(reserved)
	t.MeanGPUCapacityVRAMMB = meanPtr(caps)
	t.MeanGPUReservationRatio = meanPtr(ratios)
	t.MeanFreeGPUVRAMMB = meanPtr(free)
	t.MeanLargeJobSlackMB = meanPtr(slack)
	t.MeanWholeDeviceStrandedVRAMMB = meanPtr(stranded)
	t.MaxOldestReadyAgeMS = maxPtr(oldest)
}

func (r *runner) startFleet(n int, lease time.Duration) error {
	if r.opts.workloadProfile == "cpu" {
		for i := 0; i < n; i++ {
			if e := r.startWorker(workerSpec{id: fmt.Sprintf("w00-cpu-%02d", i), cpu: 4000, mem: 8192}, lease); e != nil {
				return e
			}
		}
		return nil
	}
	cpu := n / 2
	large := (n - cpu + 1) / 2
	small := n - cpu - large
	for i := 0; i < cpu; i++ {
		if e := r.startWorker(workerSpec{id: fmt.Sprintf("w00-cpu-%02d", i), cpu: 4000, mem: 8192}, lease); e != nil {
			return e
		}
	}
	for i := 0; i < large; i++ {
		if e := r.startWorker(workerSpec{id: fmt.Sprintf("w10-gpu-large-%02d", i), cpu: 4000, mem: 8192, gpuCount: 1, gpuType: "sim-gpu", gpuMem: 32768}, lease); e != nil {
			return e
		}
	}
	for i := 0; i < small; i++ {
		if e := r.startWorker(workerSpec{id: fmt.Sprintf("w20-gpu-small-%02d", i), cpu: 4000, mem: 8192, gpuCount: 1, gpuType: "sim-gpu", gpuMem: 8192}, lease); e != nil {
			return e
		}
	}
	return nil
}

func (r *runner) startWorker(w workerSpec, lease time.Duration) error {
	metrics, e := freeAddress()
	if e != nil {
		return e
	}
	app := "atlasexp-worker-" + safeAppName(w.id) + "-" + shortToken()
	class := "cpu"
	if w.gpuCount > 0 {
		class = "gpu-large"
		if w.gpuMem <= 8192 {
			class = "gpu-small"
		}
	}
	labels, _ := json.Marshal(map[string]string{"experiment": "true", "worker_class": class})
	env := map[string]string{"DATABASE_URL": r.processDBURL(app), "WORKER_ID": w.id, "WORKER_CONCURRENCY": "4", "WORKER_CPU_CAPACITY_MILLIS": strconv.Itoa(w.cpu), "WORKER_MEMORY_CAPACITY_MB": strconv.Itoa(w.mem), "WORKER_GPU_COUNT": strconv.Itoa(w.gpuCount), "WORKER_GPU_MEMORY_MB": strconv.Itoa(w.gpuMem), "WORKER_GPU_TYPE": w.gpuType, "WORKER_LABELS": string(labels), "METRICS_ADDR": metrics, "LEASE_DURATION": lease.String(), "POLL_INTERVAL": "50ms", "WORKER_RETRY_BASE_DELAY": "10ms", "WORKER_RETRY_MAX_DELAY": "100ms", "WORKER_RETRY_JITTER": "0", "WORKER_SHUTDOWN_GRACE_PERIOD": "2s", "EXPERIMENT_MIGRATIONS_DIR": r.opts.migrationsDir}
	if r.opts.otelEndpoint != "" {
		env["OTEL_EXPORTER_OTLP_ENDPOINT"] = r.opts.otelEndpoint
	}
	c, e := r.startProcess("worker-"+w.id, r.opts.workerBin, app, env)
	if e == nil {
		r.workers = append(r.workers, c)
	}
	return e
}

func (r *runner) startScheduler(policy string) error {
	metrics, e := freeAddress()
	if e != nil {
		return e
	}
	app := "atlasexp-scheduler-" + safeAppName(policy) + "-" + shortToken()
	env := map[string]string{"DATABASE_URL": r.processDBURL(app), "METRICS_ADDR": metrics, "SCHEDULING_POLICY": policy, "POLL_INTERVAL": r.opts.pollInterval.String(), "MAX_QUEUE_DEPTH": "20000", "PER_QUEUE_LIMIT": "20000", "PER_TENANT_LIMIT": "20000"}
	if r.opts.otelEndpoint != "" {
		env["OTEL_EXPORTER_OTLP_ENDPOINT"] = r.opts.otelEndpoint
	}
	c, e := r.startProcess("scheduler-"+policy, r.opts.schedulerBin, app, env)
	if e == nil {
		r.schedulers = append(r.schedulers, c)
	}
	return e
}

func (r *runner) waitFleet(ctx context.Context, n int) error {
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		var count int
		e := r.dataPool.QueryRow(ctx, `SELECT count(*) FROM workers WHERE status='alive' AND last_heartbeat_at>now()-INTERVAL '30 seconds'`).Scan(&count)
		if e == nil && count >= n {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("only %d of %d workers registered", count, n)
		case <-tick.C:
		}
	}
}

func (r *runner) waitLeader(ctx context.Context, limit time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		rows, e := r.leaderRows(ctx)
		if e == nil && len(rows) == 1 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("scheduler leader not observed (count %d): %w", len(rows), ctx.Err())
		case <-tick.C:
		}
	}
}
func (r *runner) leaderRows(ctx context.Context) ([]leaderRow, error) {
	rows, e := r.dataPool.Query(ctx, `SELECT a.application_name,a.pid FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE l.locktype='advisory' AND l.objid::bigint=727433001 AND l.objsubid=1 AND l.granted AND a.application_name LIKE 'atlasexp-scheduler-%'`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []leaderRow{}
	for rows.Next() {
		var v leaderRow
		if e := rows.Scan(&v.app, &v.pid); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (r *runner) assertNoActiveRuns(ctx context.Context) error {
	var n int
	if e := r.dataPool.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE status IN('queued','scheduled','assigned','leased','running')`).Scan(&n); e != nil {
		return e
	}
	if n > 0 {
		return fmt.Errorf("trial boundary has %d active runs", n)
	}
	return nil
}

func (r *runner) resetTrialHistory(ctx context.Context) error {
	if err := r.assertNoActiveRuns(ctx); err != nil {
		return err
	}
	var currentSchema string
	if err := r.dataPool.QueryRow(ctx, `SELECT current_schema()`).Scan(&currentSchema); err != nil {
		return fmt.Errorf("verify trial reset schema: %w", err)
	}
	if err := validateExperimentSchemaOwnership(currentSchema, r.schema); err != nil {
		return err
	}
	if _, err := r.dataPool.Exec(ctx, `TRUNCATE TABLE `+quoteIdentifier(r.schema)+`.jobs CASCADE`); err != nil {
		return fmt.Errorf("truncate isolated trial history in %s: %w", r.schema, err)
	}
	var remaining int
	if err := r.dataPool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&remaining); err != nil {
		return fmt.Errorf("verify trial history reset: %w", err)
	}
	if remaining != 0 {
		return fmt.Errorf("trial history reset left %d jobs in isolated schema %s", remaining, r.schema)
	}
	return nil
}
func (r *runner) stopWorkers() error {
	err := stopChildren(r.workers, 4*time.Second)
	r.workers = nil
	return err
}
func (r *runner) stopSchedulers() error {
	err := stopChildren(r.schedulers, 3*time.Second)
	r.schedulers = nil
	return err
}

func (r *runner) finish() {
	r.closeOnce.Do(func() {
		if r.samplerStarted {
			close(r.samplerStop)
			<-r.samplerDone
		}
		if e := stopChildren(r.processes, 4*time.Second); e != nil {
			r.finishErr = errors.Join(r.finishErr, e)
			r.report.Errors = append(r.report.Errors, "owned process cleanup: "+e.Error())
		}
		if r.pgProxy != nil {
			r.pgProxy.Close()
		}
		if r.redisProxy != nil {
			r.redisProxy.Close()
		}
		if r.adminPool != nil && r.schema != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, e := r.adminPool.Exec(ctx, "DROP SCHEMA IF EXISTS "+quoteIdentifier(r.schema)+" CASCADE")
			cancel()
			if e != nil {
				r.report.Cleanup["schema"] = "failed: " + e.Error()
				r.finishErr = errors.Join(r.finishErr, e)
				r.report.Errors = append(r.report.Errors, "isolated schema cleanup: "+e.Error())
			} else {
				r.report.Cleanup["schema"] = "dropped"
			}
		}
		if r.dataPool != nil {
			r.dataPool.Close()
		}
		if r.adminPool != nil {
			r.adminPool.Close()
		}
		r.report.FinishedAt = time.Now().UTC()
		if e := writeReport(r.opts.out, r.report); e != nil {
			r.finishErr = errors.Join(r.finishErr, e)
			fmt.Fprintln(os.Stderr, "write report:", e)
		}
	})
}

func (c *child) isRunning() bool { c.mu.Lock(); defer c.mu.Unlock(); return !c.exited }
func (c *child) kill() error {
	if !c.isRunning() {
		return nil
	}
	e := c.cmd.Process.Kill()
	if e != nil && !errors.Is(e, os.ErrProcessDone) {
		return e
	}
	select {
	case <-c.done:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("wait for killed %s", c.name)
	}
}
func stopChildren(children []*child, grace time.Duration) error {
	for _, c := range children {
		if c.isRunning() {
			_ = c.cmd.Process.Signal(os.Interrupt)
		}
	}
	deadline := time.Now().Add(grace)
	var out error
	for _, c := range children {
		if !c.isRunning() {
			continue
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		select {
		case <-c.done:
		case <-time.After(remaining):
		}
	}
	for _, c := range children {
		if c.isRunning() {
			if e := c.cmd.Process.Kill(); e != nil && !errors.Is(e, os.ErrProcessDone) {
				out = errors.Join(out, fmt.Errorf("kill owned process %s: %w", c.name, e))
			}
		}
	}
	wait := time.NewTimer(5 * time.Second)
	defer wait.Stop()
	for _, c := range children {
		if !c.isRunning() {
			continue
		}
		select {
		case <-c.done:
		case <-wait.C:
			out = errors.Join(out, fmt.Errorf("owned process %s did not exit after kill", c.name))
			return out
		}
	}
	return out
}

type pacer struct {
	rate float64
	mu   sync.Mutex
	next time.Time
}

func newPacer(rate float64) *pacer { return &pacer{rate: rate} }
func (p *pacer) Wait(ctx context.Context) error {
	if p.rate <= 0 {
		return nil
	}
	interval := time.Duration(float64(time.Second) / p.rate)
	p.mu.Lock()
	now := time.Now()
	at := p.next
	if at.Before(now) {
		at = now
	}
	p.next = at.Add(interval)
	p.mu.Unlock()
	if wait := time.Until(at); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

func workloadBatch(n int, seed int64, d time.Duration, profile string) []apiSubmission {
	cpu, small := n, 0
	if profile == "mixed" {
		cpu, small = n*70/100, n*20/100
	}
	large := n - cpu - small
	out := make([]apiSubmission, 0, n)
	for i := 0; i < cpu; i++ {
		out = append(out, apiSubmission{Workload: "cpu", Name: "simulate_cpu", Payload: map[string]any{"duration_ms": d.Milliseconds()}})
	}
	for i := 0; i < small; i++ {
		out = append(out, apiSubmission{Workload: "gpu-small", Name: "simulate_gpu", GPUCount: 1, GPUMemMB: 4096, Payload: map[string]any{"duration_ms": d.Milliseconds(), "simulate_gpu": true, "class": "small"}})
	}
	for i := 0; i < large; i++ {
		out = append(out, apiSubmission{Workload: "gpu-large", Name: "simulate_gpu", GPUCount: 1, GPUMemMB: 16384, Payload: map[string]any{"duration_ms": d.Milliseconds(), "simulate_gpu": true, "class": "large"}})
	}
	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	for i := range out {
		out[i].JobID = fmt.Sprintf("planned-%d", i)
	}
	return out
}

func parseInts(raw string) ([]int, error) {
	var out []int
	for _, s := range strings.Split(raw, ",") {
		n, e := strconv.Atoi(strings.TrimSpace(s))
		if e != nil || n < 1 {
			return nil, fmt.Errorf("worker counts must be positive integers")
		}
		out = append(out, n)
	}
	return out, nil
}
func parsePolicies(raw string) ([]string, error) {
	var out []string
	for _, s := range strings.Split(raw, ",") {
		p := strings.ToLower(strings.TrimSpace(s))
		if p != "first-fit" && p != "best-fit" {
			return nil, fmt.Errorf("policies must be first-fit,best-fit")
		}
		out = append(out, p)
	}
	return out, nil
}
func parseRates(raw string) ([]float64, error) {
	var out []float64
	for _, s := range strings.Split(raw, ",") {
		v, e := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if e != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return nil, fmt.Errorf("submission rates must be finite and nonnegative")
		}
		out = append(out, v)
	}
	return out, nil
}
func rateLabel(v float64) string {
	if v == 0 {
		return "open"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}
func ratePerSecond(n int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds()
}
func percentile(v []float64, p float64) float64 {
	if len(v) == 0 {
		return 0
	}
	ordered := append([]float64(nil), v...)
	sort.Float64s(ordered)
	i := int(float64(len(ordered))*p+0.999999) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(ordered) {
		i = len(ordered) - 1
	}
	return ordered[i]
}
func meanPtr(v []float64) *float64 {
	if len(v) == 0 {
		return nil
	}
	var n float64
	for _, x := range v {
		n += x
	}
	x := n / float64(len(v))
	return &x
}
func maxPtr(v []float64) *float64 {
	if len(v) == 0 {
		return nil
	}
	m := v[0]
	for _, x := range v[1:] {
		if x > m {
			m = x
		}
	}
	return &m
}
func countUnfinished(v []jobResult) int {
	n := 0
	for _, j := range v {
		if !j.Accepted || (j.Status != "succeeded" && j.Status != "dead" && j.Status != "failed" && j.Status != "canceled") {
			n++
		}
	}
	return n
}
func freeAddress() (string, error) {
	ln, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return "", e
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		return "", err
	}
	return addr, nil
}
func shortToken() string {
	b := make([]byte, 3)
	if _, e := cryptorand.Read(b); e != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b)
}
func safeFileName(s string) string {
	r := strings.NewReplacer("/", "-", "\\", "-", " ", "-")
	return r.Replace(s)
}
func safeAppName(s string) string {
	s = safeFileName(s)
	if len(s) > 26 {
		return s[:26]
	}
	return s
}
func defaultPort(v, f string) string {
	if v == "" {
		return f
	}
	return v
}
func quoteIdentifier(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func experimentSchemaName(runID string) string {
	return strings.ToLower("atlas_exp_" + strings.ReplaceAll(runID, "-", ""))
}
func validateExperimentSchemaOwnership(current, expected string) error {
	if expected != strings.ToLower(expected) || !strings.HasPrefix(expected, "atlas_exp_") {
		return fmt.Errorf("refusing schema operation: %q is not a lowercase atlas_exp_ schema", expected)
	}
	if current != expected {
		return fmt.Errorf("refusing schema operation: current_schema() is %q, expected isolated schema %q", current, expected)
	}
	return nil
}
func binaryMetadata(path string) map[string]any {
	info := map[string]any{"path": path}
	f, err := os.Open(path)
	if err != nil {
		info["sha256_error"] = err.Error()
		return info
	}
	h := sha256.New()
	_, copyErr := io.Copy(h, f)
	closeErr := f.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		info["sha256_error"] = err.Error()
		return info
	}
	info["sha256"] = hex.EncodeToString(h.Sum(nil))
	return info
}
func mergeEnvironment(overrides map[string]string) []string {
	values := map[string]string{}
	for _, s := range os.Environ() {
		if i := strings.IndexByte(s, '='); i >= 0 {
			values[s[:i]] = s[i+1:]
		}
	}
	for k, v := range overrides {
		values[k] = v
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+values[k])
	}
	return out
}
func migrationVersions(ctx context.Context, p *pgxpool.Pool) ([]string, error) {
	rows, e := p.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if e := rows.Scan(&v); e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
