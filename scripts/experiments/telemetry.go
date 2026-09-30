package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func writeReport(dir string, r *report) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), append(data, '\n'), 0644); err != nil {
		return err
	}
	if err := writeTrialsCSV(filepath.Join(dir, "trials.csv"), r.Trials); err != nil {
		return err
	}
	if err := writeSamplesCSV(filepath.Join(dir, "samples.csv"), r.Samples); err != nil {
		return err
	}
	return nil
}

func writeTrialsCSV(path string, trials []trialResult) (retErr error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	w := csv.NewWriter(f)
	header := []string{"scenario_id", "workload_profile", "policy", "workers", "trial", "seed", "offered_jobs_per_second", "planned", "accepted", "succeeded", "failed_or_dead", "unfinished_at_deadline", "submission_ms", "completion_drain_ms", "total_batch_ms", "observed_submit_jobs_per_second", "whole_batch_execution_jobs_per_second", "p95_acceptance_to_assignment_ms", "p95_acceptance_to_start_ms", "p50_schedule_to_start_ms", "p95_schedule_to_start_ms", "max_schedule_to_start_ms", "p95_ready_wait_by_workload", "unfinished_by_workload", "max_oldest_ready_age_ms", "mean_db_cpu_cores", "max_db_cpu_cores", "mean_sampled_lock_wait_fraction", "mean_gpu_reserved_vram_mb", "mean_gpu_capacity_vram_mb", "mean_gpu_reservation_ratio", "mean_free_gpu_vram_mb", "mean_subthreshold_available_vram_slack_mb", "mean_whole_device_stranded_vram_mb", "small_gpu_jobs_accepted", "small_gpu_jobs_placed_on_large_gpu", "error"}
	if err := w.Write(header); err != nil {
		return err
	}
	for _, t := range trials {
		byWork, _ := json.Marshal(t.P95ReadyWaitByWorkload)
		unfinished, _ := json.Marshal(t.UnfinishedByWorkload)
		row := []string{t.ScenarioID, t.WorkloadProfile, t.Policy, strconv.Itoa(t.WorkerCount), strconv.Itoa(t.Trial), strconv.FormatInt(t.Seed, 10), fmtFloat(t.OfferedSubmissionRate), strconv.Itoa(t.JobsPlanned), strconv.Itoa(t.JobsAccepted), strconv.Itoa(t.JobsSucceeded), strconv.Itoa(t.JobsDeadOrFailed), strconv.Itoa(t.UnfinishedAtDeadline), fmtFloat(t.SubmissionDurationMS), fmtFloat(t.CompletionDurationMS), fmtFloat(t.TotalDurationMS), fmtFloat(t.ObservedSubmissionRate), fmtFloat(t.ExecutionThroughput), fmtPtr(t.P95SchedulerLatencyMS), fmtPtr(t.P95AcceptanceToStartMS), fmtPtr(t.P50ReadyQueueWaitMS), fmtPtr(t.P95ReadyQueueWaitMS), fmtPtr(t.MaxReadyQueueWaitMS), string(byWork), string(unfinished), fmtPtr(t.MaxOldestReadyAgeMS), fmtPtr(t.MeanDBCPUCoreEquivalents), fmtPtr(t.MaxDBCPUCoreEquivalents), fmtPtr(t.MeanSampledLockWaitFraction), fmtPtr(t.MeanGPUReservedVRAMMB), fmtPtr(t.MeanGPUCapacityVRAMMB), fmtPtr(t.MeanGPUReservationRatio), fmtPtr(t.MeanFreeGPUVRAMMB), fmtPtr(t.MeanLargeJobSlackMB), fmtPtr(t.MeanWholeDeviceStrandedVRAMMB), strconv.Itoa(t.SmallGPUJobsAccepted), strconv.Itoa(t.SmallGPUJobsPlacedOnLargeGPU), t.Error}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func writeSamplesCSV(path string, samples []dbSample) (retErr error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	w := csv.NewWriter(f)
	if err := w.Write([]string{"at", "scenario_id", "db_cpu_core_equivalents", "proc_cpu_error", "active_app_connections", "idle_app_connections", "lock_waiters", "sampled_lock_wait_fraction", "oldest_ready_age_ms", "gpu_reserved_vram_mb", "gpu_capacity_vram_mb", "gpu_free_vram_mb", "gpu_available_devices", "gpu_reserved_devices", "gpu_reservation_ratio", "subthreshold_available_vram_slack_for_16gb_job_mb", "whole_device_stranded_vram_mb", "connections_by_application"}); err != nil {
		return err
	}
	for _, s := range samples {
		conns, _ := json.Marshal(s.ConnectionsByApp)
		row := []string{s.At.Format(time.RFC3339Nano), s.ScenarioID, fmtPtr(s.DBCPUCoreEquivalents), s.ProcCPUError, strconv.Itoa(s.ActiveAppConnections), strconv.Itoa(s.IdleAppConnections), strconv.Itoa(s.LockWaiters), fmtFloat(s.SampledLockWaitFraction), fmtPtr(s.OldestReadyAgeMS), strconv.FormatInt(s.GPUReservedVRAMMB, 10), strconv.FormatInt(s.GPUCapacityVRAMMB, 10), strconv.FormatInt(s.GPUFreeVRAMMB, 10), strconv.FormatInt(s.GPUAvailableDevices, 10), strconv.FormatInt(s.GPUReservedDevices, 10), fmtPtr(s.GPUReservationRatio), strconv.FormatInt(s.LargeJobSlackMB, 10), strconv.FormatInt(s.WholeDeviceStrandedVRAMMB, 10), string(conns)}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func fmtPtr(v *float64) string {
	if v == nil {
		return ""
	}
	return fmtFloat(*v)
}
func fmtFloat(v float64) string { return strconv.FormatFloat(v, 'f', 6, 64) }

// procCPUSampler estimates postmaster plus descendant CPU from /proc/stat and
// reads the active tick rate through getconf instead of assuming a fixed value.
type procCPUSampler struct {
	pidFile        string
	ticksPerSecond float64
	initErr        error
	lastAt         time.Time
	last           map[int]procEntry
}

func newProcCPUSampler(pidFile string) *procCPUSampler {
	p := &procCPUSampler{pidFile: pidFile, last: map[int]procEntry{}}
	raw, err := exec.Command("getconf", "CLK_TCK").Output()
	if err != nil {
		p.initErr = fmt.Errorf("getconf CLK_TCK unavailable: %w", err)
		return p
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(string(raw)), 64)
	if err != nil || v <= 0 {
		p.initErr = fmt.Errorf("invalid getconf CLK_TCK value %q", strings.TrimSpace(string(raw)))
		return p
	}
	p.ticksPerSecond = v
	return p
}

type procEntry struct {
	pid, ppid                         int
	startTime, userTicks, systemTicks uint64
}

func (p *procCPUSampler) Sample(at time.Time) (float64, error) {
	if p.initErr != nil {
		return 0, p.initErr
	}
	raw, err := os.ReadFile(p.pidFile)
	if err != nil {
		return 0, fmt.Errorf("read PostgreSQL PID file: %w", err)
	}
	lines := strings.Fields(string(raw))
	if len(lines) == 0 {
		return 0, fmt.Errorf("PostgreSQL PID file is empty")
	}
	root, err := strconv.Atoi(lines[0])
	if err != nil {
		return 0, fmt.Errorf("parse postmaster PID: %w", err)
	}
	uptimeRaw, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return 0, fmt.Errorf("read system uptime: %w", err)
	}
	uptimeFields := strings.Fields(string(uptimeRaw))
	if len(uptimeFields) == 0 {
		return 0, fmt.Errorf("/proc/uptime is empty")
	}
	uptimeSeconds, err := strconv.ParseFloat(uptimeFields[0], 64)
	if err != nil || uptimeSeconds < 0 {
		return 0, fmt.Errorf("parse /proc/uptime: %q", string(uptimeRaw))
	}
	bootAt := at.Add(-time.Duration(uptimeSeconds * float64(time.Second)))
	dirs, err := os.ReadDir("/proc")
	if err != nil {
		return 0, err
	}
	entries := make(map[int]procEntry, len(dirs))
	for _, d := range dirs {
		pid, e := strconv.Atoi(d.Name())
		if e != nil {
			continue
		}
		b, e := os.ReadFile(filepath.Join("/proc", d.Name(), "stat"))
		if e != nil {
			continue
		}
		entry, e := parseProcEntry(pid, string(b))
		if e == nil {
			entries[pid] = entry
		}
	}
	if _, ok := entries[root]; !ok {
		return 0, fmt.Errorf("postmaster process %d is not visible in /proc", root)
	}
	children := map[int][]int{}
	for pid, e := range entries {
		children[e.ppid] = append(children[e.ppid], pid)
	}
	seen := map[int]bool{}
	stack := []int{root}
	current := map[int]procEntry{}
	for len(stack) > 0 {
		pid := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[pid] {
			continue
		}
		seen[pid] = true
		if e, ok := entries[pid]; ok {
			current[pid] = e
		}
		stack = append(stack, children[pid]...)
	}
	if !p.lastAt.IsZero() && !at.After(p.lastAt) {
		return 0, fmt.Errorf("non-increasing CPU sample time")
	}
	if p.lastAt.IsZero() {
		p.lastAt = at
		p.last = current
		return 0, fmt.Errorf("CPU sampler warming up")
	}
	var deltaTicks uint64
	for pid, e := range current {
		old, ok := p.last[pid]
		nowTicks := e.userTicks + e.systemTicks
		if !ok || e.startTime != old.startTime {
			startedAt := bootAt.Add(time.Duration(float64(e.startTime) / p.ticksPerSecond * float64(time.Second)))
			if startedAt.After(p.lastAt) {
				deltaTicks += nowTicks
			}
			continue
		}
		oldTicks := old.userTicks + old.systemTicks
		if nowTicks >= oldTicks {
			deltaTicks += nowTicks - oldTicks
		}
	}
	elapsed := at.Sub(p.lastAt).Seconds()
	p.lastAt = at
	p.last = current
	return (float64(deltaTicks) / p.ticksPerSecond) / elapsed, nil
}

func parseProcEntry(pid int, raw string) (procEntry, error) {
	close := strings.LastIndex(raw, ")")
	if close < 0 {
		return procEntry{}, fmt.Errorf("malformed proc stat")
	}
	fields := strings.Fields(raw[close+1:])
	if len(fields) < 20 {
		return procEntry{}, fmt.Errorf("short proc stat")
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return procEntry{}, err
	}
	user, err := strconv.ParseUint(fields[11], 10, 64)
	if err != nil {
		return procEntry{}, err
	}
	system, err := strconv.ParseUint(fields[12], 10, 64)
	if err != nil {
		return procEntry{}, err
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return procEntry{}, err
	}
	return procEntry{pid: pid, ppid: ppid, startTime: start, userTicks: user, systemTicks: system}, nil
}
