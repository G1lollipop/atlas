package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/G1lollipop/atlas/internal/metrics"
	"github.com/G1lollipop/atlas/internal/model"
	"github.com/G1lollipop/atlas/internal/store"
)

// PriorityWeight is the amount of queue age represented by one priority point.
// One priority point equals one hour of waiting. This lets older low-priority work
// overtake newly arriving high-priority runs during a sustained backlog.
const PriorityWeight = time.Hour

// AssignmentTTL bounds how long a run may stay assigned without being leased.
const AssignmentTTL = 30 * time.Second

// HeartbeatTTL is the maximum age accepted for a worker heartbeat during dispatch.
const HeartbeatTTL = 30 * time.Second

// scheduledRunsLimit bounds each database page; listScheduledCandidates scans
// consecutive pages so incompatible leading rows cannot hide later candidates.
const scheduledRunsLimit = 1000

// Dispatcher advances due queued runs to scheduled and places them on live,
// resource-compatible workers. It is called only by the elected scheduler leader.
type Dispatcher struct {
	Store  store.Store
	Logger *slog.Logger
	Now    func() time.Time
	Policy SchedulingPolicy
}

func NewDispatcher(st store.Store, logger *slog.Logger) *Dispatcher {
	return NewDispatcherWithPolicy(st, logger, PriorityAware{})
}

func NewDispatcherWithPolicy(st store.Store, logger *slog.Logger, policy SchedulingPolicy) *Dispatcher {
	if policy == nil {
		policy = PriorityAware{}
	}
	return &Dispatcher{Store: st, Logger: logger, Now: time.Now, Policy: policy}
}

func (d *Dispatcher) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d *Dispatcher) logger() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.Default()
}

// DispatchOnce performs one leader-owned scheduling pass: expired worker leases
// and stale assignments are recovered, due queued runs become scheduled, and all
// feasible candidates are assigned until the current workers have no capacity.
func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	if d.Store == nil {
		return 0, fmt.Errorf("scheduler: dispatcher store is nil")
	}

	reclaimed, err := d.Store.ReclaimExpiredLeases(ctx)
	if err != nil {
		return 0, fmt.Errorf("scheduler: reclaim expired worker leases: %w", err)
	}
	if reclaimed > 0 {
		metrics.LeasesReclaimed.Add(float64(reclaimed))
		metrics.LeaseExpiredTotal.Add(float64(reclaimed))
	}
	expired, err := d.Store.RequeueExpiredAssignments(ctx)
	if err != nil {
		return 0, fmt.Errorf("scheduler: requeue expired assignments: %w", err)
	}
	scheduled, err := d.Store.ScheduleDueRuns(ctx)
	if err != nil {
		return 0, fmt.Errorf("scheduler: schedule due runs: %w", err)
	}

	assigned, err := d.assignScheduledRuns(ctx)
	if err != nil {
		return assigned, err
	}
	if reclaimed != 0 || expired != 0 || scheduled != 0 || assigned != 0 {
		d.logger().Info("scheduler dispatch pass completed",
			"reclaimed_leases", reclaimed,
			"expired_assignments", expired,
			"scheduled", scheduled,
			"assigned", assigned,
		)
	}
	return assigned, nil
}

func (d *Dispatcher) assignScheduledRuns(ctx context.Context) (int, error) {
	candidates, err := d.listScheduledCandidates(ctx)
	if err != nil {
		return 0, err
	}
	if len(candidates) == 0 {
		return 0, nil
	}
	workers, err := d.Store.ListWorkers(ctx)
	if err != nil {
		return 0, fmt.Errorf("scheduler: list workers: %w", err)
	}
	workers = cloneWorkers(workers)
	now := d.now()
	policy := d.Policy
	if policy == nil {
		policy = PriorityAware{}
	}
	pairs, err := policyAssignments(candidates, workers, now, policy)
	if err != nil {
		return 0, fmt.Errorf("scheduler: select worker assignments: %w", err)
	}
	candidateByID := make(map[string]*model.RunCandidate, len(candidates))
	workerByID := make(map[string]*model.Worker, len(workers))
	for _, candidate := range candidates {
		if candidate != nil && candidate.Run != nil && candidate.Job != nil {
			candidateByID[candidate.Run.ID] = candidate
		}
	}
	for _, worker := range workers {
		if worker != nil {
			workerByID[worker.ID] = worker
		}
	}

	assigned := 0
	assignedRuns := make(map[string]struct{})
	for _, scored := range pairs {
		candidate := candidateByID[scored.pair.runID]
		worker := workerByID[scored.pair.workerID]
		if candidate == nil || worker == nil {
			continue
		}
		if _, alreadyAssigned := assignedRuns[candidate.Run.ID]; alreadyAssigned {
			continue
		}
		if !WorkerCanRun(candidate.Job, worker, now, HeartbeatTTL) {
			continue
		}

		ok, err := d.Store.AssignRun(ctx, candidate.Run.ID, worker.ID, AssignmentTTL, HeartbeatTTL)
		if err != nil {
			return assigned, fmt.Errorf("scheduler: assign run %s to worker %s: %w", candidate.Run.ID, worker.ID, err)
		}
		if !ok {
			// A competing scheduler or worker heartbeat can invalidate a snapshot.
			// Each pair appears once, so a race rejection cannot spin this pass.
			continue
		}
		assigned++
		assignedRuns[candidate.Run.ID] = struct{}{}
		reserveLocal(worker, candidate.Job)
	}
	return assigned, nil
}

func (d *Dispatcher) listScheduledCandidates(ctx context.Context) ([]*model.RunCandidate, error) {
	candidates := make([]*model.RunCandidate, 0)
	if pager, ok := d.Store.(store.ScheduledRunPager); ok {
		var cursor *store.ScheduledRunCursor
		for {
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("scheduler: scan scheduled run pages: %w", err)
			}
			page, err := pager.ListScheduledRunsAfter(ctx, scheduledRunsLimit, cursor)
			if err != nil {
				return nil, fmt.Errorf("scheduler: list scheduled runs after cursor: %w", err)
			}
			candidates = append(candidates, page...)
			if len(page) == 0 || len(page) < scheduledRunsLimit {
				return candidates, nil
			}
			last := page[len(page)-1]
			if last == nil || last.Run == nil {
				return nil, fmt.Errorf("scheduler: scheduled run page ended with a missing run cursor")
			}
			cursor = &store.ScheduledRunCursor{
				Priority:    last.Run.Priority,
				ScheduledAt: last.Run.ScheduledAt,
				CreatedAt:   last.Run.CreatedAt,
				ID:          last.Run.ID,
			}
		}
	}

	// Compatibility for Store implementations that still expose only offset pages.
	for offset := 0; ; {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("scheduler: scan scheduled run pages: %w", err)
		}
		limit := scheduledRunsLimit
		page, err := d.Store.ListScheduledRuns(ctx, limit, offset)
		if err != nil {
			return nil, fmt.Errorf("scheduler: list scheduled runs at offset %d: %w", offset, err)
		}
		candidates = append(candidates, page...)
		offset += len(page)
		if len(page) < limit {
			return candidates, nil
		}
	}
}

type assignmentPair struct {
	runID    string
	workerID string
}

type scoredAssignment struct {
	pair       assignmentPair
	score      float64
	scheduled  time.Time
	workerRank int
}

func policyAssignments(candidates []*model.RunCandidate, workers []*model.Worker, now time.Time, policy SchedulingPolicy) ([]scoredAssignment, error) {
	pairs := make([]scoredAssignment, 0)
	priorityAware := isPriorityAware(policy)
	for _, candidate := range candidates {
		if candidate == nil || candidate.Run == nil || candidate.Job == nil {
			continue
		}
		remaining := orderedWorkers(workers)
		workerRank := 0
		for len(remaining) > 0 {
			worker, err := policy.SelectWorker(candidate, remaining, now)
			if err != nil {
				return nil, fmt.Errorf("run %s: %w", candidate.Run.ID, err)
			}
			if worker == nil {
				// No feasible worker is a normal scheduling outcome. Keep scanning
				// later candidates so an incompatible head cannot block the queue.
				break
			}
			index := -1
			for i, available := range remaining {
				if available.ID == worker.ID {
					index = i
					worker = available
					break
				}
			}
			if index < 0 {
				return nil, fmt.Errorf("policy %T selected worker %q outside the available worker set", policy, worker.ID)
			}
			if !WorkerCanRun(candidate.Job, worker, now, HeartbeatTTL) {
				return nil, fmt.Errorf("policy %T selected infeasible worker %q for run %q", policy, worker.ID, candidate.Run.ID)
			}

			score := SchedulingScore(candidate.Run, candidate.Job, worker, now)
			if !priorityAware {
				score = queueSchedulingScore(candidate.Run, now)
			}
			pairs = append(pairs, scoredAssignment{
				pair:  assignmentPair{runID: candidate.Run.ID, workerID: worker.ID},
				score: score, scheduled: candidate.Run.ScheduledAt, workerRank: workerRank,
			})
			workerRank++
			remaining = append(remaining[:index], remaining[index+1:]...)
		}
	}

	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].score != pairs[j].score {
			return pairs[i].score > pairs[j].score
		}
		if !pairs[i].scheduled.Equal(pairs[j].scheduled) {
			return pairs[i].scheduled.Before(pairs[j].scheduled)
		}
		if pairs[i].pair.runID != pairs[j].pair.runID {
			return pairs[i].pair.runID < pairs[j].pair.runID
		}
		if !priorityAware && pairs[i].workerRank != pairs[j].workerRank {
			return pairs[i].workerRank < pairs[j].workerRank
		}
		return pairs[i].pair.workerID < pairs[j].pair.workerID
	})
	return pairs, nil
}

func isPriorityAware(policy SchedulingPolicy) bool {
	switch policy.(type) {
	case PriorityAware, *PriorityAware:
		return true
	default:
		return false
	}
}

func queueSchedulingScore(run *model.JobRun, now time.Time) float64 {
	if run == nil {
		return math.Inf(-1)
	}
	waitSeconds := math.Max(0, now.Sub(run.ScheduledAt).Seconds())
	return float64(run.Priority)*PriorityWeight.Seconds() + waitSeconds
}

func bestAssignment(candidates []*model.RunCandidate, workers []*model.Worker, now time.Time, attempted map[assignmentPair]struct{}) (assignmentPair, bool) {
	pairs := rankedAssignments(candidates, workers, now)
	for _, candidatePair := range pairs {
		if _, seen := attempted[candidatePair.pair]; !seen {
			return candidatePair.pair, true
		}
	}
	return assignmentPair{}, false
}

func rankedAssignments(candidates []*model.RunCandidate, workers []*model.Worker, now time.Time) []scoredAssignment {
	pairs := make([]scoredAssignment, 0)
	for _, candidate := range candidates {
		if candidate == nil || candidate.Run == nil || candidate.Job == nil {
			continue
		}
		for _, worker := range workers {
			if worker == nil || !WorkerCanRun(candidate.Job, worker, now, HeartbeatTTL) {
				continue
			}
			pair := assignmentPair{runID: candidate.Run.ID, workerID: worker.ID}
			pairs = append(pairs, scoredAssignment{
				pair:      pair,
				score:     SchedulingScore(candidate.Run, candidate.Job, worker, now),
				scheduled: candidate.Run.ScheduledAt,
			})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].score != pairs[j].score {
			return pairs[i].score > pairs[j].score
		}
		if !pairs[i].scheduled.Equal(pairs[j].scheduled) {
			return pairs[i].scheduled.Before(pairs[j].scheduled)
		}
		if pairs[i].pair.runID != pairs[j].pair.runID {
			return pairs[i].pair.runID < pairs[j].pair.runID
		}
		return pairs[i].pair.workerID < pairs[j].pair.workerID
	})
	return pairs
}

func cloneWorkers(workers []*model.Worker) []*model.Worker {
	cloned := make([]*model.Worker, 0, len(workers))
	for _, worker := range workers {
		if worker == nil {
			cloned = append(cloned, nil)
			continue
		}
		copyWorker := *worker
		cloned = append(cloned, &copyWorker)
	}
	return cloned
}

func reserveLocal(worker *model.Worker, job *model.Job) {
	worker.AvailableCPUMillis = maxAvailable(worker.AvailableCPUMillis - int64(job.RequiredCPUMillis))
	worker.AvailableMemoryMB = maxAvailable(worker.AvailableMemoryMB - int64(job.RequiredMemoryMB))
	worker.AvailableGPUCount = maxAvailable(worker.AvailableGPUCount - int64(job.RequiredGPUCount))
	worker.AvailableGPUMemoryMB = maxAvailable(worker.AvailableGPUMemoryMB - int64(job.RequiredGPUCount)*int64(job.RequiredGPUMemoryMB))
}

func maxAvailable(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

// SchedulingScore uses priority*PriorityWeight + waiting_seconds - gpu_penalty.
// Waiting time is unbounded. The GPU penalty is the fraction of per-device VRAM
// left unused by this job (0..1). Since worker inventory exposes aggregate free
// VRAM, placement assumes homogeneous GPUs.
func SchedulingScore(run *model.JobRun, job *model.Job, worker *model.Worker, now time.Time) float64 {
	if run == nil || job == nil {
		return math.Inf(-1)
	}
	waitSeconds := math.Max(0, now.Sub(run.ScheduledAt).Seconds())
	priorityScore := float64(run.Priority) * PriorityWeight.Seconds()
	return priorityScore + waitSeconds - gpuFragmentationPenalty(job, worker)
}

func gpuFragmentationPenalty(job *model.Job, worker *model.Worker) float64 {
	if job == nil || worker == nil || job.RequiredGPUCount <= 0 || job.RequiredGPUMemoryMB <= 0 || worker.GPUMemoryMB <= 0 {
		return 0
	}
	unusedFraction := float64(worker.GPUMemoryMB-job.RequiredGPUMemoryMB) / float64(worker.GPUMemoryMB)
	return math.Max(0, math.Min(1, unusedFraction))
}

// WorkerCanRun checks worker liveness, capability, and the reservation-adjusted
// free resources exposed by ListWorkers. AssignRun repeats these checks atomically.
func WorkerCanRun(job *model.Job, worker *model.Worker, now time.Time, heartbeatTTL time.Duration) bool {
	if job == nil || worker == nil || worker.ID == "" || worker.Status != model.WorkerStatusAlive {
		return false
	}
	if heartbeatTTL > 0 && now.Sub(worker.LastHeartbeatAt) > heartbeatTTL {
		return false
	}
	if job.RequiredCPUMillis < 0 || job.RequiredMemoryMB < 0 || job.RequiredGPUCount < 0 || job.RequiredGPUMemoryMB < 0 {
		return false
	}
	if worker.AvailableCPUMillis < int64(job.RequiredCPUMillis) || worker.AvailableMemoryMB < int64(job.RequiredMemoryMB) {
		return false
	}
	if job.RequiredGPUCount == 0 {
		return job.RequiredGPUMemoryMB == 0 && strings.TrimSpace(job.RequiredAccelerator) == ""
	}
	if worker.AvailableGPUCount < int64(job.RequiredGPUCount) || worker.GPUMemoryMB < job.RequiredGPUMemoryMB {
		return false
	}
	neededVRAM := int64(job.RequiredGPUCount) * int64(job.RequiredGPUMemoryMB)
	if worker.AvailableGPUMemoryMB < neededVRAM {
		return false
	}
	return strings.TrimSpace(job.RequiredAccelerator) == "" || strings.EqualFold(strings.TrimSpace(job.RequiredAccelerator), strings.TrimSpace(worker.GPUType))
}
