package scheduler

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
)

// SchedulingPolicy selects the next worker to try for a scheduled run. The
// candidate carries run priority and queue age; now makes age calculations
// deterministic in tests. Dispatcher may call SelectWorker repeatedly with a
// shrinking worker set when an atomic assignment loses a race.
type SchedulingPolicy interface {
	SelectWorker(candidate *model.RunCandidate, workers []*model.Worker, now time.Time) (*model.Worker, error)
}

// FirstFit selects the first feasible worker in stable worker-ID order.
type FirstFit struct{}

// LeastLoaded selects the feasible worker with the lowest normalized utilization
// across resources requested by this run.
type LeastLoaded struct{}

// BestFit selects the feasible worker that leaves the least normalized slack for
// the resources requested by this run.
type BestFit struct{}

// PriorityAware applies the scheduler's queue priority, unbounded waiting-age,
// and GPU-fragmentation score. Across runs, Dispatcher ranks every feasible
// run-worker pair by this score.
type PriorityAware struct{}

// ParseSchedulingPolicy constructs a policy from SCHEDULING_POLICY. Empty input
// selects PriorityAware so existing deployments keep their current behavior.
func ParseSchedulingPolicy(name string) (SchedulingPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "priority-aware", "priorityaware":
		return PriorityAware{}, nil
	case "first-fit", "firstfit":
		return FirstFit{}, nil
	case "least-loaded", "leastloaded":
		return LeastLoaded{}, nil
	case "best-fit", "bestfit":
		return BestFit{}, nil
	default:
		return nil, fmt.Errorf("scheduler: unknown SCHEDULING_POLICY %q (valid values: priority-aware, first-fit, least-loaded, best-fit)", name)
	}
}

// SchedulingPolicyName returns a stable configuration/logging name for a built-in
// policy, or the Go type name for a custom implementation.
func SchedulingPolicyName(policy SchedulingPolicy) string {
	switch policy.(type) {
	case FirstFit, *FirstFit:
		return "first-fit"
	case LeastLoaded, *LeastLoaded:
		return "least-loaded"
	case BestFit, *BestFit:
		return "best-fit"
	case PriorityAware, *PriorityAware:
		return "priority-aware"
	default:
		return fmt.Sprintf("%T", policy)
	}
}

func (FirstFit) SelectWorker(candidate *model.RunCandidate, workers []*model.Worker, now time.Time) (*model.Worker, error) {
	if err := validatePolicyCandidate(candidate); err != nil {
		return nil, err
	}
	ordered := orderedWorkers(workers)
	for _, worker := range ordered {
		if WorkerCanRun(candidate.Job, worker, now, HeartbeatTTL) {
			return worker, nil
		}
	}
	return nil, nil
}

func (LeastLoaded) SelectWorker(candidate *model.RunCandidate, workers []*model.Worker, now time.Time) (*model.Worker, error) {
	if err := validatePolicyCandidate(candidate); err != nil {
		return nil, err
	}
	var selected *model.Worker
	bestLoad := math.Inf(1)
	for _, worker := range orderedWorkers(workers) {
		if !WorkerCanRun(candidate.Job, worker, now, HeartbeatTTL) {
			continue
		}
		load := normalizedWorkerLoad(candidate.Job, worker)
		if selected == nil || load < bestLoad {
			selected, bestLoad = worker, load
		}
	}
	return selected, nil
}

func (BestFit) SelectWorker(candidate *model.RunCandidate, workers []*model.Worker, now time.Time) (*model.Worker, error) {
	if err := validatePolicyCandidate(candidate); err != nil {
		return nil, err
	}
	var selected *model.Worker
	bestSlack := math.Inf(1)
	for _, worker := range orderedWorkers(workers) {
		if !WorkerCanRun(candidate.Job, worker, now, HeartbeatTTL) {
			continue
		}
		slack := normalizedRemainingCapacity(candidate.Job, worker)
		if selected == nil || slack < bestSlack {
			selected, bestSlack = worker, slack
		}
	}
	return selected, nil
}

func (PriorityAware) SelectWorker(candidate *model.RunCandidate, workers []*model.Worker, now time.Time) (*model.Worker, error) {
	if err := validatePolicyCandidate(candidate); err != nil {
		return nil, err
	}
	var selected *model.Worker
	bestScore := math.Inf(-1)
	for _, worker := range orderedWorkers(workers) {
		if !WorkerCanRun(candidate.Job, worker, now, HeartbeatTTL) {
			continue
		}
		score := SchedulingScore(candidate.Run, candidate.Job, worker, now)
		if selected == nil || score > bestScore {
			selected, bestScore = worker, score
		}
	}
	return selected, nil
}

func validatePolicyCandidate(candidate *model.RunCandidate) error {
	if candidate == nil || candidate.Run == nil || candidate.Job == nil {
		return fmt.Errorf("scheduler: scheduling policy requires a run candidate with both run and job")
	}
	return nil
}

func orderedWorkers(workers []*model.Worker) []*model.Worker {
	ordered := make([]*model.Worker, 0, len(workers))
	for _, worker := range workers {
		if worker != nil {
			ordered = append(ordered, worker)
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].ID < ordered[j].ID
	})
	return ordered
}

func normalizedWorkerLoad(job *model.Job, worker *model.Worker) float64 {
	var total float64
	var dimensions int
	add := func(available, capacity int64) {
		if capacity <= 0 {
			return
		}
		used := 1 - float64(available)/float64(capacity)
		total += math.Max(0, math.Min(1, used))
		dimensions++
	}
	if job.RequiredCPUMillis > 0 {
		add(worker.AvailableCPUMillis, int64(worker.CPUCapacity))
	}
	if job.RequiredMemoryMB > 0 {
		add(worker.AvailableMemoryMB, int64(worker.MemoryCapacityMB))
	}
	if job.RequiredGPUCount > 0 {
		add(worker.AvailableGPUCount, int64(worker.GPUCount))
		gpuMemoryCapacity := int64(worker.GPUCount) * int64(worker.GPUMemoryMB)
		add(worker.AvailableGPUMemoryMB, gpuMemoryCapacity)
	}
	if dimensions == 0 {
		return 0
	}
	return total / float64(dimensions)
}

func normalizedRemainingCapacity(job *model.Job, worker *model.Worker) float64 {
	var total float64
	var dimensions int
	add := func(available, capacity, requested int64) {
		if capacity <= 0 || requested <= 0 {
			return
		}
		remaining := float64(available-requested) / float64(capacity)
		total += math.Max(0, math.Min(1, remaining))
		dimensions++
	}
	add(worker.AvailableCPUMillis, int64(worker.CPUCapacity), int64(job.RequiredCPUMillis))
	add(worker.AvailableMemoryMB, int64(worker.MemoryCapacityMB), int64(job.RequiredMemoryMB))
	add(worker.AvailableGPUCount, int64(worker.GPUCount), int64(job.RequiredGPUCount))
	gpuMemoryCapacity := int64(worker.GPUCount) * int64(worker.GPUMemoryMB)
	gpuMemoryRequested := int64(job.RequiredGPUCount) * int64(job.RequiredGPUMemoryMB)
	add(worker.AvailableGPUMemoryMB, gpuMemoryCapacity, gpuMemoryRequested)
	if dimensions == 0 {
		return 0
	}
	return total / float64(dimensions)
}
