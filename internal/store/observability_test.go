package store

import (
	"testing"
	"time"

	"github.com/G1lollipop/atlas/internal/model"
	"github.com/google/uuid"
)

func createObservabilityJob(t *testing.T, st *PostgresStore, queue string, cronExpr *string, cpu, memory, gpu int32) *model.Job {
	t.Helper()
	job, err := st.CreateJob(t.Context(), model.NewJobInput{
		Name:                "observability-" + uuid.NewString(),
		CronExpr:            cronExpr,
		Queue:               queue,
		RequiredCPUMillis:   cpu,
		RequiredMemoryMB:    memory,
		RequiredGPUCount:    gpu,
		RequiredGPUMemoryMB: 0,
	})
	if err != nil {
		t.Fatalf("create observability job: %v", err)
	}
	return job
}

func createObservabilityRun(t *testing.T, st *PostgresStore, job *model.Job, scheduledAt time.Time) *model.JobRun {
	t.Helper()
	run, err := st.CreateRun(t.Context(), job.ID, job.Priority, scheduledAt)
	if err != nil {
		t.Fatalf("create observability run: %v", err)
	}
	return run
}

func TestObservabilitySnapshotCountsDueQueueAndCurrentReservations(t *testing.T) {
	st := cancellationIntegrationStore(t)
	ctx := t.Context()
	cron := "* * * * *"
	now := time.Now()

	createObservabilityJob(t, st, "observability", nil, 250, 256, 0) // Accepted one-shot, not yet promoted.
	cpuRecurring := createObservabilityJob(t, st, "observability", &cron, 500, 512, 0)
	cpuScheduled := createObservabilityRun(t, st, cpuRecurring, now.Add(-time.Minute))
	if _, err := st.pool.Exec(ctx, `UPDATE job_runs SET status = 'scheduled' WHERE id = $1`, cpuScheduled.ID); err != nil {
		t.Fatalf("mark CPU run due: %v", err)
	}

	delayedRetryJob := createObservabilityJob(t, st, "observability", &cron, 9000, 1024, 0)
	_ = createObservabilityRun(t, st, delayedRetryJob, now.Add(time.Hour))

	pausedJob := createObservabilityJob(t, st, "paused-queue", &cron, 100, 100, 0)
	pausedRun := createObservabilityRun(t, st, pausedJob, now.Add(-time.Minute))
	if _, err := st.pool.Exec(ctx, `UPDATE job_runs SET status = 'scheduled' WHERE id = $1`, pausedRun.ID); err != nil {
		t.Fatalf("mark paused run due: %v", err)
	}
	if _, err := st.pool.Exec(ctx, `UPDATE jobs SET status = 'paused' WHERE id = $1`, pausedJob.ID); err != nil {
		t.Fatalf("pause job: %v", err)
	}

	archivedJob := createObservabilityJob(t, st, "archived-queue", &cron, 100, 100, 0)
	archivedRun := createObservabilityRun(t, st, archivedJob, now.Add(-time.Minute))
	if _, err := st.pool.Exec(ctx, `UPDATE job_runs SET status = 'scheduled' WHERE id = $1`, archivedRun.ID); err != nil {
		t.Fatalf("mark archived run due: %v", err)
	}
	if _, err := st.pool.Exec(ctx, `UPDATE jobs SET status = 'archived' WHERE id = $1`, archivedJob.ID); err != nil {
		t.Fatalf("archive job: %v", err)
	}

	quietJob := createObservabilityJob(t, st, "quiet-history", &cron, 100, 100, 0)
	quietRun := createObservabilityRun(t, st, quietJob, now.Add(-time.Minute))
	if _, err := st.pool.Exec(ctx, `
		UPDATE job_runs SET status = 'succeeded', finished_at = now() WHERE id = $1
	`, quietRun.ID); err != nil {
		t.Fatalf("complete historical run: %v", err)
	}

	liveWorker := model.Worker{
		ID: "metrics-live-" + uuid.NewString(), Status: model.WorkerStatusAlive,
		CPUCapacity: 2000, MemoryCapacityMB: 4096, GPUCount: 2,
	}
	if err := st.UpsertWorkerHeartbeat(ctx, liveWorker); err != nil {
		t.Fatalf("upsert live worker: %v", err)
	}
	gpuAssignedJob := createObservabilityJob(t, st, "observability", &cron, 250, 512, 1)
	gpuAssigned := createObservabilityRun(t, st, gpuAssignedJob, now.Add(-time.Minute))
	if _, err := st.pool.Exec(ctx, `
		UPDATE job_runs SET status = 'assigned', assigned_worker_id = $1,
		    assignment_expires_at = now() + INTERVAL '1 minute'
		WHERE id = $2
	`, liveWorker.ID, gpuAssigned.ID); err != nil {
		t.Fatalf("assign GPU run: %v", err)
	}

	cpuRunningJob := createObservabilityJob(t, st, "observability", &cron, 500, 1024, 0)
	cpuRunning := createObservabilityRun(t, st, cpuRunningJob, now.Add(-time.Minute))
	if _, err := st.pool.Exec(ctx, `
		UPDATE job_runs SET status = 'running', attempt = 1, leased_by = $1,
		    lease_expires_at = now() + INTERVAL '1 minute'
		WHERE id = $2
	`, liveWorker.ID, cpuRunning.ID); err != nil {
		t.Fatalf("start CPU run: %v", err)
	}

	expiredJob := createObservabilityJob(t, st, "observability", &cron, 7000, 7000, 0)
	expiredRun := createObservabilityRun(t, st, expiredJob, now.Add(-time.Minute))
	if _, err := st.pool.Exec(ctx, `
		UPDATE job_runs SET status = 'running', attempt = 1, leased_by = $1,
		    lease_expires_at = now() - INTERVAL '1 minute'
		WHERE id = $2
	`, liveWorker.ID, expiredRun.ID); err != nil {
		t.Fatalf("expire CPU run lease: %v", err)
	}

	expiredAssignmentJob := createObservabilityJob(t, st, "observability", &cron, 1000, 1200, 0)
	expiredAssignment := createObservabilityRun(t, st, expiredAssignmentJob, now.Add(-time.Minute))
	if _, err := st.pool.Exec(ctx, `
		UPDATE job_runs SET status = 'assigned', assigned_worker_id = $1,
		    assignment_expires_at = now() - INTERVAL '1 minute'
		WHERE id = $2
	`, liveWorker.ID, expiredAssignment.ID); err != nil {
		t.Fatalf("expire assignment: %v", err)
	}

	staleWorker := model.Worker{
		ID: "metrics-stale-" + uuid.NewString(), Status: model.WorkerStatusAlive,
		CPUCapacity: 1000, MemoryCapacityMB: 1000,
	}
	if err := st.UpsertWorkerHeartbeat(ctx, staleWorker); err != nil {
		t.Fatalf("upsert stale worker: %v", err)
	}
	staleJob := createObservabilityJob(t, st, "stale-queue", &cron, 400, 200, 0)
	staleRun := createObservabilityRun(t, st, staleJob, now.Add(-time.Minute))
	if _, err := st.pool.Exec(ctx, `UPDATE workers SET last_heartbeat_at = now() - INTERVAL '2 minutes' WHERE id = $1`, staleWorker.ID); err != nil {
		t.Fatalf("expire stale worker heartbeat: %v", err)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE job_runs SET status = 'assigned', assigned_worker_id = $1,
		    assignment_expires_at = now() + INTERVAL '1 minute'
		WHERE id = $2
	`, staleWorker.ID, staleRun.ID); err != nil {
		t.Fatalf("seed stale worker reservation: %v", err)
	}
	ancientWorker := model.Worker{
		ID: "metrics-ancient-" + uuid.NewString(), Status: model.WorkerStatusAlive,
		CPUCapacity: 1000, MemoryCapacityMB: 1000,
	}
	if err := st.UpsertWorkerHeartbeat(ctx, ancientWorker); err != nil {
		t.Fatalf("upsert ancient worker: %v", err)
	}
	if _, err := st.pool.Exec(ctx, `
		UPDATE workers SET last_heartbeat_at = now() - ($2::bigint * INTERVAL '1 millisecond')
		WHERE id = $1
	`, ancientWorker.ID, durationMilliseconds(20*30*time.Second)); err != nil {
		t.Fatalf("age ancient worker heartbeat: %v", err)
	}

	beforeReclaim, err := st.ObservabilitySnapshot(ctx, 30*time.Second)
	if err != nil {
		t.Fatalf("read pre-reclaim observability snapshot: %v", err)
	}
	var beforeLive model.WorkerReservationSnapshot
	for _, worker := range beforeReclaim.Workers {
		if worker.WorkerID == liveWorker.ID {
			beforeLive = worker
			break
		}
	}
	if beforeLive.CPUReservedMillis != 8750 || beforeLive.MemoryReservedMB != 9736 {
		t.Fatalf("pre-reclaim reservation = %+v, want expired lease and assignment retained", beforeLive)
	}

	reclaimedLeases, err := st.ReclaimExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("reclaim expired lease: %v", err)
	}
	if reclaimedLeases != 1 {
		t.Fatalf("reclaimed leases = %d, want 1", reclaimedLeases)
	}
	afterLeaseReclaim, err := st.ObservabilitySnapshot(ctx, 30*time.Second)
	if err != nil {
		t.Fatalf("read post-lease-reclaim snapshot: %v", err)
	}
	for _, worker := range afterLeaseReclaim.Workers {
		if worker.WorkerID == liveWorker.ID && (worker.CPUReservedMillis != 1750 || worker.MemoryReservedMB != 2736) {
			t.Fatalf("reservation after lease reclaim = %+v, want expired assignment only", worker)
		}
	}

	expiredAssignments, err := st.RequeueExpiredAssignments(ctx)
	if err != nil {
		t.Fatalf("requeue expired assignment: %v", err)
	}
	if expiredAssignments != 1 {
		t.Fatalf("requeued assignments = %d, want 1", expiredAssignments)
	}
	snapshot, err := st.ObservabilitySnapshot(ctx, 30*time.Second)
	if err != nil {
		t.Fatalf("read post-reclaim observability snapshot: %v", err)
	}

	depths := make(map[string]int64, len(snapshot.QueueDepths))
	for _, depth := range snapshot.QueueDepths {
		depths[depth.Queue+"/"+depth.ResourceClass] = depth.Depth
	}
	if got := depths["observability/gpu"]; got != 1 {
		t.Errorf("GPU backlog = %d, want 1 assigned run", got)
	}
	if got := depths["paused-queue/cpu"]; got != 1 {
		t.Errorf("paused queue backlog = %d, want 1 materialized due run", got)
	}
	if got := depths["archived-queue/cpu"]; got != 1 {
		t.Errorf("archived queue backlog = %d, want 1 materialized due run", got)
	}
	if got := depths["observability/cpu"]; got != 4 {
		t.Errorf("CPU backlog after reclaim = %d, want 4 due/unpromoted runs", got)
	}
	if got := depths["stale-queue/cpu"]; got != 1 {
		t.Errorf("stale worker assigned backlog = %d, want 1", got)
	}
	if _, ok := depths["quiet-history/cpu"]; ok {
		t.Errorf("queue with only completed historical work remains in the snapshot")
	}

	workers := make(map[string]model.WorkerReservationSnapshot, len(snapshot.Workers))
	for _, worker := range snapshot.Workers {
		workers[worker.WorkerID] = worker
	}
	live := workers[liveWorker.ID]
	if !live.Alive || live.CPUReservedMillis != 750 || live.MemoryReservedMB != 1536 || live.GPUReservedCount != 1 {
		t.Errorf("live worker reservation = %+v, want active leases/assignments only", live)
	}
	stale := workers[staleWorker.ID]
	if stale.Alive || stale.CPUReservedMillis != 0 || stale.MemoryReservedMB != 0 || stale.GPUReservedCount != 0 {
		t.Errorf("stale worker reservation = %+v, want dead with zero reservations", stale)
	}
	if _, ok := workers[ancientWorker.ID]; ok {
		t.Errorf("ancient worker series remains after twenty heartbeat TTLs")
	}
}

func TestObservabilitySnapshotEmitsZeroSentinelsWhenNoQueuesExist(t *testing.T) {
	st := cancellationIntegrationStore(t)
	snapshot, err := st.ObservabilitySnapshot(t.Context(), 30*time.Second)
	if err != nil {
		t.Fatalf("read empty observability snapshot: %v", err)
	}
	if len(snapshot.QueueDepths) != 2 {
		t.Fatalf("empty queue depth rows = %d, want cpu and gpu zero sentinels", len(snapshot.QueueDepths))
	}
	classes := make(map[string]int64, len(snapshot.QueueDepths))
	for _, depth := range snapshot.QueueDepths {
		if depth.Queue != "" || depth.Depth != 0 {
			t.Errorf("empty queue sentinel = %+v, want queue empty and depth zero", depth)
		}
		classes[depth.ResourceClass] = depth.Depth
	}
	if _, ok := classes["cpu"]; !ok {
		t.Errorf("empty snapshot has no cpu zero series")
	}
	if _, ok := classes["gpu"]; !ok {
		t.Errorf("empty snapshot has no gpu zero series")
	}
}
