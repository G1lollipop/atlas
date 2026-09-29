-- Introduce a durable scheduler assignment stage between run creation and leasing.
-- Existing pending rows are unassigned work and therefore enter the queued state.
ALTER TABLE job_runs
    ALTER COLUMN status SET DEFAULT 'queued',
    ADD COLUMN assigned_worker_id TEXT REFERENCES workers(id) ON DELETE SET NULL,
    ADD COLUMN assigned_at TIMESTAMPTZ,
    ADD COLUMN assignment_expires_at TIMESTAMPTZ;

UPDATE job_runs SET status = 'queued' WHERE status = 'pending';

DROP INDEX IF EXISTS idx_job_runs_lease_queue;

CREATE INDEX IF NOT EXISTS idx_job_runs_scheduled_queue
    ON job_runs (priority DESC, scheduled_at, created_at)
    WHERE status = 'scheduled';

CREATE INDEX IF NOT EXISTS idx_job_runs_assigned_worker_active
    ON job_runs (assigned_worker_id, assignment_expires_at)
    WHERE status = 'assigned';

CREATE INDEX IF NOT EXISTS idx_job_runs_assignment_expiry
    ON job_runs (assignment_expires_at)
    WHERE status = 'assigned';

-- The existing idx_job_runs_worker_active index covers leased/running reservations.
