-- Job cancellation is durable so API and worker processes can coordinate across replicas.
ALTER TABLE job_runs ADD COLUMN cancel_requested_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_job_runs_cancel_requested
    ON job_runs (lease_expires_at)
    WHERE cancel_requested_at IS NOT NULL AND status IN ('leased', 'running');
