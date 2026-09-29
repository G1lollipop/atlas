-- Queue names classify work for capacity controls and observability. Worker placement
-- continues to depend on advertised CPU/memory/GPU capabilities, not a queue mapping.
ALTER TABLE jobs
    ADD COLUMN queue TEXT NOT NULL DEFAULT 'cpu-default',
    ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'legacy',
    ADD CONSTRAINT jobs_queue_nonempty CHECK (length(btrim(queue)) > 0),
    ADD CONSTRAINT jobs_queue_max_length CHECK (octet_length(queue) <= 128),
    ADD CONSTRAINT jobs_tenant_id_nonempty CHECK (length(btrim(tenant_id)) > 0),
    ADD CONSTRAINT jobs_tenant_id_max_length CHECK (octet_length(tenant_id) <= 128);

-- These indexes keep the backlog count's two sources inexpensive: outstanding runs,
-- plus accepted one-shot jobs that have not yet been materialized as a run.
CREATE INDEX IF NOT EXISTS idx_jobs_unpromoted_queue_tenant
    ON jobs (queue, tenant_id)
    WHERE cron_expr IS NULL AND status IN ('active', 'paused');

CREATE INDEX IF NOT EXISTS idx_job_runs_active_backpressure
    ON job_runs (job_id)
    WHERE status IN ('queued', 'scheduled', 'assigned', 'leased', 'running');
