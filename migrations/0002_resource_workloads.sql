-- Resource-aware workload scheduling. CPU values are millicores (4000 == 4 cores).
ALTER TABLE jobs
    ADD COLUMN workload_type TEXT NOT NULL DEFAULT 'generic',
    ADD COLUMN required_cpu_millis INT NOT NULL DEFAULT 0,
    ADD COLUMN required_memory_mb INT NOT NULL DEFAULT 0,
    ADD COLUMN required_gpu_count INT NOT NULL DEFAULT 0,
    ADD COLUMN required_gpu_memory_mb INT NOT NULL DEFAULT 0,
    ADD COLUMN required_accelerator TEXT NOT NULL DEFAULT '',
    ADD CONSTRAINT jobs_workload_type_nonempty CHECK (length(btrim(workload_type)) > 0),
    ADD CONSTRAINT jobs_required_cpu_millis_nonnegative CHECK (required_cpu_millis >= 0),
    ADD CONSTRAINT jobs_required_memory_mb_nonnegative CHECK (required_memory_mb >= 0),
    ADD CONSTRAINT jobs_required_gpu_count_nonnegative CHECK (required_gpu_count >= 0),
    ADD CONSTRAINT jobs_required_gpu_memory_mb_nonnegative CHECK (required_gpu_memory_mb >= 0),
    ADD CONSTRAINT jobs_gpu_requirements_need_gpu CHECK (
        required_gpu_count > 0 OR (required_gpu_memory_mb = 0 AND required_accelerator = '')
    );

ALTER TABLE workers RENAME COLUMN last_heartbeat TO last_heartbeat_at;

ALTER TABLE workers
    -- CPU capacity is millicores, matching required_cpu_millis on jobs.
    ADD COLUMN cpu_capacity INT NOT NULL DEFAULT 4000,
    ADD COLUMN memory_capacity_mb INT NOT NULL DEFAULT 8192,
    ADD COLUMN gpu_count INT NOT NULL DEFAULT 0,
    ADD COLUMN gpu_type TEXT NOT NULL DEFAULT '',
    ADD COLUMN gpu_memory_mb INT NOT NULL DEFAULT 0,
    ADD COLUMN labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD CONSTRAINT workers_cpu_capacity_nonnegative CHECK (cpu_capacity >= 0),
    ADD CONSTRAINT workers_memory_capacity_nonnegative CHECK (memory_capacity_mb >= 0),
    ADD CONSTRAINT workers_gpu_count_nonnegative CHECK (gpu_count >= 0),
    ADD CONSTRAINT workers_gpu_memory_nonnegative CHECK (gpu_memory_mb >= 0),
    ADD CONSTRAINT workers_gpu_inventory_requires_gpu CHECK (
        gpu_count > 0 OR (gpu_type = '' AND gpu_memory_mb = 0)
    ),
    ADD CONSTRAINT workers_labels_object CHECK (jsonb_typeof(labels) = 'object');

CREATE INDEX IF NOT EXISTS idx_job_runs_worker_active
    ON job_runs (leased_by)
    WHERE status IN ('leased', 'running');
