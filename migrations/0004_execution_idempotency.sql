-- Give every logical run a stable identity that survives retries and lease recovery.
ALTER TABLE job_runs ADD COLUMN execution_key UUID;

UPDATE job_runs SET execution_key = gen_random_uuid() WHERE execution_key IS NULL;

ALTER TABLE job_runs
    ALTER COLUMN execution_key SET DEFAULT gen_random_uuid(),
    ALTER COLUMN execution_key SET NOT NULL,
    ADD CONSTRAINT job_runs_execution_key_key UNIQUE (execution_key);

-- A committed ledger row and its handler's database side effects share one
-- transaction. A later attempt can read the prior result without repeating them.
CREATE TABLE execution_idempotency (
    execution_key UUID PRIMARY KEY REFERENCES job_runs(execution_key) ON DELETE CASCADE,
    result        JSONB NOT NULL DEFAULT '{}'::jsonb,
    completed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Small concrete target used by the built-in idempotent_record example handler.
CREATE TABLE idempotent_handler_effects (
    execution_key UUID PRIMARY KEY REFERENCES job_runs(execution_key) ON DELETE CASCADE,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
