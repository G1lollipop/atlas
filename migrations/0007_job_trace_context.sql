-- Persist only W3C Trace Context on jobs and runs so asynchronous stages can
-- continue the originating trace. Baggage is intentionally not stored.
ALTER TABLE jobs
    ADD COLUMN traceparent TEXT NOT NULL DEFAULT '',
    ADD COLUMN tracestate TEXT NOT NULL DEFAULT '';

ALTER TABLE job_runs
    ADD COLUMN traceparent TEXT NOT NULL DEFAULT '',
    ADD COLUMN tracestate TEXT NOT NULL DEFAULT '';
