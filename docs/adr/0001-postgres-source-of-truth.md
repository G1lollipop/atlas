# ADR 0001: PostgreSQL remains the source of truth

Status: accepted.

## Context

Placement depends on inventory and active reservations. Granting a run must
change durable ownership without overspending capacity. Adding a queue broker
would require a protocol to keep broker delivery and run state consistent.

## Decision

Keep jobs, capabilities, assignments, leases, and retry timing in PostgreSQL.
Derive reservations from active ownership rows. Lock the worker and run in the
same assignment transaction; revalidate ownership during claiming and
finalization. Neither Redis nor a read replica authorizes execution.

## Consequences

There is one transactional scheduling boundary and a small operational
footprint. The write path also has a shared ceiling: admission, assignment,
claiming, renewal, and completion all consume database capacity. Extra workers
may increase contention instead of throughput. Keyset paging avoids offset
cost but does not eliminate per-run transactions.

If measured capacity becomes insufficient, partitioning, batching, or an
outbox-backed broker needs a new ownership design and failure experiments.
A broker is not a configuration replacement for these transactions.

Reference: [PostgreSQL locking](https://www.postgresql.org/docs/current/explicit-locking.html).
