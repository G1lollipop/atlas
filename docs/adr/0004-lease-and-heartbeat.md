# ADR 0004: Lease and heartbeat are separate signals

Status: accepted.

## Context

Freshness answers whether a worker accepts placement. A lease answers whether
one attempt owns a run. A healthy process may contain a stalled handler, and a
long job may exceed its initial lease.

## Decision

Heartbeat capability and placement status independently of per-run renewal.
Renew periodically while executing. Scope renewal and finalization to owner,
attempt, status, and non-expired lease. Failed renewal means uncertain ownership:
stop finalization and let transactional reclaim recover expired work.

## Consequences

Long work retains ownership; crashes recover without release messages. Short
leases recover faster but tolerate less database outage or runtime pause.
Heartbeat TTL and lease duration require separate tuning.

A draining worker receives no new assignments but keeps renewing current work
within shutdown grace. After grace it stops renewal; recovery waits for expiry
instead of immediately granting ownership while an old handler might still run.
