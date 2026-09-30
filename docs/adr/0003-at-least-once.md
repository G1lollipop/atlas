# ADR 0003: At-least-once attempts with explicit idempotency

Status: accepted.

## Context

A worker can perform an external effect and die before recording success.
PostgreSQL cannot know whether that effect happened. Refusing recovery loses
work; retrying may repeat the effect.

## Decision

Guarantee at-least-once attempts, not exactly-once effects. Preserve each run's
`execution_key` across automatic and manual retries. Expose it to handlers and
an `IdempotencyStore` abstraction. Fence lifecycle writes by owner, attempt,
status, and a live lease.

## Consequences

Recovery retries ownership and may repeat handler invocation. The destination
needs an atomic boundary: a unique key with a business write in one transaction,
an API idempotency key, or an outbox/inbox protocol. A separate processed marker
before or after a non-transactional effect does not close the crash window.

The side-effect-before-completion chaos scenario observes a second attempt
with the same key and one protected effect. This demonstrates that example's
boundary, not exactly-once arbitrary handlers. Lease expiry can permit overlap
if a stale handler ignores cancellation or loses connectivity.
