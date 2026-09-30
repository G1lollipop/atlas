# ADR 0002: Scheduler placement with worker pull execution

Status: accepted.

## Context

Workers pulling arbitrary jobs cannot express a fleet-wide placement decision.
Direct push adds worker endpoints, acknowledgements, and ambiguous network
delivery outcomes.

## Decision

Use hybrid scheduling. The scheduler selects a compatible worker and commits
`assigned_worker_id` plus its reservation. A worker with a free slot claims only
its own assignment. Assignment expiration recovers work never started by its
designated worker.

## Consequences

Placement strategies can be compared independently of handlers. Workers need
no inbound scheduling endpoint, and durable state handles acknowledgement and
recovery. Polling costs reads and latency. Assigning ahead of execution can hold
idle reservations until expiry.

Queue names classify work rather than select machines. CPU work can use GPU
worker CPU; class-based autoscalers therefore use an approximate demand signal.
A future push notification can reduce polling while retaining the durable
assignment protocol. Notifications alone must not authorize execution.
