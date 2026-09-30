# ADR 0005: PostgreSQL election and the etcd migration boundary

Status: accepted. Etcd implementation deferred.

## Context

Schedulers already require PostgreSQL to read work and commit placement. A
separate coordination service adds an operational and failure relationship.

## Decision

Retain a dedicated-session advisory lock behind a leader-elector contract.
Campaign and leadership observation are distinct from release. Return or
discard the checked-out connection on shutdown, including losing contenders.
A dead session releases the lock, allowing a standby to resume scheduling.

## If election moved to etcd

Campaign under an etcd lease-backed election key, watch session loss, cancel
leader work on loss, and resign on orderly shutdown. An election revision can
identify a term. Database writes would still need an enforced term/fencing
protocol to stop stale former leaders. `IsLeader` is a local observation, not
an authorization token.

Etcd can elect during a PostgreSQL outage, but Atlas still cannot commit work
without PostgreSQL. Its separate coordination plane is useful only if new
requirements justify that operational cost. Row-level ownership guards remain
necessary with either backend.

## Verification

The process experiment SIGKILLs the leader, observes a standby win, and submits
work the standby promotes and completes. Transport-fault recovery is tested
separately. These do not prove partition safety for an unimplemented backend.

References: [PostgreSQL advisory locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS),
[etcd election API](https://etcd.io/docs/v3.6/dev-guide/api_concurrency_reference_v3/).
