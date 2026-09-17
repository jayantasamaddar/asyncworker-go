# 1. Lease-based reclaim for durable tasks

- Status: Accepted
- Date: 2026-09-17
- Related: [PR #3](https://github.com/jayantasamaddar/asyncworker-go/pull/3)

## Context

`Pool.Enqueue` persists durable tasks to a `Store` (`Options.Store`), and a
background poller (`pollStore` / `claimDue`) periodically calls
`Store.Claim` to pick up due records and run them through a registered
`Handler`. This is what lets work enqueued before a crash — or by a
different process sharing the same `Store` — get picked up and run.

Before this decision, `Claim` was specified only as "make a claimed record
invisible to concurrent `Claim` calls until `Complete` or `Reschedule`
resolves it." `MemoryStore` implemented this literally: `Claim` deleted the
record from its map, and only `Reschedule` (on failure) or `Complete` (on
success/exhaustion) put a record back into a resolvable state.

This left a gap: **if the process that claimed a record crashes, panics
uncatchably, or is killed (OOM, deploy, node eviction) between `Claim` and
`Complete`/`Reschedule`, the record is gone.** It was never persisted back
by anyone, so it silently vanishes — the exact durability guarantee `Store`
exists to provide is defeated by the one failure mode (a crash mid-task)
that motivated adding a `Store` in the first place.

Two adjacent, distinct concerns were raised alongside this gap and are
addressed separately:

1. **Single-execution across multiple pods/processes sharing one `Store`.**
   This was already covered by the existing `Store` contract (`Claim` must
   make a record invisible to concurrent claimers, e.g. via
   `SELECT ... FOR UPDATE SKIP LOCKED` or a lease). No code gap here — it's
   a correctness requirement on `Store` implementers, unaffected by this
   decision beyond formalizing what "invisible" means (see below).
2. **Namespacing / key-prefixing so multiple independent services can share
   one physical Redis/Postgres instance without collisions.** Deferred —
   `Store` is a caller-implemented interface, so prefixing keys
   (`NewRedisStore(client, "myapp:")`) is already entirely the
   implementer's concern. No framework-level `Namespace` option was added.

## Decision

Change `Claim` from a delete-on-claim operation to a **lease** (SQS-style
visibility timeout):

- `Store.Claim(ctx, n, leaseFor time.Duration) ([]Record, error)` — the
  signature gains a `leaseFor` parameter.
- `Record` gains a `LeaseExpiresAt time.Time` field. `Claim` sets it to
  `time.Now().Add(leaseFor)` for every record it returns; zero means "not
  currently leased."
- A record is claimable when `NextRunAt <= now` **and**
  (`LeaseExpiresAt` is zero **or** `LeaseExpiresAt <= now`).
- `Complete` still deletes the record (terminal state: success, or
  exhausted retries/unregistered handler).
- `Reschedule` clears `LeaseExpiresAt` back to zero when persisting the
  record's new `NextRunAt` after a failed attempt.
- `Options.LeaseDuration` (default **2 minutes**) configures the lease
  passed to every `Claim` call from `pollStore`.
- `MemoryStore` was updated to this model: `Claim` now mutates the record
  in place (sets the lease) instead of deleting it.

Effect: if a claimer dies before resolving a record, no other code path
needs to run — the record simply becomes claimable again, by any Pool
instance sharing the `Store`, once its lease expires. This is a passive,
self-healing mechanism; it requires no heartbeat, no dead-claimer
detection, and no coordination beyond what `Claim`/`Complete`/`Reschedule`
already do.

## Consequences

**Positive**

- Crash mid-task no longer permanently drops a durable task. This closes
  the durability gap that motivated the `Store` abstraction to begin with.
- No new moving parts: no heartbeat goroutine, no extra `Store` method, no
  additional background process. `MemoryStore`'s change is ~10 lines.
- Self-healing: reclaim happens automatically on the next `Claim` call from
  *any* Pool sharing the `Store`, not just the one that lost the record.

**Negative / accepted trade-offs**

- **At-least-once, not exactly-once.** A task that runs longer than its
  lease can be claimed and executed concurrently by a second claimer while
  the first is still legitimately working. Handlers must be idempotent (or
  tolerant of duplicate execution) if this matters — this was already true
  of retries in general, but the lease window makes duplicate concurrent
  execution possible, not just duplicate sequential execution.
- **`LeaseDuration` is a manual trade-off the caller must tune.** Too short
  → false reclaims of still-running tasks (wasted duplicate work, or worse,
  duplicate side effects). Too long → a genuinely crashed task's record sits
  unclaimable for the full lease duration before recovering. The default
  (2 minutes) is a reasonable general-purpose value, not a correct one for
  every workload.
- **Breaking change to the `Store` interface.** Any custom `Store`
  implementation (Redis, Postgres, etc.) must be updated: `Claim(ctx, n)`
  → `Claim(ctx, n, leaseFor)`, and it must track and honor
  `Record.LeaseExpiresAt` the same way `MemoryStore` does. Shipped as a
  `minor` changeset (package is pre-1.0).

## Alternatives considered

1. **Explicit heartbeat / lease extension** — a `Heartbeat`/`ExtendLease`
   call, invoked periodically by `Pool` for every in-flight durable task,
   so a genuinely long-running (but alive) task never gets reclaimed out
   from under it. Rejected *for this decision*, not permanently — see
   [ADR 2](0002-heartbeat-as-separate-module.md) for the fuller design and
   why it's more appropriately an optional add-on than part of the core
   lease mechanism.
2. **No automatic reclaim; require an operator/cron to sweep stale
   claims.** Rejected: reintroduces an external dependency/process this
   package exists to avoid, and defeats "opt in a `Store`, get durability"
   as a self-contained guarantee.
3. **Keep delete-on-claim, rely on `Store` implementations to log
   claimed-but-unresolved records for manual recovery.** Rejected: pushes
   a correctness-critical concern onto every `Store` implementer instead of
   the shared `Claim` contract, and provides no actual automatic recovery.
