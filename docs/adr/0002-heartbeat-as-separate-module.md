# 2. Heartbeat / lease extension as a separate module

- Status: Proposed (not implemented)
- Date: 2026-09-17
- Related: [ADR 1](0001-lease-based-durable-task-reclaim.md)

## Context

[ADR 1](0001-lease-based-durable-task-reclaim.md) gives every claimed
durable task a fixed lease (`Options.LeaseDuration`, default 2 minutes):
if the claimer dies before `Complete`/`Reschedule`, the record becomes
reclaimable once the lease expires.

This has one accepted weakness: a task that is still legitimately running
when its lease expires can be claimed and executed *concurrently* by a
second claimer. The standard fix for this (as in SQS, or any
visibility-timeout queue) is a **heartbeat**: the claimer periodically
extends its own lease while the task is still in progress, so only a
genuinely dead/stalled claimer's lease ever lapses.

Implementing this was scoped out of ADR 1 because it requires meaningfully
more machinery:

1. A way to renew a lease on a specific record, atomically, *only if the
   caller still owns it* — otherwise a stalled worker (GC pause, network
   partition) could wake up and renew a lease that a second claimer has
   already taken over, undoing the reclaim `Store.Claim`'s contract
   provides.
2. Correctly detecting "still owns it" in general requires a fencing
   token / claim version, not just a time comparison — a naive
   `if lease not yet expired, extend it` check has a race between "check"
   and "extend" across processes.
3. A per-in-flight-task goroutine in `Pool` that ticks at some fraction of
   `LeaseDuration` (e.g. `LeaseDuration/3`) for the duration of the task,
   and stops cleanly on success, failure, or `Pool.Close`.
4. A decision for what happens when a heartbeat fails (lease already lost):
   the task should not proceed to call `Complete`/`Reschedule` for a record
   it no longer owns, and ideally its `ctx` should be cancelled so
   cooperative handlers stop doing redundant/conflicting work.

None of this is needed by every user of this package — most durable tasks
this package targets (webhook delivery, notification sends, cleanup jobs)
are short, retry-capable units of work, for which a sufficiently generous
`LeaseDuration` is enough. `DEVELOPMENT.md` also commits the core module to
being dependency-free and usable unmodified by any Go application; adding
heartbeat machinery unconditionally to `Pool` would add complexity (and
interface surface) that most callers pay for but never use.

## Decision

**Do not add heartbeat/lease-extension to the core `asyncworker` package.**
If/when it's built, ship it as a separate Go module,
`github.com/jayantasamaddar/asyncworker-go/heartbeat`, implemented as a
**`Store` decorator** rather than a `Pool` change:

```go
store := heartbeat.Wrap(realStore, heartbeat.Options{
    Every: 30 * time.Second, // must be < the LeaseDuration passed to Claim
})

pool := asyncworker.New(ctx, asyncworker.Options{
    Store:         store,
    LeaseDuration: 2 * time.Minute,
})
```

Mechanics:

- `heartbeat.Wrap` returns a value implementing `asyncworker.Store` itself,
  so `Pool` requires **zero code changes** — it already only ever calls
  `Store.Claim` / `Complete` / `Reschedule`, all of which the wrapper can
  intercept.
- Its `Claim` delegates to the wrapped store, then — for every record
  returned — starts a goroutine that periodically calls a renew primitive
  on the *underlying concrete store* until the record is resolved or the
  context is cancelled.
- Its `Complete` and `Reschedule` cancel that record's heartbeat goroutine
  *before* delegating to the wrapped store, so no heartbeat ever fires
  after a record is resolved.
- Heartbeat goroutines are derived from the `ctx` passed into `Claim` —
  which is `Pool`'s own internal context — so they're cancelled
  automatically on `Pool.Close` with no extra wiring.
- The renew primitive is **not** part of `asyncworker.Store`. The
  `heartbeat` module defines its own small interface locally (Go permits
  this — interfaces are structural) and type-asserts the wrapped store
  against it, e.g.:

  ```go
  type leaseExtender interface {
      ExtendLease(ctx context.Context, id string, leaseFor time.Duration) error
  }
  ```

  A concrete `Store` implementation (Redis/Postgres/etc.) that wants to
  support heartbeating implements this in addition to `asyncworker.Store`.
  `ExtendLease` is responsible for the atomic "extend only if I still hold
  the lease" check (fencing) — e.g. a Redis Lua script comparing a stored
  claim token, or a SQL `UPDATE ... WHERE id = ? AND lease_expires_at = ?`.
  This mirrors the existing `store.go` convention of putting concurrency
  correctness on the implementer, not the shared interface.
- `MemoryStore` would need a corresponding `ExtendLease` implementation
  (with its own token/version check) for the heartbeat module to be usable
  in tests without a real backing store.

## Consequences

**Positive**

- Zero cost for callers who don't need it: no new dependency, no new
  interface method on `asyncworker.Store`, no behavior change to `Pool` or
  `MemoryStore`.
- Composable: works with any `Store` implementation that adds
  `ExtendLease`, without `asyncworker` knowing the concept exists.
- Keeps the core module's zero-dependency, "usable unmodified by any Go
  application" property intact (per `DEVELOPMENT.md`).

**Negative / open questions**

- **Two Go modules in one repo** is a real but standard pattern
  (`heartbeat/go.mod` requiring a published `asyncworker-go` version,
  independently tagged as `heartbeat/vX.Y.Z`). The existing Changesets
  release pipeline (`release.yml`, root `package.json`) currently assumes
  one versioned artifact and would need to either (a) version the two
  modules in lockstep, or (b) be extended for independent per-module
  versioning/tagging. Not a blocker, but not free either.
- Every concrete `Store` implementation that wants heartbeat support must
  separately implement `ExtendLease` with correct fencing — this is
  meaningfully harder to get right than `Claim`/`Complete`/`Reschedule`,
  and a buggy implementation reintroduces the exact double-execution risk
  heartbeating is meant to close.
- Still doesn't provide exactly-once semantics — it narrows the window for
  concurrent duplicate execution (from "the whole lease" to "clock skew /
  heartbeat interval slop") but doesn't eliminate it.

## Alternatives considered

1. **Heartbeat built into `Pool` and `asyncworker.Store` directly**
   (`Store.Heartbeat` as a required interface method). Rejected: forces
   every `Store` implementer — including ones that never need
   long-running-task safety — to implement it, and couples `Pool` to a
   goroutine-per-task lifecycle unconditionally.
2. **Just recommend a larger `LeaseDuration`.** Still the recommended
   default for most users (see ADR 1); not rejected so much as
   insufficient on its own for callers with genuinely variable or
   long-running task durations, which is the gap this ADR's design closes
   for those who need it.
