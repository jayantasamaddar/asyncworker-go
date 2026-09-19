# asyncworker-go

## 0.2.0

### Minor Changes

- [#6](https://github.com/jayantasamaddar/asyncworker-go/pull/6) [`fc3afaa`](https://github.com/jayantasamaddar/asyncworker-go/commit/fc3afaa44f2e3f4623e16a1a54804ec39a977166) Thanks [@jayantasamaddar](https://github.com/jayantasamaddar)! - Add `Options.PreHooks` and `Options.PostHooks` for running observer callbacks (logging, metrics, tracing) before and after every task attempt. Hooks run in order and are panic-isolated so a bad hook can't take down a worker or affect the task's own outcome.

## 0.1.1

### Patch Changes

- [#5](https://github.com/jayantasamaddar/asyncworker-go/pull/5) [`3f08ff1`](https://github.com/jayantasamaddar/asyncworker-go/commit/3f08ff1702562e169b5a1ad00f23bb19868c741f) Thanks [@jayantasamaddar](https://github.com/jayantasamaddar)! - Fix incorrect module path in `go.mod` — it declared `github.com/zenius-one/nexthis/packages/asyncworker` instead of `github.com/jayantasamaddar/asyncworker-go`.

## 0.1.0

### Minor Changes

- [#3](https://github.com/jayantasamaddar/asyncworker-go/pull/3) [`4694501`](https://github.com/jayantasamaddar/asyncworker-go/commit/4694501e3440f3379a3d7ee631861d4468e8369d) Thanks [@jayantasamaddar](https://github.com/jayantasamaddar)! - `Store.Claim` now takes a lease duration, so a claimed durable task that's never `Complete`d or `Reschedule`d (e.g. the claiming process crashes mid-task) becomes claimable again once its lease expires, instead of being lost. Configure the lease with the new `Options.LeaseDuration` (default 2 minutes). This is a breaking change for custom `Store` implementations: `Claim(ctx, n)` becomes `Claim(ctx, n, leaseFor)`, and implementations must track `Record.LeaseExpiresAt`.

## 0.0.1

### Patch Changes

- [#2](https://github.com/jayantasamaddar/asyncworker-go/pull/2) [`ee19439`](https://github.com/jayantasamaddar/asyncworker-go/commit/ee1943992e11e5154001bf09f9e14c3ea8b3ff21) Thanks [@jayantasamaddar](https://github.com/jayantasamaddar)! - Add Changesets-driven versioning and release automation: merging a PR with a changeset now bumps the version, writes the changelog, tags the release, and publishes a GitHub Release.
