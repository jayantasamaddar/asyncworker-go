# asyncworker-go

## 0.1.0

### Minor Changes

- [#3](https://github.com/jayantasamaddar/asyncworker-go/pull/3) [`4694501`](https://github.com/jayantasamaddar/asyncworker-go/commit/4694501e3440f3379a3d7ee631861d4468e8369d) Thanks [@jayantasamaddar](https://github.com/jayantasamaddar)! - `Store.Claim` now takes a lease duration, so a claimed durable task that's never `Complete`d or `Reschedule`d (e.g. the claiming process crashes mid-task) becomes claimable again once its lease expires, instead of being lost. Configure the lease with the new `Options.LeaseDuration` (default 2 minutes). This is a breaking change for custom `Store` implementations: `Claim(ctx, n)` becomes `Claim(ctx, n, leaseFor)`, and implementations must track `Record.LeaseExpiresAt`.

## 0.0.1

### Patch Changes

- [#2](https://github.com/jayantasamaddar/asyncworker-go/pull/2) [`ee19439`](https://github.com/jayantasamaddar/asyncworker-go/commit/ee1943992e11e5154001bf09f9e14c3ea8b3ff21) Thanks [@jayantasamaddar](https://github.com/jayantasamaddar)! - Add Changesets-driven versioning and release automation: merging a PR with a changeset now bumps the version, writes the changelog, tags the release, and publishes a GitHub Release.
