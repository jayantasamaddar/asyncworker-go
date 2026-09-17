---
"asyncworker-go": minor
---

`Store.Claim` now takes a lease duration, so a claimed durable task that's never `Complete`d or `Reschedule`d (e.g. the claiming process crashes mid-task) becomes claimable again once its lease expires, instead of being lost. Configure the lease with the new `Options.LeaseDuration` (default 2 minutes). This is a breaking change for custom `Store` implementations: `Claim(ctx, n)` becomes `Claim(ctx, n, leaseFor)`, and implementations must track `Record.LeaseExpiresAt`.
