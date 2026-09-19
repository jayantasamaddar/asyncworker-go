---
"asyncworker-go": minor
---

Add `Options.PreHooks` and `Options.PostHooks` for running observer callbacks (logging, metrics, tracing) before and after every task attempt. Hooks run in order and are panic-isolated so a bad hook can't take down a worker or affect the task's own outcome.
