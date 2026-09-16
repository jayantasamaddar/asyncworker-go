# asyncworker

A small, dependency-free async task runner for Go with retry and exponential backoff with opt-in pluggable persistent store support for persisting and resuming tasks.

By default it's purely in-memory — nothing written to disk or a database, no delivery guarantee across process restarts. It exists for the common case where you want to fire off background work (with retries) from inside a single process, without standing up something like Temporal.

Persistence is opt-in, via a `Store` interface (see below): plug in Redis, Postgres, or anything else to survive a process restart. Leave it unset and you get the original non-persisting, in-memory-only behavior.

---

## Usage

```go
pool := asyncworker.New(ctx, asyncworker.Options{
    Workers:     4,
    QueueSize:   256,
    RetryPolicy: asyncworker.DefaultRetryPolicy(), // 5 attempts, 200ms..30s exponential backoff, 20% jitter
    OnError: func(id string, attempt int, err error) {
        log.Printf("task %s attempt %d failed: %v", id, attempt, err)
    },
    OnExhausted: func(id string, attempts int, err error) {
        log.Printf("task %s gave up after %d attempts: %v", id, attempts, err)
    },
})
pool.Start()
defer pool.Close() // cancels in-flight tasks' context and drains workers/retries

id, err := pool.Submit(func(ctx context.Context) error {
    return sendWebhook(ctx, payload)
})
```

### Per-task overrides

```go
pool.Submit(task,
    asyncworker.WithID("webhook-42"),
    asyncworker.WithRetryPolicy(asyncworker.NoRetry()),
)
```

### Semantics

- `Submit` never blocks: it returns `ErrQueueFull` if the queue is at
  capacity, or `ErrPoolClosed` once `Close` has been called.
- Retries are scheduled with a timer (not by holding a worker) so a slow
  backoff on one task never starves the others.
- A panicking task is recovered and treated as a failed attempt.
- `Close` cancels the pool's context (visible to every running/future task
  via the `ctx` argument), then waits for in-flight tasks and pending retry
  timers to finish. Tasks still sitting in the queue buffer that no worker
  has picked up yet may be dropped — this is a best-effort, non-persisting
  queue, not a durable one.

## Opt-in persistence

A `Task` is a Go closure — it can't be serialized, so `Submit` is always
in-memory only, Store or no Store. To get durability, submit a **named,
payload-based** task instead, and register the code that runs it:

```go
type Store interface {
    Enqueue(ctx context.Context, rec Record) error
    Claim(ctx context.Context, n int) ([]Record, error)
    Reschedule(ctx context.Context, rec Record) error
    Complete(ctx context.Context, id string) error
}
```

```go
pool := asyncworker.New(ctx, asyncworker.Options{
    Workers: 4,
    Store:   myRedisStore, // implements asyncworker.Store; nil defaults to asyncworker.NewMemoryStore()
})

pool.Register("send-webhook", func(ctx context.Context, payload []byte) error {
    var req webhookRequest
    if err := json.Unmarshal(payload, &req); err != nil {
        return err // permanent decode errors will still burn retries; validate before Enqueue if that matters
    }
    return sendWebhook(ctx, req)
})
pool.Start()

payload, _ := json.Marshal(webhookRequest{URL: "https://example.com"})
id, err := pool.Enqueue(ctx, "send-webhook", payload)
```

- A background poller (tune the interval with `Options.PollInterval`,
  default 200ms) claims due records from the `Store` and runs them through
  the matching registered `Handler`. This is what lets work enqueued before
  a crash — or by a different process sharing the same `Store` — get picked
  up and run.
- A task `Enqueue`'d under a type with no registered `Handler` fails
  permanently (`UnregisteredHandlerError`), without burning retries.
- `MemoryStore` (the default when `Options.Store` is nil) is a real,
  concurrency-safe implementation of `Store` — it's just in-process and
  non-persisting, so it behaves exactly like the original queue. Implement
  `Store` yourself against Redis, Postgres, SQS, etc. for actual durability;
  see the interface doc comment in `store.go` for the concurrency
  guarantees (e.g. lease/visibility-timeout on `Claim`) a real
  implementation needs.
