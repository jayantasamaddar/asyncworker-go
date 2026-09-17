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

---

## Opt-in persistence

A `Task` is a Go closure — it can't be serialized, so `Submit` is always
in-memory only, Store or no Store. To get durability, submit a **named,
payload-based** task instead, and register the code that runs it:

```go
type Store interface {
    Enqueue(ctx context.Context, rec Record) error
    Claim(ctx context.Context, n int, leaseFor time.Duration) ([]Record, error)
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

- A background poller (tune the interval with `Options.PollInterval`, default 200ms) claims due records from the `Store` and runs them through the matching registered `Handler`. This is what lets work enqueued before a crash — or by a different process sharing the same `Store` — get picked up and run.
- A task `Enqueue`'d under a type with no registered `Handler` fails permanently (`UnregisteredHandlerError`), without burning retries.
- Claiming a record is a lease, not a delete: it stays invisible to other claimers only until `Options.LeaseDuration` (default 2 minutes) elapses.

  If the claiming process crashes before calling `Complete` or `Reschedule`, the record becomes claimable again once its lease expires, instead of being lost. Pick a `LeaseDuration` comfortably longer than the task normally takes — a task that outlives its lease can be claimed and run again concurrently.

- `MemoryStore` (the default when `Options.Store` is nil) is a real, concurrency-safe implementation of `Store` — it's just in-process and non-persisting, so it behaves exactly like the original queue. Implement `Store` yourself against Redis, Postgres, SQS, etc. for actual durability; see the interface doc comment in `store.go` for the concurrency guarantees (e.g. lease/visibility-timeout on `Claim`) a real implementation needs.

---

## Examples

Two reference `Store` implementations below — Redis and Postgres — both satisfying the concurrency contract in `store.go`: `Claim` must be atomic across concurrent callers (even from different processes sharing the same backing store), and a claimed record must stay invisible only until its lease expires.

### Redis (`redis/go-redis/v9`)

One sorted set holds claim ordering (score = next-claimable-at, in UnixNano); one string key per record holds its JSON-encoded payload. `Claim` is a single Lua script, so "pop up to `n` due IDs and push their claim-until score forward" is atomic even with multiple `Pool`s sharing the same Redis.

```go
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	asyncworker "github.com/jayantasamaddar/asyncworker-go"
)

// RedisStore is an asyncworker.Store backed by Redis. Safe for multiple
// Pool instances/processes to share the same client/keyspace.
type RedisStore struct {
	client redis.UniversalClient
	prefix string // e.g. "myapp:asyncworker:" — namespace this store's keys yourself
}

func NewRedisStore(client redis.UniversalClient, prefix string) *RedisStore {
	return &RedisStore{client: client, prefix: prefix}
}

func (s *RedisStore) queueKey() string           { return s.prefix + "queue" }
func (s *RedisStore) recordKey(id string) string { return s.prefix + "record:" + id }

func (s *RedisStore) Enqueue(ctx context.Context, rec asyncworker.Record) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}
	_, err = s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, s.recordKey(rec.ID), data, 0)
		pipe.ZAdd(ctx, s.queueKey(), redis.Z{
			Score:  float64(rec.NextRunAt.UnixNano()),
			Member: rec.ID,
		})
		return nil
	})
	return err
}

// claimScript atomically pops up to n IDs due by ARGV[1] (now) and pushes
// their score forward to ARGV[3] (now+leaseFor), so they're invisible to
// concurrent Claim calls until the lease expires.
var claimScript = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, id in ipairs(ids) do
    redis.call('ZADD', KEYS[1], ARGV[3], id)
end
return ids
`)

func (s *RedisStore) Claim(ctx context.Context, n int, leaseFor time.Duration) ([]asyncworker.Record, error) {
	now := time.Now()
	leaseUntil := now.Add(leaseFor)

	ids, err := claimScript.Run(ctx, s.client, []string{s.queueKey()},
		now.UnixNano(), n, leaseUntil.UnixNano(),
	).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = s.recordKey(id)
	}
	vals, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("claim: fetch records: %w", err)
	}

	recs := make([]asyncworker.Record, 0, len(vals))
	for _, v := range vals {
		str, ok := v.(string)
		if !ok {
			continue // deleted between ZRANGEBYSCORE and MGET (e.g. raced with a Complete)
		}
		var rec asyncworker.Record
		if err := json.Unmarshal([]byte(str), &rec); err != nil {
			return nil, fmt.Errorf("claim: unmarshal record: %w", err)
		}
		rec.LeaseExpiresAt = leaseUntil
		recs = append(recs, rec)
	}
	return recs, nil
}

func (s *RedisStore) Reschedule(ctx context.Context, rec asyncworker.Record) error {
	rec.LeaseExpiresAt = time.Time{}
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}
	_, err = s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, s.recordKey(rec.ID), data, 0)
		pipe.ZAdd(ctx, s.queueKey(), redis.Z{
			Score:  float64(rec.NextRunAt.UnixNano()),
			Member: rec.ID,
		})
		return nil
	})
	return err
}

func (s *RedisStore) Complete(ctx context.Context, id string) error {
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZRem(ctx, s.queueKey(), id)
		pipe.Del(ctx, s.recordKey(id))
		return nil
	})
	return err
}
```

```go
client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

pool := asyncworker.New(ctx, asyncworker.Options{
    Workers:       4,
    Store:         store.NewRedisStore(client, "myapp:asyncworker:"),
    LeaseDuration: 2 * time.Minute,
})
```

### Postgres (`jackc/pgx/v5`)

One table, claimed via `SELECT ... FOR UPDATE SKIP LOCKED` inside a transaction — the pattern the `Store` doc comment calls out directly, and what makes `Claim` safe with multiple `Pool`s (even in different processes) hitting the same table concurrently.

```sql
CREATE TABLE asyncworker_tasks (
    id               TEXT PRIMARY KEY,
    type             TEXT NOT NULL,
    payload          BYTEA,
    policy           JSONB NOT NULL,
    attempt          INT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL,
    next_run_at      TIMESTAMPTZ NOT NULL,
    last_error       TEXT,
    lease_expires_at TIMESTAMPTZ
);

CREATE INDEX asyncworker_tasks_claim_idx ON asyncworker_tasks (next_run_at, lease_expires_at);
```

```go
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	asyncworker "github.com/jayantasamaddar/asyncworker-go"
)

// PostgresStore is an asyncworker.Store backed by Postgres. Safe for
// multiple Pool instances/processes to share the same table.
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

func (s *PostgresStore) Enqueue(ctx context.Context, rec asyncworker.Record) error {
	policy, err := json.Marshal(rec.Policy)
	if err != nil {
		return fmt.Errorf("marshal policy: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
        INSERT INTO asyncworker_tasks
            (id, type, payload, policy, attempt, created_at, next_run_at, last_error)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (id) DO NOTHING
    `, rec.ID, rec.Type, rec.Payload, policy, rec.Attempt, rec.CreatedAt, rec.NextRunAt, rec.LastError)
	return err
}

func (s *PostgresStore) Claim(ctx context.Context, n int, leaseFor time.Duration) ([]asyncworker.Record, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) // no-op once committed below

	rows, err := tx.Query(ctx, `
        SELECT id, type, payload, policy, attempt, created_at, next_run_at, last_error
        FROM asyncworker_tasks
        WHERE next_run_at <= now()
          AND (lease_expires_at IS NULL OR lease_expires_at <= now())
        ORDER BY next_run_at
        LIMIT $1
        FOR UPDATE SKIP LOCKED
    `, n)
	if err != nil {
		return nil, fmt.Errorf("select due: %w", err)
	}

	leaseUntil := time.Now().Add(leaseFor)
	var (
		recs []asyncworker.Record
		ids  []string
	)
	for rows.Next() {
		var (
			rec        asyncworker.Record
			policyJSON []byte
		)
		if err := rows.Scan(&rec.ID, &rec.Type, &rec.Payload, &policyJSON, &rec.Attempt,
			&rec.CreatedAt, &rec.NextRunAt, &rec.LastError); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan: %w", err)
		}
		if err := json.Unmarshal(policyJSON, &rec.Policy); err != nil {
			rows.Close()
			return nil, fmt.Errorf("unmarshal policy: %w", err)
		}
		rec.LeaseExpiresAt = leaseUntil
		recs = append(recs, rec)
		ids = append(ids, rec.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	if len(ids) == 0 {
		return nil, tx.Commit(ctx)
	}

	if _, err := tx.Exec(ctx, `
        UPDATE asyncworker_tasks SET lease_expires_at = $1 WHERE id = ANY($2)
    `, leaseUntil, ids); err != nil {
		return nil, fmt.Errorf("lease: %w", err)
	}

	return recs, tx.Commit(ctx)
}

func (s *PostgresStore) Reschedule(ctx context.Context, rec asyncworker.Record) error {
	_, err := s.pool.Exec(ctx, `
        UPDATE asyncworker_tasks
        SET attempt = $2, next_run_at = $3, last_error = $4, lease_expires_at = NULL
        WHERE id = $1
    `, rec.ID, rec.Attempt, rec.NextRunAt, rec.LastError)
	return err
}

func (s *PostgresStore) Complete(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM asyncworker_tasks WHERE id = $1`, id)
	return err
}
```

```go
pgPool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
if err != nil {
    log.Fatal(err)
}

pool := asyncworker.New(ctx, asyncworker.Options{
    Workers:       4,
    Store:         store.NewPostgresStore(pgPool),
    LeaseDuration: 2 * time.Minute,
})
```
