package asyncworker

import (
	"context"
	"sync"
	"time"
)

// Record is the durable representation of a queued task: everything needed
// to re-run it without holding a live Go closure, so it can survive a
// process restart when backed by a real Store (Redis, Postgres, SQS, ...).
//
// A closure (Task) can't be serialized, so Records only ever exist for
// tasks submitted via Pool.Enqueue, not Pool.Submit — see the package doc
// for which method to use.
type Record struct {
	ID      string
	Type    string // looked up in the Pool's handler registry to find the code to run
	Payload []byte // opaque to the Pool and the Store; the Handler decides how to decode it

	Policy  RetryPolicy
	Attempt int // attempts already made

	CreatedAt time.Time
	NextRunAt time.Time // claimable once time.Now() >= NextRunAt
	LastError string    // best-effort, for observability/debugging only

	// LeaseExpiresAt is set by Claim to now+leaseFor, and is what makes a
	// claimed Record reclaimable if the claimer dies before calling
	// Complete or Reschedule: once time.Now() is past it, the record is
	// claimable again even though nothing ever resolved it. Zero means
	// "not currently leased". Reschedule and Complete both clear it.
	LeaseExpiresAt time.Time
}

// Store persists Records for durable, crash-surviving task execution. It is
// the seam this package uses for opt-in persistence: implement it against
// Redis, Postgres, SQS, or anything else, and pass it as Options.Store.
//
// Leave Options.Store nil to get MemoryStore, the zero-dependency default —
// it makes Enqueue behave exactly like the package's original non-
// persisting queue, so persistence is entirely opt-in.
//
// Implementations must be safe for concurrent use, and Claim must make a
// claimed Record invisible to concurrent Claim calls (e.g. via a lease /
// visibility timeout, or `SELECT ... FOR UPDATE SKIP LOCKED`) until
// Complete or Reschedule resolves it, so multiple Pool instances (even in
// different processes) can safely share one Store.
//
// A claim is only a lease, not a delete: if the claimer crashes before
// calling Complete or Reschedule, the record must become claimable again
// once its lease (see Claim) expires, so a crash mid-processing doesn't
// silently drop the task. This does mean a task can be delivered and run
// more than once if it outlives its lease — see Options.LeaseDuration.
type Store interface {
	// Enqueue persists a new record. rec.NextRunAt is normally rec.CreatedAt
	// (claimable right away).
	Enqueue(ctx context.Context, rec Record) error

	// Claim returns up to n due records (NextRunAt <= time.Now()) whose
	// lease has expired (or that have never been claimed), and marks them
	// claimed by setting LeaseExpiresAt to time.Now().Add(leaseFor).
	// Returns a nil/empty slice, not an error, when nothing is due.
	//
	// A claimed record must remain claimed (invisible to other Claim
	// calls) only until LeaseExpiresAt, at which point it becomes
	// claimable again even if the original claimer never called Complete
	// or Reschedule — that's what lets work survive a crash mid-processing.
	Claim(ctx context.Context, n int, leaseFor time.Duration) ([]Record, error)

	// Reschedule persists rec after a failed attempt, with rec.Attempt and
	// rec.NextRunAt already updated by the caller, and clears its lease. It
	// makes the record claimable again at rec.NextRunAt.
	Reschedule(ctx context.Context, rec Record) error

	// Complete removes a record after it succeeds, exhausts its retries, or
	// turns out to have no registered handler.
	Complete(ctx context.Context, id string) error
}

// ---------------------------------------------------------------------------- //
// MemoryStore: the zero-dependency default
// ---------------------------------------------------------------------------- //

// MemoryStore is an in-process, non-persisting Store. It's what Pool uses
// automatically when Options.Store is left nil, so out of the box this
// package behaves exactly as it always has: nothing survives a process
// restart. Swap in a real Store to change that.
type MemoryStore struct {
	mu      sync.Mutex
	records map[string]Record
}

// NewMemoryStore constructs an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[string]Record)}
}

func (s *MemoryStore) Enqueue(_ context.Context, rec Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[rec.ID] = rec
	return nil
}

func (s *MemoryStore) Claim(_ context.Context, n int, leaseFor time.Duration) ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var claimed []Record
	for id, rec := range s.records {
		if len(claimed) >= n {
			break
		}
		if rec.NextRunAt.After(now) {
			continue
		}
		// Still leased to a previous claimer: not due for reclaim yet.
		if rec.LeaseExpiresAt.After(now) {
			continue
		}
		rec.LeaseExpiresAt = now.Add(leaseFor)
		s.records[id] = rec
		claimed = append(claimed, rec)
	}
	return claimed, nil
}

func (s *MemoryStore) Reschedule(_ context.Context, rec Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec.LeaseExpiresAt = time.Time{}
	s.records[rec.ID] = rec
	return nil
}

func (s *MemoryStore) Complete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
	return nil
}
