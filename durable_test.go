package asyncworker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// spyStore wraps a MemoryStore and counts calls, so tests can assert the
// Pool actually goes through the configured Store rather than silently
// falling back to some other path.
type spyStore struct {
	*MemoryStore
	enqueues, claims, reschedules, completes atomic.Int32
}

func newSpyStore() *spyStore {
	return &spyStore{MemoryStore: NewMemoryStore()}
}

func (s *spyStore) Enqueue(ctx context.Context, rec Record) error {
	s.enqueues.Add(1)
	return s.MemoryStore.Enqueue(ctx, rec)
}

func (s *spyStore) Claim(ctx context.Context, n int) ([]Record, error) {
	s.claims.Add(1)
	return s.MemoryStore.Claim(ctx, n)
}

func (s *spyStore) Reschedule(ctx context.Context, rec Record) error {
	s.reschedules.Add(1)
	return s.MemoryStore.Reschedule(ctx, rec)
}

func (s *spyStore) Complete(ctx context.Context, id string) error {
	s.completes.Add(1)
	return s.MemoryStore.Complete(ctx, id)
}

func fastPollOptions(store Store) Options {
	return Options{
		Workers:      1,
		Store:        store,
		PollInterval: 2 * time.Millisecond,
		RetryPolicy:  fastRetryPolicy(3),
	}
}

func TestPool_Enqueue_RunsThroughRegisteredHandler(t *testing.T) {
	t.Run("should claim, run and complete a durable task via its handler", func(t *testing.T) {
		store := newSpyStore()
		successCh := make(chan int, 1)
		p := New(context.Background(), Options{
			Workers:      1,
			Store:        store,
			PollInterval: 2 * time.Millisecond,
			OnSuccess: func(id string, attempts int) {
				successCh <- attempts
			},
		})

		var gotPayload []byte
		p.Register("greet", func(ctx context.Context, payload []byte) error {
			gotPayload = payload
			return nil
		})
		p.Start()
		defer p.Close()

		id, err := p.Enqueue(context.Background(), "greet", []byte("hello"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id == "" {
			t.Fatal("expected a non-empty task id")
		}

		select {
		case attempts := <-successCh:
			if attempts != 1 {
				t.Fatalf("expected 1 attempt, got %d", attempts)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for OnSuccess")
		}

		if string(gotPayload) != "hello" {
			t.Fatalf("expected handler to see payload %q, got %q", "hello", gotPayload)
		}
		if store.enqueues.Load() != 1 {
			t.Fatalf("expected 1 Enqueue call, got %d", store.enqueues.Load())
		}
		if store.completes.Load() != 1 {
			t.Fatalf("expected 1 Complete call, got %d", store.completes.Load())
		}
	})
}

func TestPool_Enqueue_RetriesThroughStore(t *testing.T) {
	t.Run("should reschedule a failing durable task via the store and eventually succeed", func(t *testing.T) {
		store := newSpyStore()
		successCh := make(chan int, 1)
		p := New(context.Background(), fastPollOptions(store))
		p.opts.OnSuccess = func(id string, attempts int) { successCh <- attempts }

		var calls atomic.Int32
		p.Register("flaky", func(ctx context.Context, payload []byte) error {
			if calls.Add(1) < 3 {
				return errors.New("not yet")
			}
			return nil
		})
		p.Start()
		defer p.Close()

		if _, err := p.Enqueue(context.Background(), "flaky", nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		select {
		case attempts := <-successCh:
			if attempts != 3 {
				t.Fatalf("expected 3 attempts, got %d", attempts)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for OnSuccess")
		}

		if got := store.reschedules.Load(); got != 2 {
			t.Fatalf("expected 2 Reschedule calls, got %d", got)
		}
		if got := store.completes.Load(); got != 1 {
			t.Fatalf("expected 1 Complete call, got %d", got)
		}
	})
}

func TestPool_Enqueue_ExhaustsRetries(t *testing.T) {
	t.Run("should give up and remove the record once retries are exhausted", func(t *testing.T) {
		store := newSpyStore()
		exhaustedCh := make(chan int, 1)
		opts := fastPollOptions(store)
		opts.OnExhausted = func(id string, attempts int, err error) { exhaustedCh <- attempts }
		p := New(context.Background(), opts)

		p.Register("always-fails", func(ctx context.Context, payload []byte) error {
			return errors.New("boom")
		})
		p.Start()
		defer p.Close()

		if _, err := p.Enqueue(context.Background(), "always-fails", nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		select {
		case attempts := <-exhaustedCh:
			if attempts != 3 {
				t.Fatalf("expected 3 attempts (MaxAttempts), got %d", attempts)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for OnExhausted")
		}

		waitFor(t, time.Second, func() bool { return store.completes.Load() == 1 })
	})
}

func TestPool_Enqueue_UnregisteredHandler(t *testing.T) {
	t.Run("should exhaust immediately, without retrying, when no handler is registered", func(t *testing.T) {
		store := newSpyStore()
		exhaustedCh := make(chan error, 1)
		opts := fastPollOptions(store)
		opts.OnExhausted = func(id string, attempts int, err error) { exhaustedCh <- err }
		p := New(context.Background(), opts)
		p.Start()
		defer p.Close()

		if _, err := p.Enqueue(context.Background(), "nobody-home", nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		select {
		case err := <-exhaustedCh:
			if _, ok := errors.AsType[*UnregisteredHandlerError](err); !ok {
				t.Fatalf("expected UnregisteredHandlerError, got %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for OnExhausted")
		}

		if got := store.reschedules.Load(); got != 0 {
			t.Fatalf("expected no Reschedule calls for an unregistered handler, got %d", got)
		}
	})
}

func TestPool_Enqueue_ValidationAndLifecycle(t *testing.T) {
	t.Run("should reject an empty task type", func(t *testing.T) {
		p := New(context.Background(), Options{Workers: 1})
		p.Start()
		defer p.Close()

		if _, err := p.Enqueue(context.Background(), "", nil); !errors.Is(err, ErrEmptyTaskType) {
			t.Fatalf("expected ErrEmptyTaskType, got %v", err)
		}
	})

	t.Run("should reject Enqueue after Close", func(t *testing.T) {
		p := New(context.Background(), Options{Workers: 1})
		p.Start()
		p.Close()

		if _, err := p.Enqueue(context.Background(), "whatever", nil); !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("expected ErrPoolClosed, got %v", err)
		}
	})
}

func TestPool_Submit_NeverTouchesStore(t *testing.T) {
	t.Run("should not call the store for plain closure-based Submit tasks", func(t *testing.T) {
		store := newSpyStore()
		p := New(context.Background(), fastPollOptions(store))
		p.Start()
		defer p.Close()

		var ran atomic.Bool
		if _, err := p.Submit(func(ctx context.Context) error {
			ran.Store(true)
			return nil
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		waitFor(t, time.Second, ran.Load)
		// Give the poller a few ticks to prove it stays idle for this job.
		time.Sleep(20 * time.Millisecond)

		if got := store.enqueues.Load(); got != 0 {
			t.Fatalf("expected 0 Enqueue calls, got %d", got)
		}
		if got := store.completes.Load(); got != 0 {
			t.Fatalf("expected 0 Complete calls, got %d", got)
		}
	})
}

func TestPool_DefaultStore_IsMemoryStore(t *testing.T) {
	t.Run("should default Options.Store to a MemoryStore", func(t *testing.T) {
		p := New(context.Background(), Options{Workers: 1})
		if _, ok := p.opts.Store.(*MemoryStore); !ok {
			t.Fatalf("expected default store to be *MemoryStore, got %T", p.opts.Store)
		}
	})
}

func TestMemoryStore_ClaimHonorsNextRunAt(t *testing.T) {
	t.Run("should not claim a record before its NextRunAt", func(t *testing.T) {
		store := NewMemoryStore()
		ctx := context.Background()

		if err := store.Enqueue(ctx, Record{ID: "later", NextRunAt: time.Now().Add(time.Hour)}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := store.Enqueue(ctx, Record{ID: "now", NextRunAt: time.Now()}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		recs, err := store.Claim(ctx, 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(recs) != 1 || recs[0].ID != "now" {
			t.Fatalf("expected only the due record to be claimed, got %+v", recs)
		}
	})
}
