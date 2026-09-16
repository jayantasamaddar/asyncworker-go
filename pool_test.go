package asyncworker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor blocks until cond returns true or timeout elapses, failing the
// test in the latter case. Keeps tests from hanging forever on a bug.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// fastRetryPolicy keeps retry-driven tests quick and deterministic.
func fastRetryPolicy(maxAttempts int) RetryPolicy {
	return RetryPolicy{
		MaxAttempts: maxAttempts,
		BaseDelay:   2 * time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
		Multiplier:  2,
	}
}

func TestPool_Submit_Success(t *testing.T) {
	t.Run("should run a task once and report success", func(t *testing.T) {
		var successes atomic.Int32
		p := New(context.Background(), Options{
			Workers: 2,
			OnSuccess: func(id string, attempts int) {
				if attempts != 1 {
					t.Errorf("expected 1 attempt, got %d", attempts)
				}
				successes.Add(1)
			},
		})
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
		waitFor(t, time.Second, func() bool { return successes.Load() == 1 })
	})
}

func TestPool_Submit_RetriesUntilSuccess(t *testing.T) {
	t.Run("should retry a failing task until it succeeds", func(t *testing.T) {
		var errCalls, successAttempts atomic.Int32
		p := New(context.Background(), Options{
			Workers:     1,
			RetryPolicy: fastRetryPolicy(5),
			OnError: func(id string, attempt int, err error) {
				errCalls.Add(1)
			},
			OnSuccess: func(id string, attempts int) {
				successAttempts.Store(int32(attempts))
			},
		})
		p.Start()
		defer p.Close()

		var calls atomic.Int32
		_, err := p.Submit(func(ctx context.Context) error {
			n := calls.Add(1)
			if n < 3 {
				return errors.New("not yet")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		waitFor(t, time.Second, func() bool { return successAttempts.Load() == 3 })
		if got := errCalls.Load(); got != 2 {
			t.Fatalf("expected 2 OnError calls, got %d", got)
		}
	})
}

func TestPool_Submit_ExhaustsRetries(t *testing.T) {
	t.Run("should give up after MaxAttempts and report OnExhausted", func(t *testing.T) {
		var exhaustedAttempts atomic.Int32
		exhaustedCh := make(chan struct{})
		p := New(context.Background(), Options{
			Workers:     1,
			RetryPolicy: fastRetryPolicy(3),
			OnExhausted: func(id string, attempts int, err error) {
				exhaustedAttempts.Store(int32(attempts))
				close(exhaustedCh)
			},
		})
		p.Start()
		defer p.Close()

		boom := errors.New("boom")
		var calls atomic.Int32
		_, err := p.Submit(func(ctx context.Context) error {
			calls.Add(1)
			return boom
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		select {
		case <-exhaustedCh:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for OnExhausted")
		}

		if got := exhaustedAttempts.Load(); got != 3 {
			t.Fatalf("expected 3 attempts, got %d", got)
		}
		if got := calls.Load(); got != 3 {
			t.Fatalf("expected task invoked 3 times, got %d", got)
		}
	})
}

func TestPool_Submit_NoRetry(t *testing.T) {
	t.Run("should invoke a NoRetry task exactly once on failure", func(t *testing.T) {
		exhaustedCh := make(chan int, 1)
		p := New(context.Background(), Options{
			Workers: 1,
			OnExhausted: func(id string, attempts int, err error) {
				exhaustedCh <- attempts
			},
		})
		p.Start()
		defer p.Close()

		var calls atomic.Int32
		_, err := p.Submit(func(ctx context.Context) error {
			calls.Add(1)
			return errors.New("fail")
		}, WithRetryPolicy(NoRetry()))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		select {
		case attempts := <-exhaustedCh:
			if attempts != 1 {
				t.Fatalf("expected 1 attempt, got %d", attempts)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for OnExhausted")
		}
		waitFor(t, time.Second, func() bool { return calls.Load() == 1 })
	})
}

func TestPool_Submit_QueueFull(t *testing.T) {
	t.Run("should return ErrQueueFull without blocking when the queue is saturated", func(t *testing.T) {
		block := make(chan struct{})
		started := make(chan struct{})
		p := New(context.Background(), Options{Workers: 1, QueueSize: 1})
		p.Start()
		defer func() {
			close(block)
			p.Close()
		}()

		// Occupy the single worker so it can't drain the queue.
		if _, err := p.Submit(func(ctx context.Context) error {
			close(started)
			<-block
			return nil
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		<-started

		// Fill the (size-1) buffered queue.
		if _, err := p.Submit(func(ctx context.Context) error { return nil }); err != nil {
			t.Fatalf("unexpected error filling queue: %v", err)
		}

		// This one has nowhere to go.
		if _, err := p.Submit(func(ctx context.Context) error { return nil }); !errors.Is(err, ErrQueueFull) {
			t.Fatalf("expected ErrQueueFull, got %v", err)
		}
	})
}

func TestPool_Submit_AfterClose(t *testing.T) {
	t.Run("should return ErrPoolClosed once the pool is closed", func(t *testing.T) {
		p := New(context.Background(), Options{Workers: 1})
		p.Start()
		p.Close()

		if _, err := p.Submit(func(ctx context.Context) error { return nil }); !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("expected ErrPoolClosed, got %v", err)
		}
	})
}

func TestPool_Submit_NilTask(t *testing.T) {
	t.Run("should reject a nil task", func(t *testing.T) {
		p := New(context.Background(), Options{Workers: 1})
		p.Start()
		defer p.Close()

		if _, err := p.Submit(nil); !errors.Is(err, ErrNilTask) {
			t.Fatalf("expected ErrNilTask, got %v", err)
		}
	})
}

func TestPool_TaskPanicRecovered(t *testing.T) {
	t.Run("should recover a panicking task as a retryable error", func(t *testing.T) {
		exhaustedCh := make(chan error, 1)
		p := New(context.Background(), Options{
			Workers:     1,
			RetryPolicy: NoRetry(),
			OnExhausted: func(id string, attempts int, err error) {
				exhaustedCh <- err
			},
		})
		p.Start()
		defer p.Close()

		_, err := p.Submit(func(ctx context.Context) error {
			panic("kaboom")
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		select {
		case gotErr := <-exhaustedCh:
			if gotErr == nil {
				t.Fatal("expected a non-nil error from the recovered panic")
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for OnExhausted")
		}
	})
}

func TestPool_Close_CancelsContextPassedToTasks(t *testing.T) {
	t.Run("should cancel task context when the pool is closed", func(t *testing.T) {
		started := make(chan struct{})
		cancelled := make(chan struct{})
		p := New(context.Background(), Options{Workers: 1})
		p.Start()

		if _, err := p.Submit(func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			close(cancelled)
			return ctx.Err()
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		<-started
		p.Close()

		select {
		case <-cancelled:
		case <-time.After(time.Second):
			t.Fatal("expected task context to be cancelled on Close")
		}
	})
}

func TestRetryPolicy_Delay(t *testing.T) {
	t.Run("should not delay the first attempt", func(t *testing.T) {
		policy := RetryPolicy{MaxAttempts: 3, BaseDelay: 100 * time.Millisecond, Multiplier: 2}
		if d := policy.delay(1); d != 0 {
			t.Fatalf("expected 0 delay before the first attempt, got %s", d)
		}
	})

	t.Run("should grow exponentially without jitter", func(t *testing.T) {
		policy := RetryPolicy{MaxAttempts: 4, BaseDelay: 10 * time.Millisecond, Multiplier: 2}
		if d := policy.delay(2); d != 10*time.Millisecond {
			t.Fatalf("expected 10ms, got %s", d)
		}
		if d := policy.delay(3); d != 20*time.Millisecond {
			t.Fatalf("expected 20ms, got %s", d)
		}
		if d := policy.delay(4); d != 40*time.Millisecond {
			t.Fatalf("expected 40ms, got %s", d)
		}
	})

	t.Run("should cap at MaxDelay", func(t *testing.T) {
		policy := RetryPolicy{MaxAttempts: 10, BaseDelay: 10 * time.Millisecond, Multiplier: 2, MaxDelay: 25 * time.Millisecond}
		if d := policy.delay(5); d != 25*time.Millisecond {
			t.Fatalf("expected delay capped at 25ms, got %s", d)
		}
	})

	t.Run("should keep jittered delay within the expected spread", func(t *testing.T) {
		policy := RetryPolicy{MaxAttempts: 5, BaseDelay: 100 * time.Millisecond, Multiplier: 1, Jitter: 0.5}
		for range 50 {
			d := policy.delay(2)
			if d < 50*time.Millisecond || d > 150*time.Millisecond {
				t.Fatalf("jittered delay %s outside expected [50ms,150ms] spread", d)
			}
		}
	})
}

func TestNoRetry(t *testing.T) {
	t.Run("should produce a policy with exactly one attempt", func(t *testing.T) {
		if got := NoRetry().MaxAttempts; got != 1 {
			t.Fatalf("expected MaxAttempts 1, got %d", got)
		}
	})
}
