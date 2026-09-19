// Package asyncworker is a small, dependency-free async task runner with
// retry and exponential backoff. It is meant for applications that want
// "fire off some work in the background with retries" without standing up
// something like Temporal.
//
// By default it is purely in-memory: nothing is persisted, so pending or
// in-flight work submitted via Submit is lost on process exit or crash.
// Persistence is opt-in and per-task: tasks submitted via Enqueue are
// backed by a Store (Options.Store) — implement it against Redis, Postgres,
// or anything else to survive a restart. Leaving Options.Store nil uses
// MemoryStore, keeping the original non-persisting behavior.
package asyncworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrPoolClosed is returned by Submit once Close has been called.
	ErrPoolClosed = errors.New("asyncworker: pool is closed")

	// ErrQueueFull is returned by Submit when the queue is at capacity.
	// Submit never blocks; the caller decides whether to drop, retry later,
	// or apply backpressure upstream.
	ErrQueueFull = errors.New("asyncworker: queue is full")

	// ErrNilTask is returned by Submit when task is nil.
	ErrNilTask = errors.New("asyncworker: task must not be nil")
)

// Task is the unit of work executed by a Pool. Return a non-nil error to
// trigger a retry (subject to the applicable RetryPolicy); return nil on
// success. The context is cancelled when the Pool is closed, so long-running
// tasks should select on ctx.Done().
type Task func(ctx context.Context) error

// PreHook is called before each attempt of a task. A panic inside a PreHook
// is recovered and does not prevent the task from running.
type PreHook func(ctx context.Context, taskID string)

// PostHook is called after each attempt of a task, whether it succeeded
// (err is nil) or failed. A panic inside a PostHook is recovered and does
// not affect the task's own outcome.
type PostHook func(ctx context.Context, taskID string, err error)

// job is a queued unit of work, either an ephemeral closure submitted via
// Submit or a durable, Store-backed task submitted via Enqueue.
type job struct {
	id        string
	task      Task   // set for Submit jobs; nil for durable jobs
	typ       string // set for durable jobs; looked up in the handler registry
	payload   []byte // set for durable jobs
	policy    RetryPolicy
	attempt   int // attempts already made
	createdAt time.Time
	durable   bool
}

// Options configures a Pool.
type Options struct {
	// Workers is the number of goroutines concurrently pulling tasks off
	// the queue. Defaults to 1.
	Workers int

	// QueueSize is the capacity of the in-memory task queue. Defaults to
	// 64. Submit fails with ErrQueueFull once it is exceeded.
	QueueSize int

	// RetryPolicy is the default policy applied to tasks submitted without
	// an explicit per-task policy (see WithRetryPolicy). Defaults to
	// DefaultRetryPolicy().
	RetryPolicy RetryPolicy

	// OnError, if set, is called after every failed attempt, including
	// ones that will still be retried. Called from a worker goroutine.
	OnError func(taskID string, attempt int, err error)

	// OnExhausted, if set, is called once for a task that failed on its
	// final attempt (or whose pending retry was cancelled by Close) and
	// will not be retried again.
	OnExhausted func(taskID string, attempts int, err error)

	// OnSuccess, if set, is called after a task completes without error.
	OnSuccess func(taskID string, attempts int)

	// Store persists durable tasks submitted via Enqueue so they can
	// survive a process restart, given a real implementation (Redis,
	// Postgres, ...). Tasks submitted via Submit are plain closures and
	// can never be persisted — only Enqueue goes through the Store.
	// Defaults to a fresh MemoryStore, which keeps the original
	// non-persisting, in-memory-only behavior: persistence is entirely
	// opt-in.
	Store Store

	// PollInterval controls how often the Pool checks the Store for due
	// durable tasks. Defaults to 200ms. Irrelevant if Enqueue is never
	// used.
	PollInterval time.Duration

	// PreHooks run, in order, before every attempt of every task (Submit or
	// Enqueue). Useful for cross-cutting concerns like logging or metrics.
	// A panicking PreHook is recovered and does not prevent the task from
	// running.
	PreHooks []PreHook

	// PostHooks run, in order, after every attempt of every task, whether
	// that attempt succeeded or failed. A panicking PostHook is recovered
	// and does not affect the task's own outcome.
	PostHooks []PostHook

	// LeaseDuration is how long a claimed durable task is invisible to
	// other claimers (including other Pool instances/processes sharing the
	// same Store) before it's treated as abandoned and reclaimed. It's
	// what lets a task survive a crash mid-processing instead of being
	// dropped forever: pick a value comfortably longer than the task
	// normally takes, since a task that outlives its lease can be claimed
	// and run again concurrently. Defaults to 2 minutes. Irrelevant if
	// Enqueue is never used.
	LeaseDuration time.Duration
}

// Pool is a fixed-size group of workers draining an in-memory task queue,
// with per-task retry and backoff. Zero value is not usable; construct with
// New.
type Pool struct {
	opts   Options
	ctx    context.Context
	cancel context.CancelFunc

	queue chan job

	workers sync.WaitGroup // worker goroutines
	pending sync.WaitGroup // scheduled retry timers not yet resolved

	closeOnce sync.Once
	closed    atomic.Bool
	idSeq     atomic.Uint64

	handlersMu sync.RWMutex
	handlers   map[string]Handler // registered via Register, looked up by durable jobs
}

// New builds a Pool bound to parent. Cancelling parent has the same effect
// as calling Close. Call Start to begin processing.
func New(parent context.Context, opts Options) *Pool {
	if opts.Workers < 1 {
		opts.Workers = 1
	}
	if opts.QueueSize < 1 {
		opts.QueueSize = 64
	}
	if opts.RetryPolicy == (RetryPolicy{}) {
		opts.RetryPolicy = DefaultRetryPolicy()
	}
	if opts.Store == nil {
		opts.Store = NewMemoryStore()
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = 2 * time.Minute
	}

	ctx, cancel := context.WithCancel(parent)

	return &Pool{
		opts:   opts,
		ctx:    ctx,
		cancel: cancel,
		queue:  make(chan job, opts.QueueSize),
	}
}

// Start launches the worker goroutines, plus a background poller that
// claims due durable tasks from the Store (see Enqueue). Safe to call once;
// calling it again starts additional workers/pollers on top of the existing
// ones, which is normally not what you want.
func (p *Pool) Start() {
	for i := 0; i < p.opts.Workers; i++ {
		p.workers.Add(1)
		go p.worker()
	}
	p.workers.Add(1)
	go p.pollStore()
}

// SubmitOption customizes a single Submit call.
type SubmitOption func(*job)

// WithID assigns an explicit task ID instead of an auto-generated one. IDs
// are only used for observability (they're passed to the Options callbacks)
// and are not required to be unique.
func WithID(id string) SubmitOption {
	return func(j *job) { j.id = id }
}

// WithRetryPolicy overrides the pool's default RetryPolicy for this task.
func WithRetryPolicy(policy RetryPolicy) SubmitOption {
	return func(j *job) { j.policy = policy }
}

// Submit enqueues task for execution and returns its ID. It never blocks:
// if the queue is full it returns ErrQueueFull, and once the pool has been
// closed it returns ErrPoolClosed.
func (p *Pool) Submit(task Task, opts ...SubmitOption) (string, error) {
	if task == nil {
		return "", ErrNilTask
	}
	if p.closed.Load() {
		return "", ErrPoolClosed
	}

	j := job{
		id:     fmt.Sprintf("task-%d", p.idSeq.Add(1)),
		task:   task,
		policy: p.opts.RetryPolicy,
	}
	for _, opt := range opts {
		opt(&j)
	}

	select {
	case p.queue <- j:
		return j.id, nil
	default:
		return "", ErrQueueFull
	}
}

// Close stops accepting new tasks, cancels the pool's context (so in-flight
// and future task invocations observe cancellation), and waits for all
// worker goroutines and pending retry timers to finish. Safe to call
// multiple times; only the first call has effect.
//
// Close does not guarantee delivery of tasks still sitting unpicked in the
// queue buffer at the moment of cancellation — this is a non-persisting,
// best-effort in-memory queue, not a durable one.
func (p *Pool) Close() {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		p.cancel()
		p.workers.Wait()
		p.pending.Wait()
	})
}

// Wait blocks until all worker goroutines and pending retry timers have
// finished. Workers only exit once the pool's context is cancelled, so Wait
// is normally called after (or concurrently with, from another goroutine)
// Close.
func (p *Pool) Wait() {
	p.workers.Wait()
	p.pending.Wait()
}

// ---------------------------------------------------------------------------- //
// Internals
// ---------------------------------------------------------------------------- //

func (p *Pool) worker() {
	defer p.workers.Done()
	for {
		select {
		case j := <-p.queue:
			p.run(j)
		case <-p.ctx.Done():
			return
		}
	}
}

// run executes a single attempt of j and, on failure, schedules a retry if
// the policy allows it.
func (p *Pool) run(j job) {
	j.attempt++

	p.executePreHooksSafe(j)
	err := p.invoke(j)
	p.executePostHooksSafe(j, err)

	if err == nil {
		if j.durable {
			if serr := p.opts.Store.Complete(p.ctx, j.id); serr != nil && p.opts.OnError != nil {
				p.opts.OnError(j.id, j.attempt, fmt.Errorf("asyncworker: complete: %w", serr))
			}
		}
		if p.opts.OnSuccess != nil {
			p.opts.OnSuccess(j.id, j.attempt)
		}
		return
	}

	if p.opts.OnError != nil {
		p.opts.OnError(j.id, j.attempt, err)
	}

	_, unregistered := errors.AsType[*UnregisteredHandlerError](err)
	exhausted := j.attempt >= j.policy.MaxAttempts || unregistered

	if exhausted {
		if j.durable {
			if serr := p.opts.Store.Complete(p.ctx, j.id); serr != nil && p.opts.OnError != nil {
				p.opts.OnError(j.id, j.attempt, fmt.Errorf("asyncworker: complete: %w", serr))
			}
		}
		if p.opts.OnExhausted != nil {
			p.opts.OnExhausted(j.id, j.attempt, err)
		}
		return
	}

	if j.durable {
		rec := Record{
			ID:        j.id,
			Type:      j.typ,
			Payload:   j.payload,
			Policy:    j.policy,
			Attempt:   j.attempt,
			CreatedAt: j.createdAt,
			NextRunAt: time.Now().Add(j.policy.delay(j.attempt + 1)),
			LastError: err.Error(),
		}
		if serr := p.opts.Store.Reschedule(p.ctx, rec); serr != nil && p.opts.OnError != nil {
			p.opts.OnError(j.id, j.attempt, fmt.Errorf("asyncworker: reschedule: %w", serr))
		}
		return
	}

	p.pending.Add(1)
	go p.scheduleRetry(j, j.policy.delay(j.attempt+1))
}

// executePreHooksSafe runs all configured PreHooks for j, recovering from
// any panic so a bad hook can't take down a worker goroutine or block the
// task it's observing.
func (p *Pool) executePreHooksSafe(j job) {
	for _, hook := range p.opts.PreHooks {
		p.runPreHookSafe(hook, j)
	}
}

func (p *Pool) runPreHookSafe(hook PreHook, j job) {
	defer func() {
		if r := recover(); r != nil && p.opts.OnError != nil {
			p.opts.OnError(j.id, j.attempt, fmt.Errorf("asyncworker: pre-hook panicked: %v", r))
		}
	}()
	hook(p.ctx, j.id)
}

// executePostHooksSafe runs all configured PostHooks for j, recovering from
// any panic so a bad hook can't take down a worker goroutine or affect the
// task's own outcome.
func (p *Pool) executePostHooksSafe(j job, taskErr error) {
	for _, hook := range p.opts.PostHooks {
		p.runPostHookSafe(hook, j, taskErr)
	}
}

func (p *Pool) runPostHookSafe(hook PostHook, j job, taskErr error) {
	defer func() {
		if r := recover(); r != nil && p.opts.OnError != nil {
			p.opts.OnError(j.id, j.attempt, fmt.Errorf("asyncworker: post-hook panicked: %v", r))
		}
	}()
	hook(p.ctx, j.id, taskErr)
}

// invoke runs the task (a closure for Submit jobs, or a registered Handler
// for durable jobs), converting a panic into an error so one bad task can't
// take down a worker goroutine.
func (p *Pool) invoke(j job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("asyncworker: task %q panicked: %v", j.id, r)
		}
	}()

	if j.durable {
		h, ok := p.handler(j.typ)
		if !ok {
			return &UnregisteredHandlerError{Type: j.typ}
		}
		return h(p.ctx, j.payload)
	}

	return j.task(p.ctx)
}

// scheduleRetry waits out the backoff delay, then re-queues j for another
// attempt. It bails out (reporting OnExhausted) if the pool is closed
// before the delay elapses or before the re-queue can be delivered.
func (p *Pool) scheduleRetry(j job, delay time.Duration) {
	defer p.pending.Done()

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-p.ctx.Done():
		if p.opts.OnExhausted != nil {
			p.opts.OnExhausted(j.id, j.attempt, p.ctx.Err())
		}
		return
	}

	select {
	case p.queue <- j:
	case <-p.ctx.Done():
		if p.opts.OnExhausted != nil {
			p.opts.OnExhausted(j.id, j.attempt, p.ctx.Err())
		}
	}
}
