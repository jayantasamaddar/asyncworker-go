package asyncworker

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrEmptyTaskType is returned by Enqueue when taskType is "".
var ErrEmptyTaskType = errors.New("asyncworker: taskType must not be empty")

// Handler runs a durable task enqueued under a given type. payload is
// exactly what was passed to Enqueue; decoding it (JSON, protobuf, ...) is
// the handler's responsibility.
type Handler func(ctx context.Context, payload []byte) error

// UnregisteredHandlerError is the error a durable task fails with when no
// Handler is registered for its type. It is treated as permanent (not
// retried) since retrying can't make a missing handler appear.
type UnregisteredHandlerError struct {
	Type string
}

func (e *UnregisteredHandlerError) Error() string {
	return fmt.Sprintf("asyncworker: no handler registered for task type %q", e.Type)
}

// Register associates a Handler with a task type, so durable tasks
// Enqueue'd (by this Pool or, for a shared real Store, by another process)
// under that type can be executed. Safe to call concurrently, including
// after Start; register a type before Enqueue-ing tasks of that type,
// otherwise they'll fail with UnregisteredHandlerError until registered.
func (p *Pool) Register(taskType string, h Handler) {
	p.handlersMu.Lock()
	defer p.handlersMu.Unlock()
	if p.handlers == nil {
		p.handlers = make(map[string]Handler)
	}
	p.handlers[taskType] = h
}

func (p *Pool) handler(taskType string) (Handler, bool) {
	p.handlersMu.RLock()
	defer p.handlersMu.RUnlock()
	h, ok := p.handlers[taskType]
	return h, ok
}

// Enqueue persists a durable task of the given type through the Pool's
// Store (Options.Store, or a MemoryStore by default) and returns its ID.
// Unlike Submit, an Enqueue'd task survives a process restart when the Pool
// is configured with a real Store, because it's identified by a type name
// plus an opaque payload rather than a Go closure — register a matching
// Handler with Register so it can actually run.
//
// Enqueue only performs the write to the Store; a background poller (start
// with Start, tuned with Options.PollInterval) claims and executes due
// tasks.
func (p *Pool) Enqueue(ctx context.Context, taskType string, payload []byte, opts ...SubmitOption) (string, error) {
	if taskType == "" {
		return "", ErrEmptyTaskType
	}
	if p.closed.Load() {
		return "", ErrPoolClosed
	}

	now := time.Now()
	j := job{
		id:        fmt.Sprintf("task-%d", p.idSeq.Add(1)),
		typ:       taskType,
		payload:   payload,
		policy:    p.opts.RetryPolicy,
		createdAt: now,
		durable:   true,
	}
	for _, opt := range opts {
		opt(&j)
	}

	rec := Record{
		ID:        j.id,
		Type:      j.typ,
		Payload:   j.payload,
		Policy:    j.policy,
		CreatedAt: j.createdAt,
		NextRunAt: j.createdAt,
	}
	if err := p.opts.Store.Enqueue(ctx, rec); err != nil {
		return "", fmt.Errorf("asyncworker: enqueue: %w", err)
	}
	return j.id, nil
}

// pollStore periodically claims due durable tasks from the Store and feeds
// them to the worker queue. It is what lets a durable task recovered after
// a crash (or enqueued by a different process sharing the same Store) get
// picked up and run.
func (p *Pool) pollStore() {
	defer p.workers.Done()

	interval := p.opts.PollInterval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.claimDue()
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Pool) claimDue() {
	batch := p.opts.Workers * 2
	recs, err := p.opts.Store.Claim(p.ctx, batch, p.opts.LeaseDuration)
	if err != nil {
		if p.opts.OnError != nil {
			p.opts.OnError("", 0, fmt.Errorf("asyncworker: claim: %w", err))
		}
		return
	}

	for _, rec := range recs {
		j := job{
			id:        rec.ID,
			typ:       rec.Type,
			payload:   rec.Payload,
			policy:    rec.Policy,
			attempt:   rec.Attempt,
			createdAt: rec.CreatedAt,
			durable:   true,
		}
		select {
		case p.queue <- j:
		case <-p.ctx.Done():
			return
		}
	}
}
