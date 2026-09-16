package asyncworker

import (
	"math"
	"math/rand"
	"time"
)

// RetryPolicy controls how a failed Task is retried. Retries are purely
// in-memory: nothing is persisted, so pending retries are lost on process
// restart and there is no delivery guarantee beyond "this process, this
// run".
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts, including the first,
	// e.g. MaxAttempts: 3 means 1 initial try plus up to 2 retries.
	// MaxAttempts <= 1 disables retries.
	MaxAttempts int

	// BaseDelay is the delay before the first retry (i.e. before attempt 2).
	BaseDelay time.Duration

	// MaxDelay caps the computed backoff delay. Zero means uncapped.
	MaxDelay time.Duration

	// Multiplier grows the delay after every attempt for exponential
	// backoff. Values <= 1 keep the delay constant at BaseDelay.
	Multiplier float64

	// Jitter randomizes the computed delay by +/- this fraction (0..1) to
	// avoid many tasks retrying in lockstep. 0 disables jitter.
	Jitter float64
}

// DefaultRetryPolicy returns a sane exponential-backoff-with-jitter policy:
// up to 5 attempts, starting at 200ms, doubling up to a 30s ceiling, with
// 20% jitter.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   200 * time.Millisecond,
		MaxDelay:    30 * time.Second,
		Multiplier:  2,
		Jitter:      0.2,
	}
}

// NoRetry disables retries entirely: a task submitted with this policy gets
// exactly one attempt.
func NoRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 1}
}

// delay computes the backoff delay before the given attempt is made.
// attempt is 1-indexed and refers to the *upcoming* attempt, so
// delay(2) is the wait before the first retry.
func (p RetryPolicy) delay(attempt int) time.Duration {
	if p.BaseDelay <= 0 || attempt < 2 {
		return 0
	}

	mult := p.Multiplier
	if mult < 1 {
		mult = 1
	}

	d := float64(p.BaseDelay) * math.Pow(mult, float64(attempt-2))
	if p.MaxDelay > 0 && d > float64(p.MaxDelay) {
		d = float64(p.MaxDelay)
	}

	if p.Jitter > 0 {
		j := p.Jitter
		if j > 1 {
			j = 1
		}
		spread := d * j
		// Uniformly distribute in [d-spread, d+spread].
		d = d - spread + rand.Float64()*2*spread
	}

	if d < 0 {
		d = 0
	}

	return time.Duration(d)
}
