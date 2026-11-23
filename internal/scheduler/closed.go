package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"api-load-testing-engine/internal/metrics"
)

// ClosedExecutor runs N virtual users looping with a configurable think time.
type ClosedExecutor struct {
	targetVUs  int64
	thinkTime  time.Duration
	runner     IterationRunner
	aggregator *metrics.LocalAggregator

	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	vuIDSeq  int64
	activeVU int64
}

// NewClosedExecutor creates a closed-model scheduler.
func NewClosedExecutor(targetVUs int, thinkTime time.Duration, runner IterationRunner, agg *metrics.LocalAggregator) *ClosedExecutor {
	if targetVUs < 0 {
		targetVUs = 0
	}
	return &ClosedExecutor{
		targetVUs:  int64(targetVUs),
		thinkTime:  thinkTime,
		runner:     runner,
		aggregator: agg,
	}
}

// Start begins running the closed workload.
func (e *ClosedExecutor) Start(ctx context.Context) error {
	e.mu.Lock()
	ctx, cancel := context.WithCancel(ctx)
	e.ctx = ctx
	e.cancel = cancel
	target := atomic.LoadInt64(&e.targetVUs)
	e.mu.Unlock()

	for i := int64(0); i < target; i++ {
		e.spawnVU(ctx)
	}

	return nil
}

func (e *ClosedExecutor) spawnVU(ctx context.Context) {
	vuID := int(atomic.AddInt64(&e.vuIDSeq, 1))
	atomic.AddInt64(&e.activeVU, 1)
	if e.aggregator != nil {
		e.aggregator.SetActiveVUs(uint32(atomic.LoadInt64(&e.activeVU)))
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer func() {
			atomic.AddInt64(&e.activeVU, -1)
			if e.aggregator != nil {
				e.aggregator.SetActiveVUs(uint32(atomic.LoadInt64(&e.activeVU)))
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// If current active VUs exceeds target (scale down), this VU can terminate
			currentActive := atomic.LoadInt64(&e.activeVU)
			currentTarget := atomic.LoadInt64(&e.targetVUs)
			if currentActive > currentTarget {
				return
			}

			start := time.Now()
			_ = e.runner(ctx, vuID, start)

			if e.thinkTime > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(e.thinkTime):
				}
			}
		}
	}()
}

// UpdateTarget dynamically scales the number of concurrent VUs.
func (e *ClosedExecutor) UpdateTarget(target float64) {
	newTarget := int64(target)
	if newTarget < 0 {
		newTarget = 0
	}

	e.mu.Lock()
	atomic.StoreInt64(&e.targetVUs, newTarget)
	cancel := e.cancel
	e.mu.Unlock()

	if cancel == nil {
		return
	}

	currentActive := atomic.LoadInt64(&e.activeVU)
	if newTarget > currentActive {
		// Scale up
		diff := newTarget - currentActive
		e.mu.Lock()
		ctx := e.ctx
		e.mu.Unlock()
		if ctx != nil {
			for i := int64(0); i < diff; i++ {
				e.spawnVU(ctx)
			}
		}
	}
}

// Stop terminates all running VUs and waits for them to exit.
func (e *ClosedExecutor) Stop() {
	e.mu.Lock()
	if e.cancel != nil {
		e.cancel()
	}
	e.mu.Unlock()

	e.wg.Wait()
}
