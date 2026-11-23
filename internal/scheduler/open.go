package scheduler

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"api-load-testing-engine/internal/metrics"
)

// OpenExecutor generates load at an arrival rate lambda on a fixed timeline,
// with a bounded in-flight worker pool to prevent runaway resource usage.
type OpenExecutor struct {
	targetRPS  float64
	maxVUs     int
	runner     IterationRunner
	aggregator *metrics.LocalAggregator

	mu       sync.Mutex
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	sem      chan struct{}
	activeVU int64
}

// NewOpenExecutor creates an open-model arrival-rate scheduler.
func NewOpenExecutor(targetRPS float64, maxVUs int, runner IterationRunner, agg *metrics.LocalAggregator) *OpenExecutor {
	if maxVUs <= 0 {
		maxVUs = 1000
	}
	return &OpenExecutor{
		targetRPS:  targetRPS,
		maxVUs:     maxVUs,
		runner:     runner,
		aggregator: agg,
		sem:        make(chan struct{}, maxVUs),
	}
}

// Start launches the fixed-timeline arrival dispatcher.
func (e *OpenExecutor) Start(ctx context.Context) error {
	e.mu.Lock()
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	e.mu.Unlock()

	e.wg.Add(1)
	go e.runDispatcher(ctx)

	return nil
}

func (e *OpenExecutor) runDispatcher(ctx context.Context) {
	defer e.wg.Done()

	startTime := time.Now()
	var iterationCount int64

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		e.mu.Lock()
		rps := e.targetRPS
		e.mu.Unlock()

		if rps <= 0 {
			time.Sleep(10 * time.Millisecond)
			startTime = time.Now()
			iterationCount = 0
			continue
		}

		// Calculate exact intended arrival time for iteration k to avoid timer drift
		intervalSec := 1.0 / rps
		intendedDuration := time.Duration(float64(iterationCount) * intervalSec * float64(time.Second))
		intendedTime := startTime.Add(intendedDuration)

		now := time.Now()
		if intendedTime.After(now) {
			sleepDuration := intendedTime.Sub(now)
			// High precision sleep
			if sleepDuration > 2*time.Millisecond {
				time.Sleep(sleepDuration - time.Millisecond)
			}
			// Spin-wait final sub-millisecond for microsecond accuracy
			for time.Now().Before(intendedTime) {
			}
		}

		iterationCount++

		// Attempt to acquire in-flight VU slot
		select {
		case e.sem <- struct{}{}:
			// Acquired slot
			atomic.AddInt64(&e.activeVU, 1)
			if e.aggregator != nil {
				e.aggregator.SetActiveVUs(uint32(atomic.LoadInt64(&e.activeVU)))
			}

			e.wg.Add(1)
			go func(intended time.Time) {
				defer e.wg.Done()
				defer func() {
					<-e.sem
					atomic.AddInt64(&e.activeVU, -1)
					if e.aggregator != nil {
						e.aggregator.SetActiveVUs(uint32(atomic.LoadInt64(&e.activeVU)))
					}
				}()

				_ = e.runner(ctx, 0, intended)
			}(intendedTime)

		default:
			// In-flight pool exhausted: record dropped iteration
			if e.aggregator != nil {
				e.aggregator.AddDroppedIterations(1)
			}
		}
	}
}

// UpdateTarget updates the target arrival rate in RPS.
func (e *OpenExecutor) UpdateTarget(target float64) {
	if math.IsNaN(target) || target < 0 {
		target = 0
	}
	e.mu.Lock()
	e.targetRPS = target
	e.mu.Unlock()
}

// Stop stops the arrival loop and waits for in-flight requests to drain.
func (e *OpenExecutor) Stop() {
	e.mu.Lock()
	if e.cancel != nil {
		e.cancel()
	}
	e.mu.Unlock()

	e.wg.Wait()
}
