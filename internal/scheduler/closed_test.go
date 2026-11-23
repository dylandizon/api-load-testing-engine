package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"api-load-testing-engine/internal/metrics"
)

func TestClosedExecutor_ExecutionAndThinkTime(t *testing.T) {
	agg := metrics.NewLocalAggregator("test-closed")
	var executionCount int64

	runner := func(ctx context.Context, vuID int, intendedArrival time.Time) error {
		atomic.AddInt64(&executionCount, 1)
		time.Sleep(5 * time.Millisecond) // simulate brief work
		return nil
	}

	exec := NewClosedExecutor(3, 10*time.Millisecond, runner, agg)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	err := exec.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start closed executor: %v", err)
	}

	// Let it run for the timeout duration
	<-ctx.Done()
	exec.Stop()

	count := atomic.LoadInt64(&executionCount)
	// With 3 VUs running (5ms work + 10ms think = ~15ms per cycle), over 80ms each VU runs ~5 times -> total ~15.
	if count < 6 {
		t.Fatalf("expected at least 6 iterations, got %d", count)
	}
}

func TestClosedExecutor_ScaleVUs(t *testing.T) {
	agg := metrics.NewLocalAggregator("test-closed-scale")
	var executionCount int64

	runner := func(ctx context.Context, vuID int, intendedArrival time.Time) error {
		atomic.AddInt64(&executionCount, 1)
		time.Sleep(5 * time.Millisecond)
		return nil
	}

	exec := NewClosedExecutor(1, 10*time.Millisecond, runner, agg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = exec.Start(ctx)
	time.Sleep(30 * time.Millisecond)

	// Scale up to 4 VUs
	exec.UpdateTarget(4)
	time.Sleep(40 * time.Millisecond)

	// Scale down to 1 VU
	exec.UpdateTarget(1)
	time.Sleep(30 * time.Millisecond)

	cancel()
	exec.Stop()

	if atomic.LoadInt64(&executionCount) < 5 {
		t.Fatalf("expected iterations across scaling, got %d", atomic.LoadInt64(&executionCount))
	}
}
