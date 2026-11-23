package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"api-load-testing-engine/internal/metrics"
)

func TestOpenExecutor_ArrivalRate(t *testing.T) {
	agg := metrics.NewLocalAggregator("test-open-rate")
	var dispatches int64

	runner := func(ctx context.Context, vuID int, intendedArrival time.Time) error {
		atomic.AddInt64(&dispatches, 1)
		return nil
	}

	// 50 RPS = 1 request every 20ms. In 200ms, expect ~10 requests
	exec := NewOpenExecutor(50, 500, runner, agg)

	ctx, cancel := context.WithTimeout(context.Background(), 210*time.Millisecond)
	defer cancel()

	_ = exec.Start(ctx)
	<-ctx.Done()
	exec.Stop()

	count := atomic.LoadInt64(&dispatches)
	if count < 7 || count > 14 {
		t.Fatalf("expected ~10 arrivals for 50 RPS in 200ms, got %d", count)
	}
}

func TestOpenExecutor_PoolSaturationAndDroppedIterations(t *testing.T) {
	agg := metrics.NewLocalAggregator("test-open-saturated")

	// Runner takes 80ms to complete
	runner := func(ctx context.Context, vuID int, intendedArrival time.Time) error {
		time.Sleep(80 * time.Millisecond)
		return nil
	}

	// Max 2 in-flight VUs, but arrival rate is 100 RPS (1 arrival every 10ms)
	exec := NewOpenExecutor(100, 2, runner, agg)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_ = exec.Start(ctx)
	<-ctx.Done()
	exec.Stop()

	batch := agg.SnapshotAndReset()
	if batch.Counters.DroppedIterations == 0 {
		t.Fatalf("expected dropped iterations due to pool saturation, got 0")
	}
}
