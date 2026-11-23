package scheduler

import (
	"context"
	"testing"
	"time"

	"api-load-testing-engine/internal/metrics"
)

func BenchmarkOpenExecutor_DispatchLoop(b *testing.B) {
	agg := metrics.NewLocalAggregator("bench-open")
	runner := func(ctx context.Context, vuID int, intendedArrival time.Time) error {
		return nil
	}

	exec := NewOpenExecutor(100000, 10000, runner, agg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = exec.Start(ctx)

	b.ResetTimer()
	time.Sleep(100 * time.Millisecond)
	b.StopTimer()

	cancel()
	exec.Stop()
}

func BenchmarkLocalAggregator_RecordDuration(b *testing.B) {
	agg := metrics.NewLocalAggregator("bench-agg")
	d := 25 * time.Millisecond

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			agg.RecordDuration("http_req_duration", d)
		}
	})
}
