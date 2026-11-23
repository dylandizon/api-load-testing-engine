package store

import (
	"context"
	"testing"
	"time"

	"api-load-testing-engine/internal/metrics"
)

func TestMemStore_SaveAndRetrieve(t *testing.T) {
	s := NewMemStore()
	defer s.Close()

	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	// Save rollups
	r1 := metrics.MetricRollup{
		Timestamp: now,
		RunID:     "run-test-store",
		Metric:    "http_req_duration",
		Count:     100,
		P50:       15.2,
		P95:       45.8,
		P99:       92.1,
	}
	r2 := metrics.MetricRollup{
		Timestamp: now,
		RunID:     "run-test-store",
		Metric:    "iteration_duration",
		Count:     50,
		P50:       30.5,
		P95:       70.1,
		P99:       120.0,
	}

	_ = s.SaveRollup(r1)
	_ = s.SaveRollup(r2)

	// Save counters
	c1 := metrics.RunCountersRecord{
		Timestamp: now,
		RunID:     "run-test-store",
		Requests:  150,
		Errors4xx: 2,
		Errors5xx: 1,
		ActiveVUs: 10,
	}
	_ = s.SaveCounters(c1)

	// Retrieve filtered by metric
	rollups, err := s.GetRollups(ctx, "run-test-store", "http_req_duration")
	if err != nil {
		t.Fatalf("failed to get rollups: %v", err)
	}
	if len(rollups) != 1 {
		t.Fatalf("expected 1 rollup, got %d", len(rollups))
	}
	if rollups[0].P95 != 45.8 {
		t.Fatalf("expected p95 45.8, got %f", rollups[0].P95)
	}

	// Retrieve all rollups
	allRollups, err := s.GetRollups(ctx, "run-test-store", "")
	if err != nil {
		t.Fatalf("failed to get all rollups: %v", err)
	}
	if len(allRollups) != 2 {
		t.Fatalf("expected 2 rollups, got %d", len(allRollups))
	}

	// Retrieve counters
	counters, err := s.GetCounters(ctx, "run-test-store")
	if err != nil {
		t.Fatalf("failed to get counters: %v", err)
	}
	if len(counters) != 1 {
		t.Fatalf("expected 1 counter record, got %d", len(counters))
	}
	if counters[0].Requests != 150 || counters[0].Errors4xx != 2 {
		t.Fatalf("counter values mismatch: %+v", counters[0])
	}
}
