package metrics

import (
	"testing"
	"time"
)

func TestLocalAggregator_RecordAndSnapshot(t *testing.T) {
	agg := NewLocalAggregator("test-run-1")

	// Record latencies
	agg.RecordDuration("http_req_duration", 10*time.Millisecond)
	agg.RecordDuration("http_req_duration", 20*time.Millisecond)
	agg.RecordDuration("http_req_duration", 30*time.Millisecond)
	agg.RecordDuration("http_req_duration", 40*time.Millisecond)
	agg.RecordDuration("http_req_duration", 50*time.Millisecond)

	// Update counters
	agg.AddRequests(5)
	agg.AddErrors4xx(1)
	agg.AddErrors5xx(0)
	agg.AddTimeout()
	agg.AddBytesSent(500)
	agg.AddBytesReceived(2500)
	agg.SetActiveVUs(10)
	agg.AddDroppedIterations(2)

	// Snapshot
	batch := agg.SnapshotAndReset()
	if batch.RunId != "test-run-1" {
		t.Fatalf("expected runId test-run-1, got %s", batch.RunId)
	}
	if batch.Seq != 1 {
		t.Fatalf("expected seq 1, got %d", batch.Seq)
	}
	if batch.Counters.Requests != 5 {
		t.Fatalf("expected 5 requests, got %d", batch.Counters.Requests)
	}
	if batch.Counters.Errors_4Xx != 1 {
		t.Fatalf("expected 1 4xx error, got %d", batch.Counters.Errors_4Xx)
	}
	if batch.Counters.Timeouts != 1 {
		t.Fatalf("expected 1 timeout, got %d", batch.Counters.Timeouts)
	}
	if batch.Counters.ActiveVus != 10 {
		t.Fatalf("expected 10 active VUs, got %d", batch.Counters.ActiveVus)
	}
	if batch.Counters.DroppedIterations != 2 {
		t.Fatalf("expected 2 dropped iterations, got %d", batch.Counters.DroppedIterations)
	}
	if len(batch.Histograms) != 1 {
		t.Fatalf("expected 1 histogram, got %d", len(batch.Histograms))
	}

	histMsg := batch.Histograms[0]
	if histMsg.Name != "http_req_duration" {
		t.Fatalf("expected metric name http_req_duration, got %s", histMsg.Name)
	}

	// Decode histogram
	h, err := DecodeHistogram(histMsg.HdrHistogram)
	if err != nil {
		t.Fatalf("failed to decode histogram: %v", err)
	}
	if h.TotalCount() != 5 {
		t.Fatalf("expected total count 5, got %d", h.TotalCount())
	}

	summary := ComputeSummary(h)
	if summary.Count != 5 {
		t.Fatalf("expected count 5, got %d", summary.Count)
	}
	if summary.Min < 9.0 || summary.Min > 11.0 {
		t.Fatalf("expected min ~10ms, got %f", summary.Min)
	}
	if summary.Max < 49.0 || summary.Max > 51.0 {
		t.Fatalf("expected max ~50ms, got %f", summary.Max)
	}

	// Verify next snapshot is clean
	emptyBatch := agg.SnapshotAndReset()
	if emptyBatch.Counters.Requests != 0 {
		t.Fatalf("expected 0 requests in reset snapshot, got %d", emptyBatch.Counters.Requests)
	}
	if len(emptyBatch.Histograms) != 0 {
		t.Fatalf("expected 0 histograms in reset snapshot, got %d", len(emptyBatch.Histograms))
	}
}
