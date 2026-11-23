package metrics

import (
	"math"
	"testing"
	"time"

	pb "api-load-testing-engine/proto"
	"github.com/HdrHistogram/hdrhistogram-go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mockSink struct {
	rollups  []MetricRollup
	counters []RunCountersRecord
}

func (s *mockSink) SaveRollup(r MetricRollup) error {
	s.rollups = append(s.rollups, r)
	return nil
}

func (s *mockSink) SaveCounters(c RunCountersRecord) error {
	s.counters = append(s.counters, c)
	return nil
}

func TestHdrHistogramLosslessMerge(t *testing.T) {
	// Control histogram recording all samples directly
	control := hdrhistogram.New(MinTrackableValue, MaxTrackableValue, SigFigs)
	samples1 := []int64{10000, 20000, 30000, 40000} // us
	samples2 := []int64{15000, 25000, 35000, 80000} // us

	for _, s := range samples1 {
		_ = control.RecordValue(s)
	}
	for _, s := range samples2 {
		_ = control.RecordValue(s)
	}

	// Worker 1 histogram
	w1 := hdrhistogram.New(MinTrackableValue, MaxTrackableValue, SigFigs)
	for _, s := range samples1 {
		_ = w1.RecordValue(s)
	}
	b1, err := w1.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
	if err != nil {
		t.Fatalf("worker 1 encode error: %v", err)
	}

	// Worker 2 histogram
	w2 := hdrhistogram.New(MinTrackableValue, MaxTrackableValue, SigFigs)
	for _, s := range samples2 {
		_ = w2.RecordValue(s)
	}
	b2, err := w2.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
	if err != nil {
		t.Fatalf("worker 2 encode error: %v", err)
	}

	sink := &mockSink{}
	merger := NewMerger(sink, 10*time.Millisecond)
	defer merger.Stop()

	now := time.Now()
	nowProto := timestamppb.New(now)

	// Ingest Worker 1
	_ = merger.IngestBatch("w1", &pb.MetricsBatch{
		RunId:       "run-merge-test",
		Seq:         1,
		WindowStart: nowProto,
		Histograms: []*pb.HistogramMetric{
			{Name: "http_req_duration", HdrHistogram: b1},
		},
		Counters: &pb.Counters{Requests: 4},
	})

	// Ingest Worker 2
	_ = merger.IngestBatch("w2", &pb.MetricsBatch{
		RunId:       "run-merge-test",
		Seq:         1,
		WindowStart: nowProto,
		Histograms: []*pb.HistogramMetric{
			{Name: "http_req_duration", HdrHistogram: b2},
		},
		Counters: &pb.Counters{Requests: 4},
	})

	// Flush immediately for verification
	merger.FlushAll()

	if len(sink.counters) != 1 {
		t.Fatalf("expected 1 merged counters record, got %d", len(sink.counters))
	}
	if sink.counters[0].Requests != 8 {
		t.Fatalf("expected 8 merged requests, got %d", sink.counters[0].Requests)
	}

	if len(sink.rollups) != 1 {
		t.Fatalf("expected 1 merged rollup record, got %d", len(sink.rollups))
	}

	mergedRollup := sink.rollups[0]
	if mergedRollup.Count != 8 {
		t.Fatalf("expected 8 merged samples, got %d", mergedRollup.Count)
	}

	controlSummary := ComputeSummary(control)
	if math.Abs(mergedRollup.P50-controlSummary.P50) > 0.001 {
		t.Fatalf("p50 mismatch: merged=%f control=%f", mergedRollup.P50, controlSummary.P50)
	}
	if math.Abs(mergedRollup.P95-controlSummary.P95) > 0.001 {
		t.Fatalf("p95 mismatch: merged=%f control=%f", mergedRollup.P95, controlSummary.P95)
	}
	if math.Abs(mergedRollup.Max-controlSummary.Max) > 0.001 {
		t.Fatalf("max mismatch: merged=%f control=%f", mergedRollup.Max, controlSummary.Max)
	}
}

func TestMerger_Deduplication(t *testing.T) {
	sink := &mockSink{}
	merger := NewMerger(sink, 10*time.Millisecond)
	defer merger.Stop()

	now := timestamppb.Now()

	// First batch seq 1
	_ = merger.IngestBatch("worker-dedupe", &pb.MetricsBatch{
		RunId:       "run-dedupe",
		Seq:         1,
		WindowStart: now,
		Counters:    &pb.Counters{Requests: 5},
	})

	// Duplicate batch seq 1
	_ = merger.IngestBatch("worker-dedupe", &pb.MetricsBatch{
		RunId:       "run-dedupe",
		Seq:         1,
		WindowStart: now,
		Counters:    &pb.Counters{Requests: 5},
	})

	merger.FlushAll()

	if len(sink.counters) != 1 {
		t.Fatalf("expected 1 merged counters record, got %d", len(sink.counters))
	}
	// Requests must be 5 (duplicate was dropped)
	if sink.counters[0].Requests != 5 {
		t.Fatalf("expected 5 requests after deduplication, got %d", sink.counters[0].Requests)
	}
}
