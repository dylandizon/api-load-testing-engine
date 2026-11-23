package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "api-load-testing-engine/proto"
	"github.com/HdrHistogram/hdrhistogram-go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// MinTrackableValue is 1 microsecond
	MinTrackableValue = 1
	// MaxTrackableValue is 1 hour in microseconds (3,600,000,000 us)
	MaxTrackableValue = 3600 * 1000 * 1000
	// SigFigs precision
	SigFigs = 3
)

// PercentileSummary contains precomputed quantiles and stats.
type PercentileSummary struct {
	Count int64
	Min   float64
	Max   float64
	Mean  float64
	P50   float64
	P90   float64
	P95   float64
	P99   float64
	P999  float64
}

// LocalAggregator aggregates per-second histograms and counters.
type LocalAggregator struct {
	runID     string
	mu        sync.Mutex
	histMap   map[string]*hdrhistogram.Histogram
	windowStart time.Time

	// Atomic counters for high performance
	requests           uint64
	errors4xx          uint64
	errors5xx          uint64
	timeouts           uint64
	connResets         uint64
	assertionFailures  uint64
	bytesSent          uint64
	bytesReceived      uint64
	activeVUs          uint32
	droppedIterations  uint64
	seq                uint64
}

// NewLocalAggregator creates a new local aggregator for a run.
func NewLocalAggregator(runID string) *LocalAggregator {
	return &LocalAggregator{
		runID:       runID,
		histMap:     make(map[string]*hdrhistogram.Histogram),
		windowStart: time.Now(),
	}
}

// RecordDuration records a duration for a named metric into HdrHistogram (in microseconds).
func (a *LocalAggregator) RecordDuration(name string, d time.Duration) {
	us := d.Microseconds()
	if us < MinTrackableValue {
		us = MinTrackableValue
	}
	if us > MaxTrackableValue {
		us = MaxTrackableValue
	}

	a.mu.Lock()
	h, exists := a.histMap[name]
	if !exists {
		h = hdrhistogram.New(MinTrackableValue, MaxTrackableValue, SigFigs)
		a.histMap[name] = h
	}
	_ = h.RecordValue(us)
	a.mu.Unlock()
}

// Counter update methods
func (a *LocalAggregator) AddRequests(n uint64)          { atomic.AddUint64(&a.requests, n) }
func (a *LocalAggregator) AddErrors4xx(n uint64)         { atomic.AddUint64(&a.errors4xx, n) }
func (a *LocalAggregator) AddErrors5xx(n uint64)         { atomic.AddUint64(&a.errors5xx, n) }
func (a *LocalAggregator) AddTimeout()                   { atomic.AddUint64(&a.timeouts, 1) }
func (a *LocalAggregator) AddConnReset()                 { atomic.AddUint64(&a.connResets, 1) }
func (a *LocalAggregator) AddAssertionFailure()          { atomic.AddUint64(&a.assertionFailures, 1) }
func (a *LocalAggregator) AddBytesSent(n uint64)         { atomic.AddUint64(&a.bytesSent, n) }
func (a *LocalAggregator) AddBytesReceived(n uint64)     { atomic.AddUint64(&a.bytesReceived, n) }
func (a *LocalAggregator) SetActiveVUs(vus uint32)       { atomic.StoreUint32(&a.activeVUs, vus) }
func (a *LocalAggregator) AddDroppedIterations(n uint64) { atomic.AddUint64(&a.droppedIterations, n) }

// SnapshotAndReset rotates active histograms and counters, returning a MetricsBatch.
func (a *LocalAggregator) SnapshotAndReset() *pb.MetricsBatch {
	windowEnd := time.Now()

	a.mu.Lock()
	oldHistMap := a.histMap
	oldWindowStart := a.windowStart

	// Reset for next window
	a.histMap = make(map[string]*hdrhistogram.Histogram)
	a.windowStart = windowEnd
	a.mu.Unlock()

	// Swap atomic counters
	req := atomic.SwapUint64(&a.requests, 0)
	err4 := atomic.SwapUint64(&a.errors4xx, 0)
	err5 := atomic.SwapUint64(&a.errors5xx, 0)
	to := atomic.SwapUint64(&a.timeouts, 0)
	cr := atomic.SwapUint64(&a.connResets, 0)
	af := atomic.SwapUint64(&a.assertionFailures, 0)
	bs := atomic.SwapUint64(&a.bytesSent, 0)
	br := atomic.SwapUint64(&a.bytesReceived, 0)
	dropped := atomic.SwapUint64(&a.droppedIterations, 0)
	vus := atomic.LoadUint32(&a.activeVUs)

	seq := atomic.AddUint64(&a.seq, 1)

	// Serialize histograms
	var histMetrics []*pb.HistogramMetric
	for name, h := range oldHistMap {
		if h.TotalCount() == 0 {
			continue
		}
		buf, err := h.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
		if err == nil {
			histMetrics = append(histMetrics, &pb.HistogramMetric{
				Name:         name,
				HdrHistogram: buf,
			})
		}
	}

	return &pb.MetricsBatch{
		RunId:       a.runID,
		Seq:         seq,
		WindowStart: timestamppb.New(oldWindowStart),
		WindowEnd:   timestamppb.New(windowEnd),
		Histograms:  histMetrics,
		Counters: &pb.Counters{
			Requests:          req,
			Errors_4Xx:        err4,
			Errors_5Xx:        err5,
			Timeouts:          to,
			ConnResets:        cr,
			AssertionFailures: af,
			BytesSent:         bs,
			BytesReceived:     br,
			ActiveVus:         vus,
			DroppedIterations: dropped,
		},
	}
}

// DecodeHistogram decodes compressed HdrHistogram bytes.
func DecodeHistogram(data []byte) (*hdrhistogram.Histogram, error) {
	if len(data) == 0 {
		return hdrhistogram.New(MinTrackableValue, MaxTrackableValue, SigFigs), nil
	}
	h, err := hdrhistogram.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode compressed hdrhistogram: %w", err)
	}
	return h, nil
}

// ComputeSummary extracts min, max, mean, and percentiles from an HdrHistogram (values in milliseconds).
func ComputeSummary(h *hdrhistogram.Histogram) PercentileSummary {
	if h == nil || h.TotalCount() == 0 {
		return PercentileSummary{}
	}

	toMS := func(us int64) float64 {
		return float64(us) / 1000.0
	}

	return PercentileSummary{
		Count: h.TotalCount(),
		Min:   toMS(h.Min()),
		Max:   toMS(h.Max()),
		Mean:  float64(h.Mean()) / 1000.0,
		P50:   toMS(h.ValueAtQuantile(50.0)),
		P90:   toMS(h.ValueAtQuantile(90.0)),
		P95:   toMS(h.ValueAtQuantile(95.0)),
		P99:   toMS(h.ValueAtQuantile(99.0)),
		P999:  toMS(h.ValueAtQuantile(99.9)),
	}
}
