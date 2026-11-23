package metrics

import (
	"fmt"
	"log"
	"sync"
	"time"

	pb "api-load-testing-engine/proto"
	"github.com/HdrHistogram/hdrhistogram-go"
)

// MetricRollup represents a persisted merged 1-second rollup record.
type MetricRollup struct {
	Timestamp time.Time
	RunID     string
	Metric    string
	Count     int64
	Sum       float64
	Min       float64
	Max       float64
	P50       float64
	P90       float64
	P95       float64
	P99       float64
	P999      float64
	Histogram []byte // compressed HdrHistogram blob
}

// RunCountersRecord represents a persisted merged 1-second counters record.
type RunCountersRecord struct {
	Timestamp         time.Time
	RunID             string
	Requests          int64
	Errors4xx         int64
	Errors5xx         int64
	Timeouts          int64
	ConnResets        int64
	AssertionFailures int64
	BytesSent         int64
	BytesReceived     int64
	ActiveVUs         int32
	DroppedIterations int64
}

// WindowBucket aggregates worker batches falling into the same 1-second timestamp window.
type WindowBucket struct {
	Timestamp         time.Time
	RunID             string
	histograms        map[string]*hdrhistogram.Histogram
	requests          int64
	errors4xx         int64
	errors5xx         int64
	timeouts          int64
	connResets        int64
	assertionFailures int64
	bytesSent         int64
	bytesReceived     int64
	activeVUs         int32
	droppedIterations int64
	workerCount       int
	createdAt         time.Time
}

// RollupSink defines the interface where merged rollups and counters are flushed.
type RollupSink interface {
	SaveRollup(rollup MetricRollup) error
	SaveCounters(counters RunCountersRecord) error
}

// Merger combines per-second worker MetricsBatches losslessly.
type Merger struct {
	mu          sync.Mutex
	sink        RollupSink
	windows     map[string]*WindowBucket // key: runID:timestampUnix
	lastSeq     map[string]uint64        // key: workerID -> lastSeq
	gracePeriod time.Duration
	stopCh      chan struct{}
}

// NewMerger creates a new metrics merger with a window flush grace period.
func NewMerger(sink RollupSink, gracePeriod time.Duration) *Merger {
	if gracePeriod <= 0 {
		gracePeriod = 1500 * time.Millisecond
	}
	m := &Merger{
		sink:        sink,
		windows:     make(map[string]*WindowBucket),
		lastSeq:     make(map[string]uint64),
		gracePeriod: gracePeriod,
		stopCh:      make(chan struct{}),
	}
	go m.flusherLoop()
	return m
}

// IngestBatch ingests a worker MetricsBatch, merging histograms and summing counters.
func (m *Merger) IngestBatch(workerID string, batch *pb.MetricsBatch) error {
	if batch == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Deduplication / sequence gap check
	if last, exists := m.lastSeq[workerID]; exists {
		if batch.Seq <= last {
			log.Printf("[merger] duplicate or out-of-order batch from %s (seq %d <= last %d)", workerID, batch.Seq, last)
			return nil
		}
	}
	m.lastSeq[workerID] = batch.Seq

	// Truncate to second
	var windowTime time.Time
	if batch.WindowStart != nil {
		windowTime = batch.WindowStart.AsTime().Truncate(time.Second)
	} else {
		windowTime = time.Now().Truncate(time.Second)
	}

	bucketKey := fmt.Sprintf("%s:%d", batch.RunId, windowTime.Unix())
	bucket, exists := m.windows[bucketKey]
	if !exists {
		bucket = &WindowBucket{
			Timestamp:  windowTime,
			RunID:      batch.RunId,
			histograms: make(map[string]*hdrhistogram.Histogram),
			createdAt:  time.Now(),
		}
		m.windows[bucketKey] = bucket
	}

	bucket.workerCount++

	// Merge counters
	if c := batch.Counters; c != nil {
		bucket.requests += int64(c.Requests)
		bucket.errors4xx += int64(c.Errors_4Xx)
		bucket.errors5xx += int64(c.Errors_5Xx)
		bucket.timeouts += int64(c.Timeouts)
		bucket.connResets += int64(c.ConnResets)
		bucket.assertionFailures += int64(c.AssertionFailures)
		bucket.bytesSent += int64(c.BytesSent)
		bucket.bytesReceived += int64(c.BytesReceived)
		bucket.activeVUs += int32(c.ActiveVus)
		bucket.droppedIterations += int64(c.DroppedIterations)
	}

	// Lossless histogram merge
	for _, hm := range batch.Histograms {
		if len(hm.HdrHistogram) == 0 {
			continue
		}
		decoded, err := DecodeHistogram(hm.HdrHistogram)
		if err != nil {
			log.Printf("[merger] decode histogram error for %s: %v", hm.Name, err)
			continue
		}

		targetHist, exists := bucket.histograms[hm.Name]
		if !exists {
			bucket.histograms[hm.Name] = decoded
		} else {
			targetHist.Merge(decoded)
		}
	}

	return nil
}

func (m *Merger) flusherLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			m.flushExpired(0) // flush all on stop
			return
		case <-ticker.C:
			m.flushExpired(m.gracePeriod)
		}
	}
}

func (m *Merger) flushExpired(grace time.Duration) {
	m.mu.Lock()
	now := time.Now()
	var toFlush []*WindowBucket

	for key, bucket := range m.windows {
		if now.Sub(bucket.createdAt) >= grace {
			toFlush = append(toFlush, bucket)
			delete(m.windows, key)
		}
	}
	m.mu.Unlock()

	for _, bucket := range toFlush {
		m.flushBucket(bucket)
	}
}

func (m *Merger) flushBucket(bucket *WindowBucket) {
	if m.sink == nil {
		return
	}

	// 1. Flush counters
	countersRec := RunCountersRecord{
		Timestamp:         bucket.Timestamp,
		RunID:             bucket.RunID,
		Requests:          bucket.requests,
		Errors4xx:         bucket.errors4xx,
		Errors5xx:         bucket.errors5xx,
		Timeouts:          bucket.timeouts,
		ConnResets:        bucket.connResets,
		AssertionFailures: bucket.assertionFailures,
		BytesSent:         bucket.bytesSent,
		BytesReceived:     bucket.bytesReceived,
		ActiveVUs:         bucket.activeVUs,
		DroppedIterations: bucket.droppedIterations,
	}
	_ = m.sink.SaveCounters(countersRec)

	// 2. Flush histograms and precomputed rollups
	for metricName, h := range bucket.histograms {
		summary := ComputeSummary(h)
		blob, _ := h.Encode(hdrhistogram.V2CompressedEncodingCookieBase)

		rollup := MetricRollup{
			Timestamp: bucket.Timestamp,
			RunID:     bucket.RunID,
			Metric:    metricName,
			Count:     summary.Count,
			Sum:       summary.Mean * float64(summary.Count),
			Min:       summary.Min,
			Max:       summary.Max,
			P50:       summary.P50,
			P90:       summary.P90,
			P95:       summary.P95,
			P99:       summary.P99,
			P999:      summary.P999,
			Histogram: blob,
		}
		_ = m.sink.SaveRollup(rollup)
	}
}

// FlushAll immediately flushes all pending window buckets to the sink.
func (m *Merger) FlushAll() {
	m.flushExpired(0)
}

// Stop flushes remaining buckets and terminates the flusher routine.
func (m *Merger) Stop() {
	close(m.stopCh)
	m.FlushAll()
}
