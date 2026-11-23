package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"api-load-testing-engine/internal/metrics"
)

// RunRecord represents a persistent record of a test run.
type RunRecord struct {
	ID            string    `json:"id"`
	TestID        string    `json:"test_id"`
	Definition    []byte    `json:"definition"`
	Status        string    `json:"status"`
	CurrentStage  int       `json:"current_stage"`
	Degraded      bool      `json:"degraded"`
	ExitCode      int       `json:"exit_code"`
	FailureReason string    `json:"failure_reason"`
	CreatedAt     time.Time `json:"created_at"`
	StartedAt     time.Time `json:"started_at"`
	EndedAt       time.Time `json:"ended_at"`
}

// FailureTraceRecord represents a sampled failed request/response trace.
type FailureTraceRecord struct {
	ID           int64     `json:"id"`
	RunID        string    `json:"run_id"`
	WorkerID     string    `json:"worker_id"`
	Timestamp    time.Time `json:"timestamp"`
	ErrorType    string    `json:"error_type"`
	URL          string    `json:"url"`
	Method       string    `json:"method"`
	StatusCode   int       `json:"status_code"`
	ResponseBody string    `json:"response_body"`
	ObjectKey    string    `json:"object_key"`
}

// Store defines operations for storing and querying metrics and test metadata.
type Store interface {
	metrics.RollupSink
	GetRollups(ctx context.Context, runID string, metric string) ([]metrics.MetricRollup, error)
	GetCounters(ctx context.Context, runID string) ([]metrics.RunCountersRecord, error)

	// Run persistence and crash recovery
	SaveRun(ctx context.Context, r RunRecord) error
	UpdateRun(ctx context.Context, runID string, status string, currentStage int, degraded bool, exitCode int, failureReason string) error
	GetRun(ctx context.Context, runID string) (*RunRecord, error)
	GetActiveRuns(ctx context.Context) ([]*RunRecord, error)

	// Failure trace sampling
	SaveFailureTrace(ctx context.Context, trace FailureTraceRecord) error
	GetFailureTraces(ctx context.Context, runID string) ([]FailureTraceRecord, error)

	Close() error
}

// MemStore provides an in-memory implementation of Store for standalone testing.
type MemStore struct {
	mu       sync.RWMutex
	rollups  map[string][]metrics.MetricRollup     // runID -> list
	counters map[string][]metrics.RunCountersRecord // runID -> list
	runs     map[string]*RunRecord                 // runID -> record
	traces   map[string][]FailureTraceRecord       // runID -> list
}

// NewMemStore creates a new in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{
		rollups:  make(map[string][]metrics.MetricRollup),
		counters: make(map[string][]metrics.RunCountersRecord),
		runs:     make(map[string]*RunRecord),
		traces:   make(map[string][]FailureTraceRecord),
	}
}

// SaveRollup stores a merged metric rollup in memory.
func (s *MemStore) SaveRollup(rollup metrics.MetricRollup) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rollups[rollup.RunID] = append(s.rollups[rollup.RunID], rollup)
	return nil
}

// SaveCounters stores a merged run counters record in memory.
func (s *MemStore) SaveCounters(counters metrics.RunCountersRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counters[counters.RunID] = append(s.counters[counters.RunID], counters)
	return nil
}

// GetRollups retrieves rollups for a run, optionally filtered by metric name.
func (s *MemStore) GetRollups(ctx context.Context, runID string, metric string) ([]metrics.MetricRollup, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	all, exists := s.rollups[runID]
	if !exists {
		return nil, nil
	}

	if metric == "" {
		res := make([]metrics.MetricRollup, len(all))
		copy(res, all)
		return res, nil
	}

	var filtered []metrics.MetricRollup
	for _, r := range all {
		if r.Metric == metric {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

// GetCounters retrieves all counter records for a run.
func (s *MemStore) GetCounters(ctx context.Context, runID string) ([]metrics.RunCountersRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	all, exists := s.counters[runID]
	if !exists {
		return nil, nil
	}
	res := make([]metrics.RunCountersRecord, len(all))
	copy(res, all)
	return res, nil
}

// SaveRun stores a new run record.
func (s *MemStore) SaveRun(ctx context.Context, r RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[r.ID] = &r
	return nil
}

// UpdateRun updates status and stage of an existing run.
func (s *MemStore) UpdateRun(ctx context.Context, runID string, status string, currentStage int, degraded bool, exitCode int, failureReason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, exists := s.runs[runID]
	if !exists {
		return fmt.Errorf("run %s not found", runID)
	}
	r.Status = status
	r.CurrentStage = currentStage
	r.Degraded = degraded
	r.ExitCode = exitCode
	if failureReason != "" {
		r.FailureReason = failureReason
	}
	if status == "COMPLETED" || status == "ABORTED" || status == "FAILED" {
		r.EndedAt = time.Now()
	} else if status == "RUNNING" && r.StartedAt.IsZero() {
		r.StartedAt = time.Now()
	}
	return nil
}

// GetRun retrieves a run record by ID.
func (s *MemStore) GetRun(ctx context.Context, runID string) (*RunRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	r, exists := s.runs[runID]
	if !exists {
		return nil, fmt.Errorf("run %s not found", runID)
	}
	copy := *r
	return &copy, nil
}

// GetActiveRuns retrieves all runs that have not completed or failed.
func (s *MemStore) GetActiveRuns(ctx context.Context) ([]*RunRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var active []*RunRecord
	for _, r := range s.runs {
		if r.Status == "PENDING" || r.Status == "ASSIGNING" || r.Status == "WAITING_READY" || r.Status == "RUNNING" || r.Status == "DRAINING" {
			copy := *r
			active = append(active, &copy)
		}
	}
	return active, nil
}

// SaveFailureTrace stores a sampled failure trace in memory.
func (s *MemStore) SaveFailureTrace(ctx context.Context, trace FailureTraceRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	trace.ID = int64(len(s.traces[trace.RunID]) + 1)
	s.traces[trace.RunID] = append(s.traces[trace.RunID], trace)
	return nil
}

// GetFailureTraces retrieves all failure traces for a given run.
func (s *MemStore) GetFailureTraces(ctx context.Context, runID string) ([]FailureTraceRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	traces := s.traces[runID]
	res := make([]FailureTraceRecord, len(traces))
	copy(res, traces)
	return res, nil
}

// Close is a no-op for MemStore.
func (s *MemStore) Close() error {
	return nil
}
