package store

import (
	"context"
	"sync"

	"api-load-testing-engine/internal/metrics"
)

// Store defines operations for storing and querying metrics and test metadata.
type Store interface {
	metrics.RollupSink
	GetRollups(ctx context.Context, runID string, metric string) ([]metrics.MetricRollup, error)
	GetCounters(ctx context.Context, runID string) ([]metrics.RunCountersRecord, error)
	Close() error
}

// MemStore provides an in-memory implementation of Store for standalone testing.
type MemStore struct {
	mu       sync.RWMutex
	rollups  map[string][]metrics.MetricRollup     // runID -> list
	counters map[string][]metrics.RunCountersRecord // runID -> list
}

// NewMemStore creates a new in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{
		rollups:  make(map[string][]metrics.MetricRollup),
		counters: make(map[string][]metrics.RunCountersRecord),
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

// Close is a no-op for MemStore.
func (s *MemStore) Close() error {
	return nil
}
