package controller

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// StartBarrier coordinates the two-phase start across multiple workers.
type StartBarrier struct {
	mu           sync.Mutex
	runID        string
	required     map[string]bool // workerID -> receivedReady
	readyCount   int
	allReadyCh   chan struct{}
	leadTime     time.Duration
}

// NewStartBarrier creates a start barrier for a list of assigned workers.
func NewStartBarrier(runID string, assignedWorkers []string, leadTime time.Duration) *StartBarrier {
	if leadTime <= 0 {
		leadTime = 2 * time.Second
	}

	req := make(map[string]bool, len(assignedWorkers))
	for _, id := range assignedWorkers {
		req[id] = false
	}

	return &StartBarrier{
		runID:      runID,
		required:   req,
		allReadyCh: make(chan struct{}),
		leadTime:   leadTime,
	}
}

// RecordReady marks a worker as ready. If all workers are ready, closes allReadyCh.
func (b *StartBarrier) RecordReady(workerID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	ready, exists := b.required[workerID]
	if !exists || ready {
		return false
	}

	b.required[workerID] = true
	b.readyCount++

	if b.readyCount == len(b.required) {
		close(b.allReadyCh)
		return true
	}
	return false
}

// AwaitStartTimestamp waits for all workers to become ready and returns the synchronized future start time.
func (b *StartBarrier) AwaitStartTimestamp(ctx context.Context, timeout time.Duration) (time.Time, error) {
	select {
	case <-b.allReadyCh:
		// All workers ready: schedule Start at T = now + leadTime
		return time.Now().Add(b.leadTime), nil
	case <-time.After(timeout):
		b.mu.Lock()
		defer b.mu.Unlock()
		var pending []string
		for id, ready := range b.required {
			if !ready {
				pending = append(pending, id)
			}
		}
		return time.Time{}, fmt.Errorf("start barrier timeout (%v) waiting for workers: %v", timeout, pending)
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	}
}
