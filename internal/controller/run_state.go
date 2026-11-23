package controller

import (
	"fmt"
	"sync"
	"time"
)

// RunStatus defines the lifecycle states of a load test run.
type RunStatus string

const (
	StatusPending      RunStatus = "PENDING"
	StatusAssigning    RunStatus = "ASSIGNING"
	StatusWaitingReady RunStatus = "WAITING_READY"
	StatusRunning      RunStatus = "RUNNING"
	StatusDraining     RunStatus = "DRAINING"
	StatusCompleted    RunStatus = "COMPLETED"
	StatusAborted      RunStatus = "ABORTED"
	StatusFailed       RunStatus = "FAILED"
)

// RunState represents the in-memory and database state of an active load test run.
type RunState struct {
	mu            sync.RWMutex
	ID            string
	TestID        string
	Definition    *TestDefinition
	Status        RunStatus
	CurrentStage  int
	Degraded      bool
	DegradedNotes []string
	ExitCode      int
	FailureReason string
	CreatedAt     time.Time
	StartedAt     time.Time
	EndedAt       time.Time
	Workers       map[string]string // workerID -> state ("ASSIGNED", "READY", "RUNNING", "FINISHED", "FAILED")
}

// NewRunState initializes a run in PENDING status.
func NewRunState(runID, testID string, def *TestDefinition) *RunState {
	return &RunState{
		ID:         runID,
		TestID:     testID,
		Definition: def,
		Status:     StatusPending,
		CreatedAt:  time.Now(),
		Workers:    make(map[string]string),
	}
}

// Transition performs a guarded state transition.
func (r *RunState) Transition(expected, next RunStatus) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.Status != expected {
		return false
	}

	r.Status = next
	if next == StatusRunning && r.StartedAt.IsZero() {
		r.StartedAt = time.Now()
	} else if next == StatusCompleted || next == StatusAborted || next == StatusFailed {
		r.EndedAt = time.Now()
	}
	return true
}

// ForceTransition transitions to a terminal state (FAILED or ABORTED) regardless of current state.
func (r *RunState) ForceTransition(next RunStatus, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Status = next
	r.FailureReason = reason
	if r.EndedAt.IsZero() {
		r.EndedAt = time.Now()
	}
}

// SetDegraded flags the run as degraded due to worker saturation or failure.
func (r *RunState) SetDegraded(note string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Degraded = true
	if note != "" {
		r.DegradedNotes = append(r.DegradedNotes, fmt.Sprintf("[%s] %s", time.Now().Format(time.RFC3339), note))
	}
}

// SetWorkerState updates an individual worker's execution state for this run.
func (r *RunState) SetWorkerState(workerID, state string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Workers[workerID] = state
}

// AllWorkersInState checks if all assigned workers are in the target state.
func (r *RunState) AllWorkersInState(targetState string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.Workers) == 0 {
		return false
	}
	for _, st := range r.Workers {
		if st != targetState {
			return false
		}
	}
	return true
}

// AllActiveWorkersInState checks if all non-lost assigned workers are in the target state.
func (r *RunState) AllActiveWorkersInState(targetState string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	activeCount := 0
	for _, st := range r.Workers {
		if st == "LOST" || st == "DEAD" {
			continue
		}
		activeCount++
		if st != targetState {
			return false
		}
	}
	return activeCount > 0
}

// GetDegradedNotes returns a copy of degradation notes.
func (r *RunState) GetDegradedNotes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	notes := make([]string, len(r.DegradedNotes))
	copy(notes, r.DegradedNotes)
	return notes
}

// SetExitCode sets the deterministic CLI exit code for this run.
func (r *RunState) SetExitCode(code int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ExitCode = code
}

// GetStatus returns the current status and stage of the run.
func (r *RunState) GetStatus() (RunStatus, int, bool, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.Status, r.CurrentStage, r.Degraded, r.ExitCode
}
