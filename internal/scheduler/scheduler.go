package scheduler

import (
	"context"
	"time"
)

// IterationRunner defines the function signature to execute one test iteration.
type IterationRunner func(ctx context.Context, vuID int, intendedArrival time.Time) error

// Executor represents a load generation scheduler (closed or open).
type Executor interface {
	Start(ctx context.Context) error
	Stop()
	UpdateTarget(target float64)
}

// Stage defines a load phase with a duration and target (VUs or RPS).
type Stage struct {
	Duration time.Duration `json:"duration"`
	Target   float64       `json:"target"`
}
