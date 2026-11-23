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

// WorkerConfigSlice defines the parsed configuration slice delivered to a worker in Assign.
type WorkerConfigSlice struct {
	BaseURL   string  `json:"base_url"`
	Executor  string  `json:"executor"` // "open" or "closed"
	TargetVUs int     `json:"target_vus"`
	TargetRPS float64 `json:"target_rps"`
	ThinkTime string  `json:"think_time"`
	MaxVUs    int     `json:"max_vus"`
	Stages    []Stage `json:"stages"`
}
