package scheduler

import (
	"context"
	"encoding/json"
	"time"

	"gopkg.in/yaml.v3"
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
	Duration time.Duration `json:"duration" yaml:"duration"`
	Target   float64       `json:"target" yaml:"target"`
}

func (s *Stage) UnmarshalJSON(data []byte) error {
	var raw struct {
		Duration interface{} `json:"duration"`
		Target   float64     `json:"target"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	s.Target = raw.Target
	s.Duration = parseDurationHelper(raw.Duration)
	return nil
}

func (s *Stage) UnmarshalYAML(value *yaml.Node) error {
	var raw struct {
		Duration interface{} `yaml:"duration"`
		Target   float64     `yaml:"target"`
	}
	if err := value.Decode(&raw); err != nil {
		return err
	}
	s.Target = raw.Target
	s.Duration = parseDurationHelper(raw.Duration)
	return nil
}

func parseDurationHelper(v interface{}) time.Duration {
	switch val := v.(type) {
	case string:
		d, err := time.ParseDuration(val)
		if err == nil {
			return d
		}
		return 0
	case float64:
		return time.Duration(val * float64(time.Second))
	case int:
		return time.Duration(val) * time.Second
	default:
		return 0
	}
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
