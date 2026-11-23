package controller

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"api-load-testing-engine/internal/scheduler"
)

// ScenarioDefinition defines a test scenario from test definition.
type ScenarioDefinition struct {
	Name      string            `json:"name"`
	Executor  string            `json:"executor"` // "open" or "closed"
	Stages    []scheduler.Stage `json:"stages"`
	MaxVUs    int               `json:"max_vus"`
	ThinkTime string            `json:"think_time"`
	Script    string            `json:"script"`
}

// TestDefinition represents the complete test configuration.
type TestDefinition struct {
	Name       string               `json:"name"`
	Version    int                  `json:"version"`
	BaseURL    string               `json:"base_url"`
	Protocol   string               `json:"protocol"`
	Scenarios  []ScenarioDefinition `json:"scenarios"`
	Thresholds []ThresholdDef       `json:"thresholds"`
}

// ThresholdDef defines a threshold rule.
type ThresholdDef struct {
	Metric      string `json:"metric"`
	Expr        string `json:"expr"`
	AbortOnFail bool   `json:"abort_on_fail"`
}

// WorkerAssignment contains the partitioned config slice for a single worker.
type WorkerAssignment struct {
	WorkerID    string
	ConfigSlice scheduler.WorkerConfigSlice
	ConfigBytes []byte
}

// Partitioner divides test workload proportionally across registered workers.
type Partitioner struct{}

// NewPartitioner creates a new workload partitioner.
func NewPartitioner() *Partitioner {
	return &Partitioner{}
}

// Partition divides a scenario's target VUs and RPS across available workers.
func (p *Partitioner) Partition(def *TestDefinition, workers []*Worker) (map[string]WorkerAssignment, error) {
	if len(workers) == 0 {
		return nil, fmt.Errorf("no workers available for partitioning")
	}
	if len(def.Scenarios) == 0 {
		return nil, fmt.Errorf("test definition has no scenarios")
	}

	scenario := def.Scenarios[0] // v1 supports single active scenario
	numWorkers := len(workers)

	// Sort workers descending by capacity (cores, then memory)
	sortedWorkers := make([]*Worker, numWorkers)
	copy(sortedWorkers, workers)
	sort.Slice(sortedWorkers, func(i, j int) bool {
		if sortedWorkers[i].Cores != sortedWorkers[j].Cores {
			return sortedWorkers[i].Cores > sortedWorkers[j].Cores
		}
		return sortedWorkers[i].MemMB > sortedWorkers[j].MemMB
	})

	var totalCores int32
	for _, w := range sortedWorkers {
		c := w.Cores
		if c <= 0 {
			c = 1
		}
		totalCores += c
	}

	// Prepare per-worker assignment structures
	assignments := make(map[string]*scheduler.WorkerConfigSlice, numWorkers)
	for _, w := range sortedWorkers {
		assignments[w.ID] = &scheduler.WorkerConfigSlice{
			BaseURL:   def.BaseURL,
			Executor:  scenario.Executor,
			ThinkTime: scenario.ThinkTime,
			MaxVUs:    int(math.Ceil(float64(scenario.MaxVUs) / float64(numWorkers))),
			Stages:    make([]scheduler.Stage, len(scenario.Stages)),
		}
	}

	// Partition each stage proportionally
	for stageIdx, stage := range scenario.Stages {
		if scenario.Executor == "closed" {
			// Partition integer VUs
			totalVUs := int(stage.Target)
			remainingVUs := totalVUs

			// Base allocation
			for _, w := range sortedWorkers {
				c := w.Cores
				if c <= 0 {
					c = 1
				}
				share := int(math.Floor(float64(totalVUs) * float64(c) / float64(totalCores)))
				assignments[w.ID].Stages[stageIdx] = scheduler.Stage{
					Duration: stage.Duration,
					Target:   float64(share),
				}
				remainingVUs -= share
			}

			// Distribute remainder +1 to highest-capacity workers
			for i := 0; i < remainingVUs && i < numWorkers; i++ {
				wID := sortedWorkers[i].ID
				assignments[wID].Stages[stageIdx].Target += 1
			}

		} else {
			// Open model: Partition float64 RPS
			totalRPS := stage.Target
			var allocatedRPS float64

			for i, w := range sortedWorkers {
				c := w.Cores
				if c <= 0 {
					c = 1
				}
				var share float64
				if i == numWorkers-1 {
					// Last worker takes exact remainder to ensure sum equals totalRPS
					share = totalRPS - allocatedRPS
					if share < 0 {
						share = 0
					}
				} else {
					share = math.Round((totalRPS*float64(c)/float64(totalCores))*100) / 100
					allocatedRPS += share
				}

				assignments[w.ID].Stages[stageIdx] = scheduler.Stage{
					Duration: stage.Duration,
					Target:   share,
				}
			}
		}
	}

	// Set initial stage target for each worker
	result := make(map[string]WorkerAssignment, numWorkers)
	for _, w := range sortedWorkers {
		cfg := assignments[w.ID]
		if len(cfg.Stages) > 0 {
			if scenario.Executor == "closed" {
				cfg.TargetVUs = int(cfg.Stages[0].Target)
			} else {
				cfg.TargetRPS = cfg.Stages[0].Target
			}
		}

		bytes, err := json.Marshal(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal config slice for worker %s: %w", w.ID, err)
		}

		result[w.ID] = WorkerAssignment{
			WorkerID:    w.ID,
			ConfigSlice: *cfg,
			ConfigBytes: bytes,
		}
	}

	return result, nil
}
