package controller

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"api-load-testing-engine/internal/scheduler"
	"gopkg.in/yaml.v3"
)

// ScenarioDefinition defines a test scenario from test definition.
type ScenarioDefinition struct {
	Name      string            `json:"name" yaml:"name"`
	Executor  string            `json:"executor" yaml:"executor"` // "open" or "closed"
	Stages    []scheduler.Stage `json:"stages" yaml:"stages"`
	MaxVUs    int               `json:"max_vus" yaml:"max_vus"`
	ThinkTime string            `json:"think_time" yaml:"think_time"`
	Script    string            `json:"script" yaml:"script"`
}

// TestDefinition represents the complete test configuration.
type TestDefinition struct {
	Name       string               `json:"name" yaml:"name"`
	Version    int                  `json:"version" yaml:"version"`
	BaseURL    string               `json:"base_url" yaml:"base_url"`
	Protocol   string               `json:"protocol" yaml:"protocol"`
	Scenarios  []ScenarioDefinition `json:"scenarios" yaml:"scenarios"`
	Thresholds []ThresholdDef       `json:"thresholds" yaml:"thresholds"`
}

// UnmarshalYAML supports flexible test definitions (slice or map scenarios, or root stages).
func (t *TestDefinition) UnmarshalYAML(value *yaml.Node) error {
	type rawTestDef struct {
		Name       string            `yaml:"name"`
		Version    int               `yaml:"version"`
		BaseURL    string            `yaml:"base_url"`
		Protocol   string            `yaml:"protocol"`
		Executor   string            `yaml:"executor"`
		Stages     []scheduler.Stage `yaml:"stages"`
		MaxVUs     int               `yaml:"max_vus"`
		ThinkTime  string            `yaml:"think_time"`
		Script     string            `yaml:"script"`
		Scenarios  yaml.Node         `yaml:"scenarios"`
		Thresholds []ThresholdDef    `yaml:"thresholds"`
	}

	var raw rawTestDef
	if err := value.Decode(&raw); err != nil {
		return err
	}

	t.Name = raw.Name
	t.Version = raw.Version
	t.BaseURL = raw.BaseURL
	t.Protocol = raw.Protocol
	t.Thresholds = raw.Thresholds

	if raw.Scenarios.Kind == yaml.SequenceNode {
		if err := raw.Scenarios.Decode(&t.Scenarios); err != nil {
			return err
		}
	} else if raw.Scenarios.Kind == yaml.MappingNode {
		var scenMap map[string]ScenarioDefinition
		if err := raw.Scenarios.Decode(&scenMap); err != nil {
			return err
		}
		for name, sc := range scenMap {
			if sc.Name == "" {
				sc.Name = name
			}
			if sc.Executor == "" && raw.Executor != "" {
				sc.Executor = raw.Executor
			}
			if len(sc.Stages) == 0 && len(raw.Stages) > 0 {
				sc.Stages = raw.Stages
			}
			t.Scenarios = append(t.Scenarios, sc)
		}
	} else if len(raw.Stages) > 0 {
		executor := raw.Executor
		if executor == "" {
			executor = "closed"
		}
		t.Scenarios = append(t.Scenarios, ScenarioDefinition{
			Name:      "default",
			Executor:  executor,
			Stages:    raw.Stages,
			MaxVUs:    raw.MaxVUs,
			ThinkTime: raw.ThinkTime,
			Script:    raw.Script,
		})
	}
	return nil
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
