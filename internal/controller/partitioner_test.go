package controller

import (
	"testing"
	"time"

	"api-load-testing-engine/internal/scheduler"
)

func TestPartitioner_ClosedModelProportionalAllocation(t *testing.T) {
	p := NewPartitioner()

	workers := []*Worker{
		{ID: "worker-large", Cores: 8, MemMB: 16384},
		{ID: "worker-medium", Cores: 4, MemMB: 8192},
		{ID: "worker-small", Cores: 2, MemMB: 4096},
	}

	testDef := &TestDefinition{
		Name:    "checkout-test",
		BaseURL: "https://api.example.com",
		Scenarios: []ScenarioDefinition{
			{
				Name:      "flow",
				Executor:  "closed",
				ThinkTime: "500ms",
				Stages: []scheduler.Stage{
					{Duration: 1 * time.Minute, Target: 100}, // 100 VUs total
					{Duration: 2 * time.Minute, Target: 35},  // 35 VUs total (odd number to test remainder rounding)
				},
			},
		},
	}

	assignments, err := p.Partition(testDef, workers)
	if err != nil {
		t.Fatalf("partition failed: %v", err)
	}

	if len(assignments) != 3 {
		t.Fatalf("expected 3 assignments, got %d", len(assignments))
	}

	// Verify Stage 0 (100 VUs)
	totalVUsStage0 := 0
	for _, a := range assignments {
		totalVUsStage0 += int(a.ConfigSlice.Stages[0].Target)
	}
	if totalVUsStage0 != 100 {
		t.Fatalf("expected exactly 100 total VUs for stage 0, got %d", totalVUsStage0)
	}

	// Verify Stage 1 (35 VUs)
	totalVUsStage1 := 0
	for _, a := range assignments {
		totalVUsStage1 += int(a.ConfigSlice.Stages[1].Target)
	}
	if totalVUsStage1 != 35 {
		t.Fatalf("expected exactly 35 total VUs for stage 1, got %d", totalVUsStage1)
	}

	// Verify proportions: Large > Medium > Small
	wLarge := assignments["worker-large"].ConfigSlice.Stages[0].Target
	wMed := assignments["worker-medium"].ConfigSlice.Stages[0].Target
	wSmall := assignments["worker-small"].ConfigSlice.Stages[0].Target

	if !(wLarge > wMed && wMed > wSmall) {
		t.Fatalf("proportionality violated: large=%f, med=%f, small=%f", wLarge, wMed, wSmall)
	}
}

func TestPartitioner_OpenModelProportionalAllocation(t *testing.T) {
	p := NewPartitioner()

	workers := []*Worker{
		{ID: "worker-1", Cores: 4},
		{ID: "worker-2", Cores: 4},
		{ID: "worker-3", Cores: 2},
	}

	testDef := &TestDefinition{
		Name:    "open-test",
		BaseURL: "https://api.example.com",
		Scenarios: []ScenarioDefinition{
			{
				Name:     "open-flow",
				Executor: "open",
				MaxVUs:   1000,
				Stages: []scheduler.Stage{
					{Duration: 1 * time.Minute, Target: 250.0}, // 250 RPS total
				},
			},
		},
	}

	assignments, err := p.Partition(testDef, workers)
	if err != nil {
		t.Fatalf("partition failed: %v", err)
	}

	var sumRPS float64
	for _, a := range assignments {
		sumRPS += a.ConfigSlice.Stages[0].Target
	}

	if sumRPS != 250.0 {
		t.Fatalf("expected exactly 250 RPS sum, got %f", sumRPS)
	}

	// worker-1 and worker-2 should have equal RPS, worker-3 less
	r1 := assignments["worker-1"].ConfigSlice.Stages[0].Target
	r2 := assignments["worker-2"].ConfigSlice.Stages[0].Target
	r3 := assignments["worker-3"].ConfigSlice.Stages[0].Target

	if r1 != r2 {
		t.Fatalf("expected equal allocation for equal cores: r1=%f, r2=%f", r1, r2)
	}
	if r3 >= r1 {
		t.Fatalf("expected r3 to be smaller than r1: r3=%f, r1=%f", r3, r1)
	}
}
