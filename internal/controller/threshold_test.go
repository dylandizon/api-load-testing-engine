package controller

import (
	"context"
	"testing"
	"time"

	"api-load-testing-engine/internal/metrics"
	"api-load-testing-engine/internal/scheduler"
	"api-load-testing-engine/internal/store"
	pb "api-load-testing-engine/proto"
	"github.com/HdrHistogram/hdrhistogram-go"
)

func TestParseThreshold(t *testing.T) {
	tests := []struct {
		name      string
		def       ThresholdDef
		wantField string
		wantOp    string
		wantVal   float64
		wantErr   bool
	}{
		{
			name:      "p95 latency threshold",
			def:       ThresholdDef{Metric: "http_req_duration", Expr: "p95 < 200", AbortOnFail: false},
			wantField: "p95",
			wantOp:    "<",
			wantVal:   200,
			wantErr:   false,
		},
		{
			name:      "error rate threshold with abort",
			def:       ThresholdDef{Metric: "error_rate", Expr: "rate < 0.005", AbortOnFail: true},
			wantField: "rate",
			wantOp:    "<",
			wantVal:   0.005,
			wantErr:   false,
		},
		{
			name:      "p99 threshold with greater equal",
			def:       ThresholdDef{Metric: "http_req_duration", Expr: "p99 <= 500.5"},
			wantField: "p99",
			wantOp:    "<=",
			wantVal:   500.5,
			wantErr:   false,
		},
		{
			name:      "request count threshold",
			def:       ThresholdDef{Metric: "requests", Expr: "count >= 1000"},
			wantField: "count",
			wantOp:    ">=",
			wantVal:   1000,
			wantErr:   false,
		},
		{
			name:    "invalid operator",
			def:     ThresholdDef{Metric: "http_req_duration", Expr: "p95 ~ 200"},
			wantErr: true,
		},
		{
			name:    "invalid target number",
			def:     ThresholdDef{Metric: "http_req_duration", Expr: "p95 < abc"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseThreshold(tt.def)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseThreshold() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if parsed.Field != tt.wantField {
					t.Errorf("field = %s, want %s", parsed.Field, tt.wantField)
				}
				if parsed.Operator != tt.wantOp {
					t.Errorf("op = %s, want %s", parsed.Operator, tt.wantOp)
				}
				if parsed.TargetVal != tt.wantVal {
					t.Errorf("val = %f, want %f", parsed.TargetVal, tt.wantVal)
				}
				if parsed.AbortOnFail != tt.def.AbortOnFail {
					t.Errorf("abortOnFail = %v, want %v", parsed.AbortOnFail, tt.def.AbortOnFail)
				}
			}
		})
	}
}

func TestEvaluateThreshold_Latency(t *testing.T) {
	h := hdrhistogram.New(1, 3600000000, 3)
	// Record 100 samples of 100ms (100,000 µs) and 5 samples of 500ms (500,000 µs)
	for i := 0; i < 95; i++ {
		_ = h.RecordValue(100000)
	}
	for i := 0; i < 5; i++ {
		_ = h.RecordValue(500000)
	}

	// p90 should be 100ms
	p90Th, _ := ParseThreshold(ThresholdDef{Metric: "http_req_duration", Expr: "p90 < 200"})
	res90 := EvaluateThreshold(p90Th, h, nil)
	if !res90.Passed {
		t.Errorf("expected p90 < 200 to pass, got: %s", res90.Detail)
	}

	// p95 threshold: p95 is ~100-500ms; let's check p99 < 200 (fails since 5% are 500ms)
	p99Th, _ := ParseThreshold(ThresholdDef{Metric: "http_req_duration", Expr: "p99 < 200"})
	res99 := EvaluateThreshold(p99Th, h, nil)
	if res99.Passed {
		t.Errorf("expected p99 < 200 to fail, observed: %f", res99.Observed)
	}
}

func TestEvaluateThreshold_ErrorRate(t *testing.T) {
	counters := &metrics.RunCountersRecord{
		Requests:  1000,
		Errors4xx: 2,
		Errors5xx: 3,
		Timeouts:  1,
	}
	// Total errors: 6 / 1000 = 0.006 (0.6%)

	th1, _ := ParseThreshold(ThresholdDef{Metric: "error_rate", Expr: "rate < 0.01"}) // 1% allowed -> pass
	res1 := EvaluateThreshold(th1, nil, counters)
	if !res1.Passed {
		t.Errorf("expected rate < 0.01 to pass, got: %s", res1.Detail)
	}
	if res1.Observed < 0.0059 || res1.Observed > 0.0061 {
		t.Errorf("expected observed rate ~0.006, got: %f", res1.Observed)
	}

	th2, _ := ParseThreshold(ThresholdDef{Metric: "error_rate", Expr: "rate < 0.005", AbortOnFail: true}) // 0.5% allowed -> fail
	res2 := EvaluateThreshold(th2, nil, counters)
	if res2.Passed {
		t.Errorf("expected rate < 0.005 to fail, got: %s", res2.Detail)
	}
	if !res2.AbortOnFail {
		t.Errorf("expected AbortOnFail to be true")
	}
}

func TestOrchestrator_ThresholdAbortOnFail(t *testing.T) {
	memStore := store.NewMemStore()
	defer memStore.Close()

	reg := NewRegistry()
	server := NewCoordinatorServer(reg, ServerHooks{})
	merger := metrics.NewMerger(memStore, 100*time.Millisecond)
	server.SetMerger(merger)
	defer merger.Stop()

	orch := NewOrchestrator(reg, server, merger)
	orch.SetStore(memStore)

	ch := make(chan *pb.ControllerMessage, 10)
	server.RegisterWorkerChannel("w1", ch)
	reg.Register("w1", &pb.Register{Cores: 2, MemMb: 4096}, ch)

	testDef := &TestDefinition{
		Name: "abort-test",
		Scenarios: []ScenarioDefinition{
			{
				Name:     "sc1",
				Executor: "closed",
				Stages: []scheduler.Stage{
					{Duration: 2 * time.Second, Target: 10},
				},
			},
		},
		Thresholds: []ThresholdDef{
			{
				Metric:      "error_rate",
				Expr:        "rate < 0.05",
				AbortOnFail: true,
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runState, err := orch.StartRun(ctx, testDef)
	if err != nil {
		t.Fatalf("StartRun failed: %v", err)
	}

	// Worker signals Ready
	orch.handleWorkerReady("w1", &pb.Ready{RunId: runState.ID})

	// Wait for RUNNING status
	for i := 0; i < 20; i++ {
		status, _, _, _ := runState.GetStatus()
		if status == StatusRunning {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Inject failing counters into store (50 errors / 100 requests = 50% > 5%)
	_ = memStore.SaveCounters(metrics.RunCountersRecord{
		RunID:     runState.ID,
		Requests:  100,
		Errors5xx: 50,
	})

	// Monitor checks every 500ms -> should abort
	var finalStatus RunStatus
	for i := 0; i < 20; i++ {
		status, _, _, _ := runState.GetStatus()
		if status == StatusAborted {
			finalStatus = status
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if finalStatus != StatusAborted {
		t.Fatalf("expected run to be aborted on threshold breach, got %s", finalStatus)
	}

	_, _, _, exitCode := runState.GetStatus()
	if exitCode != 1 {
		t.Errorf("expected exit code 1, got %d", exitCode)
	}
}
