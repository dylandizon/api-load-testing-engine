package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"api-load-testing-engine/internal/metrics"
	"api-load-testing-engine/internal/scheduler"
	"api-load-testing-engine/internal/store"
	pb "api-load-testing-engine/proto"
)

func TestFaultTolerance_WorkerLeaseExpirationMarksRunDegraded(t *testing.T) {
	memStore := store.NewMemStore()
	defer memStore.Close()

	reg := NewRegistry()
	server := NewCoordinatorServer(reg, ServerHooks{})
	merger := metrics.NewMerger(memStore, 50*time.Millisecond)
	server.SetMerger(merger)
	defer merger.Stop()

	orch := NewOrchestrator(reg, server, merger)
	orch.SetStore(memStore)

	ch1 := make(chan *pb.ControllerMessage, 10)
	ch2 := make(chan *pb.ControllerMessage, 10)
	server.RegisterWorkerChannel("worker-healthy", ch1)
	server.RegisterWorkerChannel("worker-dying", ch2)
	reg.Register("worker-healthy", &pb.Register{Cores: 2, MemMb: 4096}, ch1)
	reg.Register("worker-dying", &pb.Register{Cores: 2, MemMb: 4096}, ch2)

	testDef := &TestDefinition{
		Name: "fault-test",
		Scenarios: []ScenarioDefinition{
			{
				Name:     "sc1",
				Executor: "closed",
				Stages: []scheduler.Stage{
					{Duration: 2 * time.Second, Target: 10},
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	runState, err := orch.StartRun(ctx, testDef)
	if err != nil {
		t.Fatalf("StartRun failed: %v", err)
	}

	// Workers signal Ready
	orch.handleWorkerReady("worker-healthy", &pb.Ready{RunId: runState.ID})
	orch.handleWorkerReady("worker-dying", &pb.Ready{RunId: runState.ID})

	// Wait for RUNNING
	for i := 0; i < 20; i++ {
		status, _, _, _ := runState.GetStatus()
		if status == StatusRunning {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Simulate worker-dying heartbeat dying (last heartbeat 16 seconds ago)
	wDying, _ := reg.GetWorker("worker-dying")
	wDying.LastHeartbeat = time.Now().Add(-16 * time.Second)
	reg.mu.Lock()
	reg.workers["worker-dying"].LastHeartbeat = time.Now().Add(-16 * time.Second)
	reg.mu.Unlock()

	// Trigger lease check: suspect at 5s, dead at 15s
	_, dead := reg.CheckLeases(5*time.Second, 15*time.Second)
	if len(dead) == 0 || dead[0] != "worker-dying" {
		t.Fatalf("expected worker-dying to be dead, got %v", dead)
	}

	// Handle dead workers
	orch.handleDeadWorkers(dead)

	// Verify run is marked degraded
	status, _, degraded, _ := runState.GetStatus()
	if !degraded {
		t.Fatalf("expected run to be marked DEGRADED, got degraded=%v", degraded)
	}
	notes := runState.GetDegradedNotes()
	if len(notes) == 0 {
		t.Fatalf("expected degraded notes to explain worker loss, got empty")
	}
	t.Logf("Degraded note: %s", notes[0])

	// Worker-healthy completes
	orch.handleWorkerStateChange("worker-healthy", &pb.StateChange{
		RunId: runState.ID,
		State: pb.StateChange_FINISHED,
	})

	// Remaining active workers finished -> AllActiveWorkersInState("FINISHED") should be true
	if !runState.AllActiveWorkersInState("FINISHED") {
		t.Errorf("expected AllActiveWorkersInState('FINISHED') to be true despite lost worker")
	}
	if status == StatusFailed {
		t.Errorf("run should not have failed since healthy worker was still present")
	}
}

func TestFaultTolerance_GeneratorSaturation(t *testing.T) {
	memStore := store.NewMemStore()
	defer memStore.Close()

	reg := NewRegistry()
	server := NewCoordinatorServer(reg, ServerHooks{})
	merger := metrics.NewMerger(memStore, 50*time.Millisecond)
	server.SetMerger(merger)
	defer merger.Stop()

	orch := NewOrchestrator(reg, server, merger)
	orch.SetStore(memStore)

	ch := make(chan *pb.ControllerMessage, 10)
	server.RegisterWorkerChannel("worker-sat", ch)
	reg.Register("worker-sat", &pb.Register{Cores: 2, MemMb: 4096}, ch)

	runState := NewRunState("run-sat", "test-1", &TestDefinition{Name: "sat-test"})
	runState.Status = StatusRunning
	orch.runs["run-sat"] = runState

	// Normal heartbeat
	orch.handleWorkerHeartbeat("worker-sat", &pb.Heartbeat{
		ActiveRunId:    "run-sat",
		CpuPct:         45.0,
		SchedulerLagMs: 2.0,
	})
	if runState.Degraded {
		t.Errorf("run should not be degraded under normal CPU")
	}

	// Saturated heartbeat: CPU > 85%
	orch.handleWorkerHeartbeat("worker-sat", &pb.Heartbeat{
		ActiveRunId:    "run-sat",
		CpuPct:         92.5,
		SchedulerLagMs: 80.0,
	})
	if !runState.Degraded {
		t.Errorf("run should be degraded when worker CPU/lag saturates")
	}
	notes := runState.GetDegradedNotes()
	if len(notes) == 0 {
		t.Errorf("expected degradation notes for generator saturation")
	} else {
		t.Logf("Saturation note: %s", notes[0])
	}
}

func TestFaultTolerance_CrashRecoveryAndWorkerRebind(t *testing.T) {
	memStore := store.NewMemStore()
	defer memStore.Close()

	// 1. Controller 1 writes an active run to Postgres/Store before crashing
	ctx := context.Background()
	testDefBytes, _ := json.Marshal(&TestDefinition{
		Name: "crash-recovery-test",
		Scenarios: []ScenarioDefinition{
			{Name: "default", Executor: "closed"},
		},
	})
	_ = memStore.SaveRun(ctx, store.RunRecord{
		ID:           "run-rehydrate-123",
		TestID:       "test-rehydrate-456",
		Definition:   testDefBytes,
		Status:       "RUNNING",
		CurrentStage: 1,
		CreatedAt:    time.Now().Add(-10 * time.Second),
		StartedAt:    time.Now().Add(-5 * time.Second),
	})

	// 2. Controller 2 starts fresh with the same store
	reg2 := NewRegistry()
	server2 := NewCoordinatorServer(reg2, ServerHooks{})
	merger2 := metrics.NewMerger(memStore, 50*time.Millisecond)
	server2.SetMerger(merger2)
	defer merger2.Stop()

	orch2 := NewOrchestrator(reg2, server2, merger2)
	orch2.SetStore(memStore)

	// Rehydrate active runs
	recovered, err := orch2.RecoverRuns(ctx)
	if err != nil {
		t.Fatalf("RecoverRuns failed: %v", err)
	}
	if len(recovered) != 1 || recovered[0] != "run-rehydrate-123" {
		t.Fatalf("expected recovered run-rehydrate-123, got %v", recovered)
	}

	rehydratedRun, exists := orch2.GetRun("run-rehydrate-123")
	if !exists {
		t.Fatalf("recovered run not found in memory map")
	}
	if rehydratedRun.Status != StatusRunning || rehydratedRun.CurrentStage != 1 {
		t.Fatalf("unexpected recovered state: status=%s, stage=%d", rehydratedRun.Status, rehydratedRun.CurrentStage)
	}

	// 3. Worker reconnects sending ResumeRunId
	chWorker := make(chan *pb.ControllerMessage, 10)
	reg2.Register("worker-reconnected", &pb.Register{
		Cores:       4,
		MemMb:       8192,
		ResumeRunId: "run-rehydrate-123",
	}, chWorker)

	rebound := orch2.RebindWorker("worker-reconnected", "run-rehydrate-123")
	if !rebound {
		t.Fatalf("expected worker to be rebound to active run")
	}

	rehydratedRun.mu.RLock()
	workerState := rehydratedRun.Workers["worker-reconnected"]
	rehydratedRun.mu.RUnlock()

	if workerState != "RUNNING" {
		t.Errorf("expected worker state RUNNING, got %s", workerState)
	}
}
