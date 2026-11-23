package controller

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"api-load-testing-engine/internal/metrics"
	pb "api-load-testing-engine/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Orchestrator coordinates multi-worker runs, barriers, and stage executions.
type Orchestrator struct {
	mu          sync.RWMutex
	registry    *Registry
	server      *CoordinatorServer
	partitioner *Partitioner
	merger      *metrics.Merger
	runs        map[string]*RunState
	barriers    map[string]*StartBarrier
}

// NewOrchestrator creates a new multi-worker run orchestrator.
func NewOrchestrator(registry *Registry, server *CoordinatorServer, merger *metrics.Merger) *Orchestrator {
	orch := &Orchestrator{
		registry:    registry,
		server:      server,
		partitioner: NewPartitioner(),
		merger:      merger,
		runs:        make(map[string]*RunState),
		barriers:    make(map[string]*StartBarrier),
	}

	// Register hooks on server
	server.hooks.OnReady = orch.handleWorkerReady
	server.hooks.OnStateChange = orch.handleWorkerStateChange

	return orch
}

func (o *Orchestrator) handleWorkerReady(workerID string, ready *pb.Ready) {
	o.mu.RLock()
	barrier, exists := o.barriers[ready.RunId]
	run, runExists := o.runs[ready.RunId]
	o.mu.RUnlock()

	if runExists {
		run.SetWorkerState(workerID, "READY")
	}
	if exists {
		barrier.RecordReady(workerID)
	}
}

func (o *Orchestrator) handleWorkerStateChange(workerID string, state *pb.StateChange) {
	o.mu.RLock()
	run, exists := o.runs[state.RunId]
	o.mu.RUnlock()

	if exists {
		switch state.State {
		case pb.StateChange_RUNNING:
			run.SetWorkerState(workerID, "RUNNING")
		case pb.StateChange_FINISHED:
			run.SetWorkerState(workerID, "FINISHED")
		case pb.StateChange_FAILED:
			run.SetWorkerState(workerID, "FAILED")
			run.SetDegraded(fmt.Sprintf("worker %s reported failure: %s", workerID, state.Detail))
		}
	}
}

// StartRun initializes, partitions, synchronizes, and executes a test across available workers.
func (o *Orchestrator) StartRun(ctx context.Context, testDef *TestDefinition) (*RunState, error) {
	availableWorkers := o.registry.ListWorkers()
	if len(availableWorkers) == 0 {
		return nil, fmt.Errorf("no workers registered with controller")
	}

	runID := fmt.Sprintf("run-%s", uuid.New().String()[:8])
	testID := fmt.Sprintf("test-%s", uuid.New().String()[:8])

	// 1. Partition workload
	assignments, err := o.partitioner.Partition(testDef, availableWorkers)
	if err != nil {
		return nil, fmt.Errorf("partition error: %w", err)
	}

	assignedWorkerIDs := make([]string, 0, len(assignments))
	for wID := range assignments {
		assignedWorkerIDs = append(assignedWorkerIDs, wID)
	}

	runState := NewRunState(runID, testID, testDef)
	for _, wID := range assignedWorkerIDs {
		runState.SetWorkerState(wID, "ASSIGNED")
	}

	// 2. Setup Barrier
	barrier := NewStartBarrier(runID, assignedWorkerIDs, 1*time.Second)

	o.mu.Lock()
	o.runs[runID] = runState
	o.barriers[runID] = barrier
	o.mu.Unlock()

	// 3. Dispatch Assign to all workers
	runState.Transition(StatusPending, StatusAssigning)
	scriptBytes := []byte(testDef.Scenarios[0].Script)

	for _, a := range assignments {
		assignMsg := &pb.ControllerMessage{
			Msg: &pb.ControllerMessage_Assign{
				Assign: &pb.Assign{
					RunId:       runID,
					Script:      scriptBytes,
					ConfigSlice: a.ConfigBytes,
				},
			},
		}
		if err := o.server.SendMessage(a.WorkerID, assignMsg); err != nil {
			runState.ForceTransition(StatusFailed, fmt.Sprintf("failed to assign to worker %s: %v", a.WorkerID, err))
			return runState, fmt.Errorf("assign error: %w", err)
		}
	}

	runState.Transition(StatusAssigning, StatusWaitingReady)
	log.Printf("[orchestrator] sent Assign to %d workers for run %s. Awaiting Ready barrier...", len(assignments), runID)

	// 4. Await Start Barrier & Execute asynchronously
	go func() {
		startAt, err := barrier.AwaitStartTimestamp(ctx, 30*time.Second)
		if err != nil {
			runState.ForceTransition(StatusFailed, fmt.Sprintf("barrier failed: %v", err))
			log.Printf("[orchestrator] run %s barrier error: %v", runID, err)
			return
		}

		// Broadcast Start(T)
		startMsg := &pb.ControllerMessage{
			Msg: &pb.ControllerMessage_Start{
				Start: &pb.Start{
					RunId:   runID,
					StartAt: timestamppb.New(startAt),
				},
			},
		}
		for _, wID := range assignedWorkerIDs {
			_ = o.server.SendMessage(wID, startMsg)
		}

		log.Printf("[orchestrator] broadcast Start(T=%s) for run %s to %d workers",
			startAt.Format(time.RFC3339), runID, len(assignedWorkerIDs))

		// Launch stage manager
		stageManager := NewStageManager(runID, runState, testDef.Scenarios[0].Stages, o.server, assignedWorkerIDs)
		if err := stageManager.RunStages(ctx, startAt); err != nil && err != context.Canceled {
			runState.ForceTransition(StatusFailed, fmt.Sprintf("stage manager error: %v", err))
			return
		}

		// Drain and await worker completion
		_ = stageManager.DrainAndComplete(ctx, 10*time.Second)
	}()

	return runState, nil
}

// GetRun returns the run state by ID.
func (o *Orchestrator) GetRun(runID string) (*RunState, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	r, exists := o.runs[runID]
	return r, exists
}
