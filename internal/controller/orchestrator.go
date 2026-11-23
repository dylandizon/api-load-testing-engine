package controller

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"api-load-testing-engine/internal/metrics"
	"api-load-testing-engine/internal/store"
	pb "api-load-testing-engine/proto"
	"github.com/HdrHistogram/hdrhistogram-go"
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
	store       store.Store
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

// SetStore sets the backing store for threshold monitoring and result queries.
func (o *Orchestrator) SetStore(s store.Store) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.store = s
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

		abortCtx, cancelAbort := context.WithCancel(ctx)
		go o.monitorAbortThresholds(abortCtx, runState)

		if err := stageManager.RunStages(ctx, startAt); err != nil && err != context.Canceled {
			cancelAbort()
			runState.ForceTransition(StatusFailed, fmt.Sprintf("stage manager error: %v", err))
			return
		}
		cancelAbort()

		// Drain and await worker completion
		_ = stageManager.DrainAndComplete(ctx, 10*time.Second)
	}()

	return runState, nil
}

func (o *Orchestrator) monitorAbortThresholds(ctx context.Context, runState *RunState) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			status, _, _, _ := runState.GetStatus()
			if status != StatusRunning {
				if status == StatusCompleted || status == StatusAborted || status == StatusFailed {
					return
				}
				continue
			}
			if o.checkThresholds(runState) {
				return
			}
		}
	}
}

func (o *Orchestrator) checkThresholds(run *RunState) bool {
	o.mu.RLock()
	st := o.store
	o.mu.RUnlock()

	if st == nil || run == nil || run.Definition == nil {
		return false
	}

	// Check if any threshold has AbortOnFail
	hasAbort := false
	for _, th := range run.Definition.Thresholds {
		if th.AbortOnFail {
			hasAbort = true
			break
		}
	}
	if !hasAbort {
		return false
	}

	ctx := context.Background()
	rollups, _ := st.GetRollups(ctx, run.ID, "")
	countersList, _ := st.GetCounters(ctx, run.ID)

	var mergedCounters metrics.RunCountersRecord
	for _, c := range countersList {
		mergedCounters.Requests += c.Requests
		mergedCounters.Errors4xx += c.Errors4xx
		mergedCounters.Errors5xx += c.Errors5xx
		mergedCounters.Timeouts += c.Timeouts
		mergedCounters.AssertionFailures += c.AssertionFailures
	}

	histMap := make(map[string]*hdrhistogram.Histogram)
	for _, r := range rollups {
		if len(r.Histogram) > 0 {
			h, err := metrics.DecodeHistogram(r.Histogram)
			if err == nil {
				if existing, ok := histMap[r.Metric]; ok {
					existing.Merge(h)
				} else {
					histMap[r.Metric] = h
				}
			}
		}
	}

	for _, thDef := range run.Definition.Thresholds {
		if !thDef.AbortOnFail {
			continue
		}
		parsed, err := ParseThreshold(thDef)
		if err != nil {
			continue
		}
		res := EvaluateThreshold(parsed, histMap[parsed.Metric], &mergedCounters)
		if !res.Passed {
			_ = o.AbortRun(run.ID, fmt.Sprintf("Threshold breached (abort_on_fail): %s (%s)", thDef.Expr, res.Detail))
			return true
		}
	}
	return false
}

// GetRun returns the run state by ID.
func (o *Orchestrator) GetRun(runID string) (*RunState, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	r, exists := o.runs[runID]
	return r, exists
}

// AbortRun terminates a run early, broadcasting Abort messages to all assigned workers.
func (o *Orchestrator) AbortRun(runID string, reason string) error {
	o.mu.RLock()
	run, exists := o.runs[runID]
	o.mu.RUnlock()

	if !exists {
		return fmt.Errorf("run %s not found", runID)
	}

	run.ForceTransition(StatusAborted, reason)
	run.SetExitCode(1)

	abortMsg := &pb.ControllerMessage{
		Msg: &pb.ControllerMessage_Abort{
			Abort: &pb.Abort{
				RunId:    runID,
				Graceful: true,
				Reason:   reason,
			},
		},
	}

	run.mu.RLock()
	workers := make([]string, 0, len(run.Workers))
	for wID := range run.Workers {
		workers = append(workers, wID)
	}
	run.mu.RUnlock()

	for _, wID := range workers {
		_ = o.server.SendMessage(wID, abortMsg)
	}

	log.Printf("[orchestrator] run %s aborted: %s", runID, reason)
	return nil
}
