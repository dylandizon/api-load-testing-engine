package controller

import (
	"context"
	"encoding/json"
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
	server.hooks.OnRegister = func(workerID string, reg *pb.Register) {
		if reg.ResumeRunId != "" {
			orch.RebindWorker(workerID, reg.ResumeRunId)
		}
	}
	server.hooks.OnHeartbeat = orch.handleWorkerHeartbeat
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

// RebindWorker handles a reconnecting worker that specifies resume_run_id.
func (o *Orchestrator) RebindWorker(workerID string, resumeRunID string) bool {
	o.mu.RLock()
	run, exists := o.runs[resumeRunID]
	o.mu.RUnlock()

	if !exists {
		return false
	}

	run.mu.Lock()
	run.Workers[workerID] = "RUNNING"
	run.mu.Unlock()

	log.Printf("[orchestrator] rebound worker %s to active run %s", workerID, resumeRunID)
	return true
}

// RecoverRuns rehydrates active runs from the store after a controller restart.
func (o *Orchestrator) RecoverRuns(ctx context.Context) ([]string, error) {
	o.mu.RLock()
	st := o.store
	o.mu.RUnlock()

	if st == nil {
		return nil, nil
	}

	activeRuns, err := st.GetActiveRuns(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load active runs from store: %w", err)
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	var recoveredIDs []string
	for _, rec := range activeRuns {
		var def TestDefinition
		if len(rec.Definition) > 0 {
			_ = json.Unmarshal(rec.Definition, &def)
		}
		runState := NewRunState(rec.ID, rec.TestID, &def)
		runState.Status = RunStatus(rec.Status)
		runState.CurrentStage = rec.CurrentStage
		runState.Degraded = rec.Degraded
		runState.StartedAt = rec.StartedAt

		o.runs[rec.ID] = runState
		recoveredIDs = append(recoveredIDs, rec.ID)
		log.Printf("[orchestrator] recovered active run %s (status: %s, stage: %d)", rec.ID, rec.Status, rec.CurrentStage)
	}

	return recoveredIDs, nil
}

func (o *Orchestrator) handleWorkerHeartbeat(workerID string, hb *pb.Heartbeat) {
	if hb == nil {
		return
	}

	o.registry.UpdateHeartbeat(workerID, hb)

	// Check for generator saturation
	if hb.CpuPct > 85.0 || hb.SchedulerLagMs > 50.0 {
		runID := hb.ActiveRunId
		if runID != "" {
			o.mu.RLock()
			run, exists := o.runs[runID]
			o.mu.RUnlock()
			if exists {
				run.SetDegraded(fmt.Sprintf("generator saturation on worker %s: CPU=%.1f%%, scheduler_lag=%.1fms",
					workerID, hb.CpuPct, hb.SchedulerLagMs))
			}
		}
	}
}

// StartLeaseMonitor starts a background loop monitoring worker heartbeats and marking lost workers.
func (o *Orchestrator) StartLeaseMonitor(ctx context.Context, suspectTimeout, deadTimeout, interval time.Duration) {
	if interval <= 0 {
		interval = 1 * time.Second
	}
	if suspectTimeout <= 0 {
		suspectTimeout = 5 * time.Second
	}
	if deadTimeout <= 0 {
		deadTimeout = 15 * time.Second
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, deadWorkers := o.registry.CheckLeases(suspectTimeout, deadTimeout)
				if len(deadWorkers) > 0 {
					o.handleDeadWorkers(deadWorkers)
				}
			}
		}
	}()
}

func (o *Orchestrator) handleDeadWorkers(deadWorkers []string) {
	o.mu.RLock()
	activeRuns := make([]*RunState, 0, len(o.runs))
	for _, r := range o.runs {
		activeRuns = append(activeRuns, r)
	}
	o.mu.RUnlock()

	for _, deadID := range deadWorkers {
		for _, run := range activeRuns {
			run.mu.Lock()
			wState, assigned := run.Workers[deadID]
			if assigned && wState != "FINISHED" && wState != "LOST" {
				run.Workers[deadID] = "LOST"
				run.Degraded = true
				run.DegradedNotes = append(run.DegradedNotes,
					fmt.Sprintf("[%s] worker %s lost: heartbeat lease expired", time.Now().Format(time.RFC3339), deadID))
				log.Printf("[orchestrator] worker %s lost mid-run; run %s flagged DEGRADED", deadID, run.ID)

				// Check if any healthy workers remain
				hasActive := false
				for _, st := range run.Workers {
					if st != "LOST" && st != "DEAD" {
						hasActive = true
						break
					}
				}
				if !hasActive && (run.Status == StatusRunning || run.Status == StatusDraining || run.Status == StatusWaitingReady) {
					run.Status = StatusFailed
					run.FailureReason = "all assigned workers lost"
					run.ExitCode = 2
					log.Printf("[orchestrator] run %s failed: all assigned workers lost", run.ID)
				}
			}
			run.mu.Unlock()
		}
	}
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
