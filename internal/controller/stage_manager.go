package controller

import (
	"context"
	"fmt"
	"log"
	"time"

	"api-load-testing-engine/internal/scheduler"
	pb "api-load-testing-engine/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// StageManager orchestrates multi-stage load schedules with absolute timestamps.
type StageManager struct {
	runID           string
	runState        *RunState
	stages          []scheduler.Stage
	server          *CoordinatorServer
	assignedWorkers []string
}

// NewStageManager creates a stage manager for an active run.
func NewStageManager(runID string, runState *RunState, stages []scheduler.Stage, server *CoordinatorServer, workers []string) *StageManager {
	return &StageManager{
		runID:           runID,
		runState:        runState,
		stages:          stages,
		server:          server,
		assignedWorkers: workers,
	}
}

// RunStages executes stage transitions across time, broadcasting StageChange messages with future absolute timestamps.
func (sm *StageManager) RunStages(ctx context.Context, startAt time.Time) error {
	// 1. Wait until synchronized start timestamp
	now := time.Now()
	if startAt.After(now) {
		select {
		case <-time.After(startAt.Sub(now)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	sm.runState.Transition(StatusWaitingReady, StatusRunning)
	log.Printf("[stage-manager] run %s started at %s with %d stages", sm.runID, startAt.Format(time.RFC3339), len(sm.stages))

	// 2. Loop through stages
	for idx, stage := range sm.stages {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		sm.runState.CurrentStage = idx

		// For stages after stage 0, broadcast StageChange with future timestamp
		if idx > 0 {
			effectiveAt := time.Now().Add(500 * time.Millisecond)
			log.Printf("[stage-manager] run %s: transitioning to stage %d (target: %.2f) at %s",
				sm.runID, idx, stage.Target, effectiveAt.Format(time.RFC3339))

			stageMsg := &pb.ControllerMessage{
				Msg: &pb.ControllerMessage_StageChange{
					StageChange: &pb.StageChange{
						RunId:       sm.runID,
						StageIndex:  int32(idx),
						EffectiveAt: timestamppb.New(effectiveAt),
					},
				},
			}

			for _, wID := range sm.assignedWorkers {
				_ = sm.server.SendMessage(wID, stageMsg)
			}

			// Wait until effectiveAt
			delay := time.Until(effectiveAt)
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		// Run for stage duration
		log.Printf("[stage-manager] run %s: running stage %d for %v", sm.runID, idx, stage.Duration)
		select {
		case <-time.After(stage.Duration):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// 3. All stages finished: transition to DRAINING and broadcast graceful completion to workers
	log.Printf("[stage-manager] run %s completed all stages. Transitioning to DRAINING...", sm.runID)
	sm.runState.Transition(StatusRunning, StatusDraining)

	drainMsg := &pb.ControllerMessage{
		Msg: &pb.ControllerMessage_Abort{
			Abort: &pb.Abort{
				RunId:    sm.runID,
				Graceful: true,
				Reason:   "stages completed",
			},
		},
	}
	for _, wID := range sm.assignedWorkers {
		_ = sm.server.SendMessage(wID, drainMsg)
	}

	return nil
}

// DrainAndComplete waits for all workers to report FINISHED, or timeouts after drainTimeout.
func (sm *StageManager) DrainAndComplete(ctx context.Context, drainTimeout time.Duration) error {
	if drainTimeout <= 0 {
		drainTimeout = 10 * time.Second
	}

	deadline := time.Now().Add(drainTimeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			if sm.runState.AllWorkersInState("FINISHED") {
				sm.runState.Transition(StatusDraining, StatusCompleted)
				log.Printf("[stage-manager] run %s successfully completed on all workers", sm.runID)
				return nil
			}
			if now.After(deadline) {
				sm.runState.Transition(StatusDraining, StatusFailed)
				return fmt.Errorf("drain timeout reached for run %s", sm.runID)
			}
		}
	}
}
