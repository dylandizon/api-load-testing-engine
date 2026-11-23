package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"api-load-testing-engine/internal/metrics"
	"api-load-testing-engine/internal/protocol"
	"api-load-testing-engine/internal/scheduler"
	"api-load-testing-engine/internal/worker/script"
	pb "api-load-testing-engine/proto"
)

// LoadRunner executes real load tests driven by the controller.
type LoadRunner struct {
	client     *Client
	httpClient *protocol.ProtocolClient

	mu         sync.Mutex
	activeRun  string
	runtime    *script.Runtime
	aggregator *metrics.LocalAggregator
	executor   scheduler.Executor
	cancel     context.CancelFunc
	config     scheduler.WorkerConfigSlice
	flushStop  chan struct{}
}

// NewLoadRunner creates a real load generation runner.
func NewLoadRunner(httpClient *protocol.ProtocolClient) *LoadRunner {
	if httpClient == nil {
		httpClient = protocol.NewProtocolClient()
	}
	return &LoadRunner{
		httpClient: httpClient,
	}
}

// SetClient binds the gRPC client.
func (r *LoadRunner) SetClient(client *Client) {
	r.client = client
}

// OnAssign receives script and config slice, compiles the script, and signals Ready.
func (r *LoadRunner) OnAssign(assign *pb.Assign) error {
	r.mu.Lock()
	r.activeRun = assign.RunId

	var cfg scheduler.WorkerConfigSlice
	if len(assign.ConfigSlice) > 0 {
		if err := json.Unmarshal(assign.ConfigSlice, &cfg); err != nil {
			log.Printf("[load-runner] config unmarshal error: %v", err)
		}
	}
	r.config = cfg

	scriptSrc := string(assign.Script)
	if scriptSrc == "" {
		// Default ping script if no custom script provided
		scriptSrc = `http.get('/');`
	}

	agg := metrics.NewLocalAggregator(assign.RunId)
	r.aggregator = agg

	rt, err := script.NewRuntime(scriptSrc, cfg.BaseURL, agg, r.httpClient)
	if err != nil {
		r.mu.Unlock()
		return fmt.Errorf("failed to initialize script runtime: %w", err)
	}
	r.runtime = rt

	// Create iteration runner function
	iterationRunner := func(ctx context.Context, vuID int, intendedArrival time.Time) error {
		vm := rt.NewVMInstance()
		return vm.ExecuteIteration(ctx, nil, intendedArrival)
	}

	// Initialize executor
	if cfg.Executor == "open" {
		r.executor = scheduler.NewOpenExecutor(cfg.TargetRPS, cfg.MaxVUs, iterationRunner, agg)
	} else {
		var thinkDur time.Duration
		if cfg.ThinkTime != "" {
			thinkDur, _ = time.ParseDuration(cfg.ThinkTime)
		}
		r.executor = scheduler.NewClosedExecutor(cfg.TargetVUs, thinkDur, iterationRunner, agg)
	}
	r.mu.Unlock()

	r.client.SetActiveRun(assign.RunId)
	log.Printf("[load-runner] pre-warmed for run %s (executor: %s). Sending Ready...", assign.RunId, cfg.Executor)

	return r.client.Send(&pb.WorkerMessage{
		Msg: &pb.WorkerMessage_Ready{
			Ready: &pb.Ready{
				RunId: assign.RunId,
			},
		},
	})
}

// OnStart begins executing the load schedule and starts the 1-second metrics flusher.
func (r *LoadRunner) OnStart(start *pb.Start) error {
	r.mu.Lock()
	exec := r.executor
	runID := r.activeRun
	r.flushStop = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.mu.Unlock()

	if exec == nil {
		return fmt.Errorf("no executor configured for run %s", runID)
	}

	log.Printf("[load-runner] starting load execution for run %s...", runID)
	if err := exec.Start(ctx); err != nil {
		return err
	}

	// 1-second metrics flush loop
	go r.runMetricsFlusher(runID, r.flushStop)

	return nil
}

func (r *LoadRunner) runMetricsFlusher(runID string, stopCh chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			// Final flush
			r.flushMetrics(runID)
			return
		case <-ticker.C:
			r.flushMetrics(runID)
		}
	}
}

func (r *LoadRunner) flushMetrics(runID string) {
	r.mu.Lock()
	agg := r.aggregator
	r.mu.Unlock()

	if agg == nil {
		return
	}

	batch := agg.SnapshotAndReset()
	if batch != nil {
		_ = r.client.Send(&pb.WorkerMessage{
			Msg: &pb.WorkerMessage_Metrics{
				Metrics: batch,
			},
		})
	}
}

// OnStageChange dynamically updates the target load.
func (r *LoadRunner) OnStageChange(change *pb.StageChange) error {
	r.mu.Lock()
	exec := r.executor
	cfg := r.config
	r.mu.Unlock()

	if exec != nil && int(change.StageIndex) < len(cfg.Stages) {
		stage := cfg.Stages[change.StageIndex]
		log.Printf("[load-runner] updating stage to %d (target: %.2f)", change.StageIndex, stage.Target)
		exec.UpdateTarget(stage.Target)
	}
	return nil
}

// OnAbort gracefully terminates execution and sends StateChange.
func (r *LoadRunner) OnAbort(abort *pb.Abort) error {
	r.mu.Lock()
	cancel := r.cancel
	exec := r.executor
	flushStop := r.flushStop
	runID := r.activeRun
	r.mu.Unlock()

	log.Printf("[load-runner] aborting run %s: graceful=%v, reason=%s", runID, abort.Graceful, abort.Reason)

	if cancel != nil {
		cancel()
	}
	if exec != nil {
		exec.Stop()
	}
	if flushStop != nil {
		close(flushStop)
	}

	return r.client.Send(&pb.WorkerMessage{
		Msg: &pb.WorkerMessage_State{
			State: &pb.StateChange{
				RunId:  runID,
				State:  pb.StateChange_FINISHED,
				Detail: "aborted: " + abort.Reason,
			},
		},
	})
}

// OnAck receives server acknowledgement for a metrics batch.
func (r *LoadRunner) OnAck(ack *pb.Ack) {
	// Acknowledged seq
}
