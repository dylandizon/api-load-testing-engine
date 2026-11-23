package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	pb "api-load-testing-engine/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SimpleConfig defines task parameters for the skeleton walking runner.
type SimpleConfig struct {
	Target   string `json:"target"`
	Requests int    `json:"requests"`
}

// SimpleRunner implements the Handler interface for Milestone 1.
type SimpleRunner struct {
	client     *Client
	httpClient *http.Client

	mu        sync.Mutex
	activeRun string
	config    SimpleConfig
	cancel    context.CancelFunc
	seq       uint64
	ackedSeq  uint64
}

// NewSimpleRunner creates a new runner for the walking skeleton.
func NewSimpleRunner(httpClient *http.Client) *SimpleRunner {
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 5 * time.Second,
		}
	}
	return &SimpleRunner{
		httpClient: httpClient,
	}
}

// SetClient binds the gRPC client to the runner.
func (r *SimpleRunner) SetClient(client *Client) {
	r.client = client
}

// OnAssign receives run assignment and sends Ready after preparation.
func (r *SimpleRunner) OnAssign(assign *pb.Assign) error {
	r.mu.Lock()
	r.activeRun = assign.RunId

	var cfg SimpleConfig
	if len(assign.ConfigSlice) > 0 {
		if err := json.Unmarshal(assign.ConfigSlice, &cfg); err != nil {
			log.Printf("[runner] warning: unmarshal config slice: %v", err)
		}
	}
	if cfg.Requests <= 0 {
		cfg.Requests = 10
	}
	r.config = cfg
	r.mu.Unlock()

	r.client.SetActiveRun(assign.RunId)

	log.Printf("[runner] assigned run %s (target: %s, requests: %d). Sending Ready...", assign.RunId, cfg.Target, cfg.Requests)

	return r.client.Send(&pb.WorkerMessage{
		Msg: &pb.WorkerMessage_Ready{
			Ready: &pb.Ready{
				RunId: assign.RunId,
			},
		},
	})
}

// OnStart begins executing the assigned load.
func (r *SimpleRunner) OnStart(start *pb.Start) error {
	r.mu.Lock()
	runID := r.activeRun
	cfg := r.config
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.mu.Unlock()

	log.Printf("[runner] start received for run %s. Executing %d requests against %s...", runID, cfg.Requests, cfg.Target)

	go r.execute(ctx, runID, cfg)
	return nil
}

func (r *SimpleRunner) execute(ctx context.Context, runID string, cfg SimpleConfig) {
	var totalRequests uint64
	var err4xx uint64
	var err5xx uint64
	var timeouts uint64
	var bytesRecv uint64

	startTime := time.Now()

	for i := 0; i < cfg.Requests; i++ {
		select {
		case <-ctx.Done():
			log.Printf("[runner] run %s aborted after %d requests", runID, totalRequests)
			return
		default:
		}

		if cfg.Target != "" {
			req, err := http.NewRequestWithContext(ctx, "GET", cfg.Target, nil)
			if err == nil {
				resp, err := r.httpClient.Do(req)
				if err != nil {
					timeouts++
				} else {
					body, _ := io.ReadAll(resp.Body)
					bytesRecv += uint64(len(body))
					resp.Body.Close()

					if resp.StatusCode >= 400 && resp.StatusCode < 500 {
						err4xx++
					} else if resp.StatusCode >= 500 {
						err5xx++
					}
				}
			}
		}
		totalRequests++
	}

	endTime := time.Now()
	seq := atomic.AddUint64(&r.seq, 1)

	// Send final MetricsBatch
	metricsMsg := &pb.WorkerMessage{
		Msg: &pb.WorkerMessage_Metrics{
			Metrics: &pb.MetricsBatch{
				RunId:       runID,
				Seq:         seq,
				WindowStart: timestamppb.New(startTime),
				WindowEnd:   timestamppb.New(endTime),
				Counters: &pb.Counters{
					Requests:       totalRequests,
					Errors_4Xx:     err4xx,
					Errors_5Xx:     err5xx,
					Timeouts:       timeouts,
					BytesReceived:  bytesRecv,
				},
			},
		},
	}
	_ = r.client.Send(metricsMsg)

	log.Printf("[runner] completed %d requests for run %s. Sending StateChange(FINISHED)...", totalRequests, runID)

	// Send StateChange FINISHED
	stateMsg := &pb.WorkerMessage{
		Msg: &pb.WorkerMessage_State{
			State: &pb.StateChange{
				RunId:  runID,
				State:  pb.StateChange_FINISHED,
				Detail: fmt.Sprintf("completed %d requests", totalRequests),
			},
		},
	}
	_ = r.client.Send(stateMsg)
}

// OnStageChange handles stage updates.
func (r *SimpleRunner) OnStageChange(change *pb.StageChange) error {
	log.Printf("[runner] stage change to index %d", change.StageIndex)
	return nil
}

// OnAbort terminates any in-flight execution.
func (r *SimpleRunner) OnAbort(abort *pb.Abort) error {
	log.Printf("[runner] abort received: graceful=%v, reason=%s", abort.Graceful, abort.Reason)
	r.mu.Lock()
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Unlock()
	return nil
}

// OnAck tracks acknowledged sequence batches.
func (r *SimpleRunner) OnAck(ack *pb.Ack) {
	atomic.StoreUint64(&r.ackedSeq, ack.Seq)
	log.Printf("[runner] received ack for run %s seq %d", ack.RunId, ack.Seq)
}
