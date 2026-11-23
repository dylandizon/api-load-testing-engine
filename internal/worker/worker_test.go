package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"api-load-testing-engine/internal/controller"
	pb "api-load-testing-engine/proto"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWorker_EndToEndWalkingSkeleton(t *testing.T) {
	// 1. Mock Target HTTP API
	var targetHitCount uint64
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint64(&targetHitCount, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer targetServer.Close()

	// 2. Controller Setup
	reg := controller.NewRegistry()

	readyCh := make(chan string, 1)
	metricsCh := make(chan *pb.MetricsBatch, 1)
	stateCh := make(chan *pb.StateChange, 1)
	registeredCh := make(chan string, 1)

	var coordServer *controller.CoordinatorServer
	hooks := controller.ServerHooks{
		OnRegister: func(workerID string, r *pb.Register) {
			registeredCh <- workerID
		},
		OnReady: func(workerID string, r *pb.Ready) {
			readyCh <- r.RunId
		},
		OnMetricsBatch: func(workerID string, b *pb.MetricsBatch) {
			metricsCh <- b
		},
		OnStateChange: func(workerID string, s *pb.StateChange) {
			stateCh <- s
		},
	}
	coordServer = controller.NewCoordinatorServer(reg, hooks)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	pb.RegisterCoordinatorServer(grpcServer, coordServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	// 3. Worker Setup
	runner := NewSimpleRunner(targetServer.Client())
	workerCfg := ClientConfig{
		WorkerID:          "worker-skeleton-1",
		ControllerAddr:    lis.Addr().String(),
		Cores:             2,
		MemMB:             4096,
		BandwidthMBPS:     1000,
		HeartbeatInterval: 100 * time.Millisecond,
	}
	client := NewClient(workerCfg, runner)
	runner.SetClient(client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		_ = client.Start(ctx)
	}()
	defer client.Stop()

	// Wait for registration
	select {
	case id := <-registeredCh:
		if id != "worker-skeleton-1" {
			t.Fatalf("unexpected worker id registered: %s", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for worker registration")
	}

	// 4. Controller sends Assign
	runID := "test-run-skeleton"
	configBytes, _ := json.Marshal(SimpleConfig{
		Target:   targetServer.URL,
		Requests: 5,
	})

	err = coordServer.SendMessage("worker-skeleton-1", &pb.ControllerMessage{
		Msg: &pb.ControllerMessage_Assign{
			Assign: &pb.Assign{
				RunId:       runID,
				ConfigSlice: configBytes,
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to send Assign: %v", err)
	}

	// 5. Wait for Ready from worker
	select {
	case rID := <-readyCh:
		if rID != runID {
			t.Fatalf("expected ready for %s, got %s", runID, rID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for worker Ready")
	}

	// 6. Controller sends Start
	err = coordServer.SendMessage("worker-skeleton-1", &pb.ControllerMessage{
		Msg: &pb.ControllerMessage_Start{
			Start: &pb.Start{
				RunId:   runID,
				StartAt: timestamppb.Now(),
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to send Start: %v", err)
	}

	// 7. Verify MetricsBatch received
	select {
	case batch := <-metricsCh:
		if batch.RunId != runID {
			t.Fatalf("expected batch for %s, got %s", runID, batch.RunId)
		}
		if batch.Counters.Requests != 5 {
			t.Fatalf("expected 5 requests in counters, got %d", batch.Counters.Requests)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for MetricsBatch")
	}

	// 8. Verify StateChange FINISHED received
	select {
	case state := <-stateCh:
		if state.RunId != runID {
			t.Fatalf("expected state for %s, got %s", runID, state.RunId)
		}
		if state.State != pb.StateChange_FINISHED {
			t.Fatalf("expected FINISHED state, got %v", state.State)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for StateChange FINISHED")
	}

	// 9. Verify target server actually received all 5 requests
	hits := atomic.LoadUint64(&targetHitCount)
	if hits != 5 {
		t.Fatalf("expected 5 hits to target server, got %d", hits)
	}

	fmt.Println("Milestone 1 End-to-End Walking Skeleton verified successfully!")
}
