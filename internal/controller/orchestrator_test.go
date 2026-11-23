package controller

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"api-load-testing-engine/internal/metrics"
	"api-load-testing-engine/internal/scheduler"
	"api-load-testing-engine/internal/store"
	pb "api-load-testing-engine/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestOrchestrator_MultiWorkerSynchronizedRun(t *testing.T) {
	// 1. Controller Server Setup
	reg := NewRegistry()
	coordServer := NewCoordinatorServer(reg, ServerHooks{})
	memStore := store.NewMemStore()
	defer memStore.Close()

	merger := metrics.NewMerger(memStore, 50*time.Millisecond)
	defer merger.Stop()
	coordServer.SetMerger(merger)

	orch := NewOrchestrator(reg, coordServer, merger)

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

	// 2. Connect 3 Simulated Workers
	workerIDs := []string{"sim-worker-1", "sim-worker-2", "sim-worker-3"}
	var startTimestamps sync.Map
	var stageChangeCounts sync.Map

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, wID := range workerIDs {
		conn, err := grpc.DialContext(ctx, lis.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		if err != nil {
			t.Fatalf("worker dial error: %v", err)
		}
		defer conn.Close()

		client := pb.NewCoordinatorClient(conn)
		stream, err := client.Connect(ctx)
		if err != nil {
			t.Fatalf("worker stream error: %v", err)
		}

		// Send Register
		_ = stream.Send(&pb.WorkerMessage{
			WorkerId: wID,
			Msg: &pb.WorkerMessage_Register{
				Register: &pb.Register{Cores: 4, MemMb: 8192},
			},
		})

		// Worker stream listener
		go func(id string, s pb.Coordinator_ConnectClient) {
			for {
				in, err := s.Recv()
				if err != nil {
					return
				}
				switch m := in.Msg.(type) {
				case *pb.ControllerMessage_Assign:
					// Reply Ready
					_ = s.Send(&pb.WorkerMessage{
						WorkerId: id,
						Msg: &pb.WorkerMessage_Ready{
							Ready: &pb.Ready{RunId: m.Assign.RunId},
						},
					})
				case *pb.ControllerMessage_Start:
					startTimestamps.Store(id, m.Start.StartAt.AsTime())
				case *pb.ControllerMessage_StageChange:
					val, _ := stageChangeCounts.LoadOrStore(id, 0)
					stageChangeCounts.Store(id, val.(int)+1)
				}
			}
		}(wID, stream)
	}

	// Allow registration to process
	time.Sleep(100 * time.Millisecond)

	// 3. Define a 2-stage test
	testDef := &TestDefinition{
		Name:    "multi-worker-flow",
		BaseURL: "http://example.com",
		Scenarios: []ScenarioDefinition{
			{
				Name:     "scenario-1",
				Executor: "open",
				MaxVUs:   300,
				Stages: []scheduler.Stage{
					{Duration: 100 * time.Millisecond, Target: 60}, // Stage 0
					{Duration: 100 * time.Millisecond, Target: 90}, // Stage 1
				},
				Script: `http.get('/');`,
			},
		},
	}

	// 4. Start Run via Orchestrator
	runState, err := orch.StartRun(ctx, testDef)
	if err != nil {
		t.Fatalf("failed to start run: %v", err)
	}

	// 5. Wait for Run to reach RUNNING status
	deadline := time.Now().Add(4 * time.Second)
	for {
		status, _, _, _ := runState.GetStatus()
		if status == StatusRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for run to reach RUNNING status, current: %s", status)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 6. Verify all 3 workers received Start with the EXACT same future timestamp
	var firstTS time.Time
	for _, wID := range workerIDs {
		val, ok := startTimestamps.Load(wID)
		if !ok {
			t.Fatalf("worker %s did not receive Start message", wID)
		}
		ts := val.(time.Time)
		if firstTS.IsZero() {
			firstTS = ts
		} else if !ts.Equal(firstTS) {
			t.Fatalf("worker %s start timestamp mismatch: got %v, want %v", wID, ts, firstTS)
		}
	}

	// 7. Wait for run to progress through stages to DRAINING
	deadline = time.Now().Add(5 * time.Second)
	for {
		status, stage, _, _ := runState.GetStatus()
		if status == StatusDraining || status == StatusCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for stages to finish, current status: %s, stage: %d", status, stage)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 8. Simulate workers reporting FINISHED
	for _, wID := range workerIDs {
		runState.SetWorkerState(wID, "FINISHED")
	}

	// Wait for stageManager to transition to COMPLETED
	time.Sleep(300 * time.Millisecond)
	status, _, _, _ := runState.GetStatus()
	if status != StatusCompleted {
		t.Fatalf("expected run to reach COMPLETED, got %s", status)
	}
}
