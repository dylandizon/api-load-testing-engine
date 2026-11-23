package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"api-load-testing-engine/internal/controller"
	"api-load-testing-engine/internal/metrics"
	"api-load-testing-engine/internal/store"
	"api-load-testing-engine/internal/worker"
	pb "api-load-testing-engine/proto"
	"google.golang.org/grpc"
)

func TestEndToEnd_Milestone5_Run(t *testing.T) {
	// 1. Controller gRPC setup
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer grpcLis.Close()

	memStore := store.NewMemStore()
	defer memStore.Close()

	merger := metrics.NewMerger(memStore, 100*time.Millisecond)
	defer merger.Stop()

	reg := controller.NewRegistry()
	hooks := controller.ServerHooks{
		OnRegister: func(workerID string, reg *pb.Register) {
			t.Logf("Worker registered: %s (cores: %d)", workerID, reg.Cores)
		},
	}
	coordServer := controller.NewCoordinatorServer(reg, hooks)
	coordServer.SetMerger(merger)

	grpcServer := grpc.NewServer()
	pb.RegisterCoordinatorServer(grpcServer, coordServer)
	go func() {
		_ = grpcServer.Serve(grpcLis)
	}()
	defer grpcServer.Stop()

	orch := controller.NewOrchestrator(reg, coordServer, merger)
	orch.SetStore(memStore)

	// 2. Controller REST API setup
	restServer := controller.NewRESTServer(orch, memStore)
	httpServer := httptest.NewServer(restServer.Router())
	defer httpServer.Close()

	// 3. Connect 2 Real Workers
	workerCount := 2
	workerClients := make([]*worker.Client, workerCount)
	for i := 0; i < workerCount; i++ {
		wID := fmt.Sprintf("e2e-worker-%d", i+1)
		runner := worker.NewLoadRunner(nil)
		cfg := worker.ClientConfig{
			WorkerID:          wID,
			ControllerAddr:    grpcLis.Addr().String(),
			Cores:             2,
			MemMB:             2048,
			BandwidthMBPS:     1000,
			HeartbeatInterval: 200 * time.Millisecond,
		}
		c := worker.NewClient(cfg, runner)
		runner.SetClient(c)
		workerClients[i] = c

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func(client *worker.Client, ctx context.Context) {
			_ = client.Start(ctx)
		}(c, ctx)
	}

	// Wait for workers to register
	for i := 0; i < 20; i++ {
		if len(reg.ListWorkers()) == workerCount {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(reg.ListWorkers()) != workerCount {
		t.Fatalf("expected %d workers, registered %d", workerCount, len(reg.ListWorkers()))
	}

	// 4. Test YAML Definition using mock endpoint
	yamlDef := fmt.Sprintf(`
name: e2e-checkout-test
version: 1
base_url: %s/mock
protocol: http1
scenarios:
  - name: checkout
    executor: closed
    stages:
      - duration: 2s
        target: 4
    think_time: 20ms
    script: |
      let res = http.get('/health');
      check(res.status == 200, 'status is 200');
thresholds:
  - metric: http_req_duration
    expr: p95 < 500
    abort_on_fail: true
  - metric: error_rate
    expr: rate < 0.05
`, httpServer.URL)

	// 5. POST /tests
	createResp, err := http.Post(httpServer.URL+"/tests", "application/x-yaml", bytes.NewBufferString(yamlDef))
	if err != nil {
		t.Fatalf("POST /tests failed: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", createResp.StatusCode)
	}

	var testInfo struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	_ = json.NewDecoder(createResp.Body).Decode(&testInfo)

	// 6. POST /tests/{id}/runs
	startResp, err := http.Post(httpServer.URL+"/tests/"+testInfo.ID+"/runs", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /tests/{id}/runs failed: %v", err)
	}
	defer startResp.Body.Close()
	if startResp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", startResp.StatusCode)
	}

	var runInfo struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	_ = json.NewDecoder(startResp.Body).Decode(&runInfo)

	// 7. Poll until run completion
	pollURL := fmt.Sprintf("%s/runs/%s", httpServer.URL, runInfo.ID)
	resultURL := fmt.Sprintf("%s/runs/%s/result", httpServer.URL, runInfo.ID)

	var lastStatus string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		res, err := http.Get(pollURL)
		if err == nil {
			var st struct {
				Status string `json:"status"`
			}
			_ = json.NewDecoder(res.Body).Decode(&st)
			res.Body.Close()
			lastStatus = st.Status
			if isTerminal(lastStatus) {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	if lastStatus != "COMPLETED" {
		t.Fatalf("expected run status COMPLETED, got %s", lastStatus)
	}

	// Flush metrics merger
	merger.FlushAll()

	// 8. GET /runs/{id}/result
	resResp, err := http.Get(resultURL)
	if err != nil {
		t.Fatalf("GET /runs/{id}/result failed: %v", err)
	}
	defer resResp.Body.Close()

	var result struct {
		RunID      string                      `json:"run_id"`
		Status     string                      `json:"status"`
		Passed     bool                        `json:"passed"`
		ExitCode   int                         `json:"exit_code"`
		Thresholds []controller.ThresholdResult `json:"thresholds"`
	}
	_ = json.NewDecoder(resResp.Body).Decode(&result)

	t.Logf("Result: Passed=%v, ExitCode=%d, Thresholds=%+v", result.Passed, result.ExitCode, result.Thresholds)

	if !result.Passed {
		t.Errorf("expected test run to PASS, got passed=%v", result.Passed)
	}
	if result.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", result.ExitCode)
	}
	if len(result.Thresholds) != 2 {
		t.Errorf("expected 2 evaluated thresholds, got %d", len(result.Thresholds))
	}
	for _, th := range result.Thresholds {
		if !th.Passed {
			t.Errorf("threshold failed: %s (%s)", th.Metric, th.Detail)
		}
	}

	for _, c := range workerClients {
		c.Stop()
	}
}
