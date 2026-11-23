package controller

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"api-load-testing-engine/internal/metrics"
	"api-load-testing-engine/internal/store"
	pb "api-load-testing-engine/proto"
)

func TestRESTAPI_Flow(t *testing.T) {
	memStore := store.NewMemStore()
	defer memStore.Close()

	reg := NewRegistry()
	hooks := ServerHooks{}
	server := NewCoordinatorServer(reg, hooks)
	merger := metrics.NewMerger(memStore, 500*time.Millisecond)
	server.SetMerger(merger)
	defer merger.Stop()

	orch := NewOrchestrator(reg, server, merger)
	restServer := NewRESTServer(orch, memStore)

	ts := httptest.NewServer(restServer.Router())
	defer ts.Close()

	// 1. Upload Test Definition (YAML)
	yamlPayload := `
name: checkout-benchmark
executor: closed
scenarios:
  checkout:
    vus: 10
    duration: 5s
stages:
  - duration: 5s
    target: 10
thresholds:
  - metric: http_req_duration
    expr: "p95 < 200"
    abort_on_fail: true
  - metric: error_rate
    expr: "rate < 0.01"
`

	resp, err := http.Post(ts.URL+"/tests", "application/x-yaml", bytes.NewBufferString(yamlPayload))
	if err != nil {
		t.Fatalf("POST /tests failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201 Created, got %d: %s", resp.StatusCode, string(b))
	}

	var createResp struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&createResp); err != nil {
		t.Fatalf("failed to decode create test response: %v", err)
	}
	if createResp.Name != "checkout-benchmark" || createResp.ID == "" {
		t.Fatalf("unexpected create response: %+v", createResp)
	}

	// 2. GET /tests/{id}
	getTestResp, err := http.Get(ts.URL + "/tests/" + createResp.ID)
	if err != nil {
		t.Fatalf("GET /tests/{id} failed: %v", err)
	}
	defer getTestResp.Body.Close()
	if getTestResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", getTestResp.StatusCode)
	}

	var fetchedDef TestDefinition
	if err := json.NewDecoder(getTestResp.Body).Decode(&fetchedDef); err != nil {
		t.Fatalf("failed to decode test definition: %v", err)
	}
	if fetchedDef.Name != "checkout-benchmark" || len(fetchedDef.Thresholds) != 2 {
		t.Fatalf("unexpected fetched definition: %+v", fetchedDef)
	}

	// 3. Register a worker so StartRun can succeed
	ch := make(chan *pb.ControllerMessage, 10)
	server.RegisterWorkerChannel("worker-test-1", ch)
	reg.Register("worker-test-1", &pb.Register{
		Cores: 4,
		MemMb: 8192,
	}, ch)

	// 4. POST /tests/{id}/runs
	startRunResp, err := http.Post(ts.URL+"/tests/"+createResp.ID+"/runs", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /tests/{id}/runs failed: %v", err)
	}
	defer startRunResp.Body.Close()

	if startRunResp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(startRunResp.Body)
		t.Fatalf("expected 202 Accepted, got %d: %s", startRunResp.StatusCode, string(b))
	}

	var runResp struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(startRunResp.Body).Decode(&runResp); err != nil {
		t.Fatalf("failed to decode start run response: %v", err)
	}
	if runResp.ID == "" {
		t.Fatalf("expected valid run ID")
	}

	// 5. GET /runs/{id}
	getStatusResp, err := http.Get(ts.URL + "/runs/" + runResp.ID)
	if err != nil {
		t.Fatalf("GET /runs/{id} failed: %v", err)
	}
	defer getStatusResp.Body.Close()
	if getStatusResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", getStatusResp.StatusCode)
	}

	var statusResp struct {
		ID       string `json:"id"`
		Status   string `json:"status"`
		Degraded bool   `json:"degraded"`
		ExitCode int    `json:"exit_code"`
	}
	if err := json.NewDecoder(getStatusResp.Body).Decode(&statusResp); err != nil {
		t.Fatalf("failed to decode run status: %v", err)
	}
	if statusResp.ID != runResp.ID {
		t.Fatalf("expected run ID %s, got %s", runResp.ID, statusResp.ID)
	}

	// 6. POST /runs/{id}/abort
	abortResp, err := http.Post(ts.URL+"/runs/"+runResp.ID+"/abort", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /runs/{id}/abort failed: %v", err)
	}
	defer abortResp.Body.Close()
	if abortResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", abortResp.StatusCode)
	}

	// 7. GET /runs/{id}/result
	resultResp, err := http.Get(ts.URL + "/runs/" + runResp.ID + "/result")
	if err != nil {
		t.Fatalf("GET /runs/{id}/result failed: %v", err)
	}
	defer resultResp.Body.Close()
	if resultResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resultResp.StatusCode)
	}

	var resData struct {
		RunID      string            `json:"run_id"`
		Status     string            `json:"status"`
		Passed     bool              `json:"passed"`
		ExitCode   int               `json:"exit_code"`
		Thresholds []ThresholdResult `json:"thresholds"`
	}
	if err := json.NewDecoder(resultResp.Body).Decode(&resData); err != nil {
		t.Fatalf("failed to decode result: %v", err)
	}
	if resData.RunID != runResp.ID {
		t.Fatalf("expected run ID %s, got %s", runResp.ID, resData.RunID)
	}
	if resData.ExitCode != 1 { // Aborted run should exit with 1
		t.Errorf("expected exit code 1 for aborted run, got %d", resData.ExitCode)
	}
}
