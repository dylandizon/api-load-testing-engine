package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"api-load-testing-engine/internal/metrics"
	"api-load-testing-engine/internal/store"
	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// RESTServer exposes the external REST API for CI/CD and CLI clients.
type RESTServer struct {
	orchestrator *Orchestrator
	store        store.Store
	mu           sync.RWMutex
	tests        map[string]*TestDefinition
	httpServer   *http.Server
}

// NewRESTServer creates a new REST API server instance.
func NewRESTServer(orch *Orchestrator, store store.Store) *RESTServer {
	s := &RESTServer{
		orchestrator: orch,
		store:        store,
		tests:        make(map[string]*TestDefinition),
	}
	return s
}

// Router returns an http.Handler implementing the endpoints.
func (s *RESTServer) Router() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/tests", s.handleTests)
	mux.HandleFunc("/tests/", s.handleTestByID)
	mux.HandleFunc("/runs/", s.handleRuns)

	// Built-in mock & health endpoints for testing and validation
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy"}`))
	})
	mux.HandleFunc("/mock/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	return mux
}

// Start listens and serves REST requests on the specified port.
func (s *RESTServer) Start(port int) error {
	addr := fmt.Sprintf(":%d", port)
	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      s.Router(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return s.httpServer.ListenAndServe()
}

// Stop gracefully shuts down the REST server.
func (s *RESTServer) Stop(ctx context.Context) error {
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

func (s *RESTServer) handleTests(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body error", http.StatusBadRequest)
		return
	}

	var testDef TestDefinition
	// Try parsing as JSON, fallback to YAML
	if err := json.Unmarshal(body, &testDef); err != nil {
		if yamlErr := yaml.Unmarshal(body, &testDef); yamlErr != nil {
			http.Error(w, fmt.Sprintf("invalid test definition format: %v", yamlErr), http.StatusBadRequest)
			return
		}
	}

	if testDef.Name == "" {
		testDef.Name = "unnamed-test"
	}
	testID := uuid.New().String()

	s.mu.Lock()
	s.tests[testID] = &testDef
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"id":   testID,
		"name": testDef.Name,
	})
}

func (s *RESTServer) handleTestByID(w http.ResponseWriter, r *http.Request) {
	// Path format: /tests/{id} or /tests/{id}/runs
	path := strings.TrimPrefix(r.URL.Path, "/tests/")
	parts := strings.Split(path, "/")
	testID := parts[0]

	s.mu.RLock()
	testDef, exists := s.tests[testID]
	s.mu.RUnlock()

	if !exists {
		http.Error(w, "test not found", http.StatusNotFound)
		return
	}

	if len(parts) == 1 && r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(testDef)
		return
	}

	if len(parts) == 2 && parts[1] == "runs" && r.Method == http.MethodPost {
		// POST /tests/{id}/runs
		runState, err := s.orchestrator.StartRun(context.Background(), testDef)
		if err != nil {
			http.Error(w, fmt.Sprintf("start run failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"id":     runState.ID,
			"status": runState.Status,
		})
		return
	}

	http.Error(w, "not found", http.StatusNotFound)
}

func (s *RESTServer) handleRuns(w http.ResponseWriter, r *http.Request) {
	// Path format: /runs/{id}, /runs/{id}/abort, /runs/{id}/result, /runs/{id}/metrics
	path := strings.TrimPrefix(r.URL.Path, "/runs/")
	parts := strings.Split(path, "/")
	runID := parts[0]

	runState, exists := s.orchestrator.GetRun(runID)
	if !exists {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	if len(parts) == 1 && r.Method == http.MethodGet {
		// GET /runs/{id}
		status, stage, degraded, exitCode := runState.GetStatus()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"id":             runState.ID,
			"status":         status,
			"current_stage":  stage,
			"degraded":       degraded,
			"degraded_notes": runState.GetDegradedNotes(),
			"exit_code":      exitCode,
			"failure_reason": runState.FailureReason,
		})
		return
	}

	if len(parts) == 2 {
		subpath := parts[1]

		switch subpath {
		case "abort":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			_ = s.orchestrator.AbortRun(runID, "manual abort via REST API")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "ABORTED",
			})
			return

		case "result":
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			s.handleRunResult(w, runState)
			return

		case "metrics":
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			ctx := r.Context()
			rollups, _ := s.store.GetRollups(ctx, runID, "")
			counters, _ := s.store.GetCounters(ctx, runID)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"rollups":  rollups,
				"counters": counters,
			})
			return

		case "traces":
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			ctx := r.Context()
			traces, _ := s.store.GetFailureTraces(ctx, runID)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"run_id": runID,
				"count":  len(traces),
				"traces": traces,
			})
			return
		}
	}

	http.Error(w, "not found", http.StatusNotFound)
}

func (s *RESTServer) handleRunResult(w http.ResponseWriter, run *RunState) {
	ctx := context.Background()
	rollups, _ := s.store.GetRollups(ctx, run.ID, "")
	countersList, _ := s.store.GetCounters(ctx, run.ID)

	// Combine all rollups and counters for the whole run
	var mergedCounters metrics.RunCountersRecord
	for _, c := range countersList {
		mergedCounters.Requests += c.Requests
		mergedCounters.Errors4xx += c.Errors4xx
		mergedCounters.Errors5xx += c.Errors5xx
		mergedCounters.Timeouts += c.Timeouts
		mergedCounters.AssertionFailures += c.AssertionFailures
	}

	// Merge histograms for cumulative evaluation
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

	// Evaluate test thresholds
	allPassed := true
	var thresholdResults []ThresholdResult

	for _, thDef := range run.Definition.Thresholds {
		parsed, err := ParseThreshold(thDef)
		if err != nil {
			continue
		}
		res := EvaluateThreshold(parsed, histMap[parsed.Metric], &mergedCounters)
		thresholdResults = append(thresholdResults, res)
		if !res.Passed {
			allPassed = false
		}
	}

	status, _, degraded, _ := run.GetStatus()
	exitCode := 0
	if status == StatusFailed {
		exitCode = 2
	} else if status == StatusAborted || !allPassed {
		exitCode = 1
	}

	run.SetExitCode(exitCode)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"run_id":         run.ID,
		"status":         status,
		"passed":         allPassed && status == StatusCompleted,
		"exit_code":      exitCode,
		"degraded":       degraded,
		"degraded_notes": run.GetDegradedNotes(),
		"thresholds":     thresholdResults,
	})
}
