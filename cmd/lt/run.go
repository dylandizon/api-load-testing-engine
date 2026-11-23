package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"api-load-testing-engine/internal/controller"
)

type createTestResp struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type startRunResp struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type runStatusResp struct {
	ID            string `json:"id"`
	Status        string `json:"status"`
	CurrentStage  int    `json:"current_stage"`
	Degraded      bool   `json:"degraded"`
	ExitCode      int    `json:"exit_code"`
	FailureReason string `json:"failure_reason"`
}

type runResultResp struct {
	RunID      string                      `json:"run_id"`
	Status     string                      `json:"status"`
	Passed     bool                        `json:"passed"`
	ExitCode   int                         `json:"exit_code"`
	Degraded   bool                        `json:"degraded"`
	Thresholds []controller.ThresholdResult `json:"thresholds"`
}

func runCLI(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	controllerURL := fs.String("controller", "http://localhost:8080", "Controller REST API URL")
	_ = fs.Parse(args)

	positional := fs.Args()
	if len(positional) == 0 {
		fmt.Println("Error: missing test definition file.")
		fmt.Println("Usage: lt run <test.yaml> [--controller http://localhost:8080]")
		os.Exit(2)
	}

	testFilePath := positional[0]
	fileBytes, err := os.ReadFile(testFilePath)
	if err != nil {
		fmt.Printf("Error reading test file '%s': %v\n", testFilePath, err)
		os.Exit(2)
	}

	client := &http.Client{Timeout: 10 * time.Second}

	// 1. POST /tests
	createURL := fmt.Sprintf("%s/tests", *controllerURL)
	resp, err := client.Post(createURL, "application/x-yaml", bytes.NewReader(fileBytes))
	if err != nil {
		fmt.Printf("Error connecting to controller at %s: %v\n", *controllerURL, err)
		os.Exit(2)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		fmt.Printf("Controller rejected test definition (%d): %s\n", resp.StatusCode, string(b))
		os.Exit(2)
	}

	var testInfo createTestResp
	_ = json.NewDecoder(resp.Body).Decode(&testInfo)
	fmt.Printf("[lt run] Uploaded test '%s' (ID: %s)\n", testInfo.Name, testInfo.ID)

	// 2. POST /tests/{id}/runs
	startURL := fmt.Sprintf("%s/tests/%s/runs", *controllerURL, testInfo.ID)
	startResp, err := client.Post(startURL, "application/json", nil)
	if err != nil {
		fmt.Printf("Error starting run: %v\n", err)
		os.Exit(2)
	}
	defer startResp.Body.Close()

	if startResp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(startResp.Body)
		fmt.Printf("Controller failed to start run (%d): %s\n", startResp.StatusCode, string(b))
		os.Exit(2)
	}

	var runInfo startRunResp
	_ = json.NewDecoder(startResp.Body).Decode(&runInfo)
	fmt.Printf("[lt run] Started run ID: %s. Monitoring live execution...\n", runInfo.ID)

	// 3. Poll GET /runs/{id}
	pollURL := fmt.Sprintf("%s/runs/%s", *controllerURL, runInfo.ID)
	resultURL := fmt.Sprintf("%s/runs/%s/result", *controllerURL, runInfo.ID)

	startTime := time.Now()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		res, err := client.Get(pollURL)
		if err != nil {
			fmt.Printf("Warning: poll error: %v\n", err)
			continue
		}

		var status runStatusResp
		_ = json.NewDecoder(res.Body).Decode(&status)
		res.Body.Close()

		elapsed := time.Since(startTime).Truncate(time.Second)
		degradedMsg := ""
		if status.Degraded {
			degradedMsg = " [DEGRADED]"
		}

		fmt.Printf("\r[%s] Status: %-12s | Stage: %d%s", elapsed, status.Status, status.CurrentStage, degradedMsg)

		if isTerminal(status.Status) {
			fmt.Println()
			break
		}
	}

	// 4. Fetch GET /runs/{id}/result
	resultResp, err := client.Get(resultURL)
	if err != nil {
		fmt.Printf("Error fetching run result: %v\n", err)
		os.Exit(2)
	}
	defer resultResp.Body.Close()

	var result runResultResp
	_ = json.NewDecoder(resultResp.Body).Decode(&result)

	fmt.Println("\n=================== TEST RESULT ===================")
	fmt.Printf("Run ID:   %s\n", result.RunID)
	fmt.Printf("Status:   %s\n", result.Status)
	if result.Degraded {
		fmt.Println("Warning:  Run was marked DEGRADED (generator saturated or worker lost)")
	}
	fmt.Println("----------------- Threshold Check -----------------")
	if len(result.Thresholds) == 0 {
		fmt.Println("No thresholds defined.")
	} else {
		for _, th := range result.Thresholds {
			tag := "[PASS]"
			if !th.Passed {
				tag = "[FAIL]"
			}
			fmt.Printf("%-6s %-20s | %s\n", tag, th.Metric, th.Detail)
		}
	}
	fmt.Println("---------------------------------------------------")
	fmt.Printf("Overall Outcome: Passed=%v | Exit Code=%d\n", result.Passed, result.ExitCode)
	fmt.Println("===================================================")

	os.Exit(result.ExitCode)
}

func isTerminal(status string) bool {
	return status == "COMPLETED" || status == "ABORTED" || status == "FAILED"
}
