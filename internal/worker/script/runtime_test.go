package script

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"api-load-testing-engine/internal/metrics"
	"api-load-testing-engine/internal/protocol"
)

func TestScriptRuntime_ExecutionAndCheck(t *testing.T) {
	// Mock target API
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" && r.Method == "POST" {
			body, _ := io.ReadAll(r.Body)
			var creds map[string]string
			_ = json.Unmarshal(body, &creds)
			if creds["user"] == "admin" && creds["pass"] == "secret" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"token": "auth-jwt-123", "code": 200}`))
				return
			}
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.URL.Path == "/cart" && r.Method == "GET" {
			auth := r.Header.Get("Authorization")
			if auth == "Bearer auth-jwt-123" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"items": 3, "total": 99.5}`))
				return
			}
			w.WriteHeader(http.StatusForbidden)
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	agg := metrics.NewLocalAggregator("test-run-script")
	client := protocol.NewProtocolClient()

	scriptCode := `
		const res = http.post('/login', { user: data.username, pass: data.password });
		const loginCheck = check(res, {
			'status is 200': r => r.status === 200,
			'has token': r => !!r.json('token')
		});
		if (!loginCheck) {
			throw new Error('login failed');
		}

		const token = res.json('token');
		const cartRes = http.get('/cart', {
			headers: { 'Authorization': 'Bearer ' + token }
		});
		check(cartRes, {
			'cart status is 200': r => r.status === 200,
			'cart total is 99.5': r => r.json('total') === 99.5
		});
	`

	rt, err := NewRuntime(scriptCode, ts.URL, agg, client)
	if err != nil {
		t.Fatalf("failed to create runtime: %v", err)
	}

	vm := rt.NewVMInstance()
	userData := map[string]interface{}{
		"username": "admin",
		"password": "secret",
	}

	ctx := context.Background()
	err = vm.ExecuteIteration(ctx, userData, time.Now())
	if err != nil {
		t.Fatalf("iteration failed: %v", err)
	}

	// Verify metrics snapshot
	batch := agg.SnapshotAndReset()
	if batch.Counters.Requests != 2 {
		t.Fatalf("expected 2 requests, got %d", batch.Counters.Requests)
	}
	if batch.Counters.Errors_4Xx != 0 || batch.Counters.Errors_5Xx != 0 {
		t.Fatalf("expected 0 errors, got 4xx: %d, 5xx: %d", batch.Counters.Errors_4Xx, batch.Counters.Errors_5Xx)
	}
	if batch.Counters.AssertionFailures != 0 {
		t.Fatalf("expected 0 assertion failures, got %d", batch.Counters.AssertionFailures)
	}
}

func TestScriptRuntime_CoordinatedOmissionDelay(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok": true}`))
	}))
	defer ts.Close()

	agg := metrics.NewLocalAggregator("test-run-co")
	client := protocol.NewProtocolClient()

	scriptCode := `http.get('/test');`
	rt, err := NewRuntime(scriptCode, ts.URL, agg, client)
	if err != nil {
		t.Fatalf("failed to create runtime: %v", err)
	}

	vm := rt.NewVMInstance()

	// Simulate that the intended arrival was 50ms ago (scheduler lag)
	intendedTime := time.Now().Add(-50 * time.Millisecond)
	err = vm.ExecuteIteration(context.Background(), nil, intendedTime)
	if err != nil {
		t.Fatalf("iteration failed: %v", err)
	}

	batch := agg.SnapshotAndReset()
	if len(batch.Histograms) == 0 {
		t.Fatal("expected histogram recorded")
	}

	// Find the general http_req_duration histogram
	var rawHist []byte
	for _, h := range batch.Histograms {
		if h.Name == "http_req_duration" {
			rawHist = h.HdrHistogram
			break
		}
	}
	if len(rawHist) == 0 {
		t.Fatal("http_req_duration histogram not found")
	}

	hist, err := metrics.DecodeHistogram(rawHist)
	if err != nil {
		t.Fatalf("decode histogram error: %v", err)
	}

	summary := metrics.ComputeSummary(hist)
	// The recorded latency should be >= 50ms because lag was charged to the first request
	if summary.Min < 48.0 {
		t.Fatalf("expected coordinated omission latency >= 50ms, got %f ms", summary.Min)
	}
}
