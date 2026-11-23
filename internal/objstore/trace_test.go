package objstore

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"api-load-testing-engine/internal/store"
)

func TestObjectStore_MemOperations(t *testing.T) {
	os := NewMemObjectStore()
	ctx := context.Background()

	key := "traces/run-1/worker-1.json"
	data := []byte(`{"error":"500 Internal Server Error"}`)

	if err := os.Put(ctx, key, data, "application/json"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	fetched, err := os.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(fetched) != string(data) {
		t.Fatalf("expected data %s, got %s", string(data), string(fetched))
	}

	presigned, err := os.PresignGet(ctx, key, 15*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet failed: %v", err)
	}
	if presigned == "" {
		t.Fatalf("expected non-empty presigned URL")
	}
}

func TestTraceSampler_RateLimiting(t *testing.T) {
	memStore := store.NewMemStore()
	defer memStore.Close()

	objStore := NewMemObjectStore()
	// Rate limit: 5 traces per second
	sampler := NewTraceSampler(objStore, memStore, 5)

	sampledCount := 0
	droppedCount := 0

	for i := 0; i < 20; i++ {
		trace := FailureTrace{
			RunID:        "run-sample-test",
			WorkerID:     "w1",
			Timestamp:    time.Now(),
			ErrorType:    "HTTP_500",
			URL:          "http://example.com/checkout",
			Method:       "POST",
			StatusCode:   500,
			ResponseBody: "Database connection failed",
		}
		if sampler.RecordTrace(trace) {
			sampledCount++
		} else {
			droppedCount++
		}
	}

	// Wait 50ms for asynchronous store writes
	time.Sleep(50 * time.Millisecond)

	if sampledCount != 5 {
		t.Errorf("expected exactly 5 traces sampled in 1s burst, got %d", sampledCount)
	}
	if droppedCount != 15 {
		t.Errorf("expected 15 traces dropped, got %d", droppedCount)
	}

	// Verify metadata persisted in store
	ctx := context.Background()
	traces, err := memStore.GetFailureTraces(ctx, "run-sample-test")
	if err != nil {
		t.Fatalf("GetFailureTraces error: %v", err)
	}
	if len(traces) != 5 {
		t.Fatalf("expected 5 traces stored in metadata store, got %d", len(traces))
	}

	// Verify object store payload
	for _, tr := range traces {
		blob, err := objStore.Get(ctx, tr.ObjectKey)
		if err != nil {
			t.Fatalf("failed to fetch trace from object store: %v", err)
		}
		var parsed FailureTrace
		if err := json.Unmarshal(blob, &parsed); err != nil {
			t.Fatalf("failed to parse trace JSON: %v", err)
		}
		if parsed.StatusCode != 500 {
			t.Errorf("expected status 500, got %d", parsed.StatusCode)
		}
	}
}
