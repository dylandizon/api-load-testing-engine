package worker

import (
	"fmt"
	"testing"

	pb "api-load-testing-engine/proto"
)

func TestMetricsBuffer_QueueLimitAndFIFO(t *testing.T) {
	// Buffer holding up to 5 batches
	buf := NewMetricsBuffer(5)

	if buf.Len() != 0 {
		t.Fatalf("expected empty buffer, got %d", buf.Len())
	}

	// Enqueue 7 batches (seq 1 to 7) -> should keep newest 5 (seq 3 to 7)
	for i := uint64(1); i <= 7; i++ {
		buf.Enqueue(&pb.MetricsBatch{
			RunId: "test-run",
			Seq:   i,
		})
	}

	if buf.Len() != 5 {
		t.Fatalf("expected buffer length 5, got %d", buf.Len())
	}

	drained := buf.Drain()
	if len(drained) != 5 {
		t.Fatalf("expected 5 drained batches, got %d", len(drained))
	}
	if buf.Len() != 0 {
		t.Fatalf("expected buffer to be empty after drain, got %d", buf.Len())
	}

	// Verify sequential FIFO ordering
	expectedSeq := uint64(3)
	for _, b := range drained {
		if b.Seq != expectedSeq {
			t.Errorf("expected seq %d, got %d", expectedSeq, b.Seq)
		}
		expectedSeq++
	}
}

func TestClient_MetricsBufferDuringDisconnect(t *testing.T) {
	cfg := ClientConfig{
		WorkerID: "worker-buffer-test",
	}
	client := NewClient(cfg, nil)

	// Initially disconnected: batches enqueue into buffer
	for i := uint64(1); i <= 30; i++ {
		client.QueueMetrics(&pb.MetricsBatch{
			RunId: "run-offline",
			Seq:   i,
		})
	}

	buf := client.GetMetricsBuffer()
	if buf.Len() != 30 {
		t.Fatalf("expected 30 buffered batches during disconnect, got %d", buf.Len())
	}

	// Drain verifies lossless retention
	drained := buf.Drain()
	if len(drained) != 30 {
		t.Fatalf("expected 30 drained batches, got %d", len(drained))
	}
	for i, b := range drained {
		if b.Seq != uint64(i+1) {
			t.Errorf("batch index %d has seq %d, want %d", i, b.Seq, i+1)
		}
	}
	fmt.Printf("Successfully validated 30s metric buffer with %d lossless batches.\n", len(drained))
}
