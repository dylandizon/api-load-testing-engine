package controller

import (
	"context"
	"testing"
	"time"
)

func TestStartBarrier_AllReady(t *testing.T) {
	assigned := []string{"w1", "w2", "w3"}
	barrier := NewStartBarrier("run-barrier-1", assigned, 500*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// w1 and w2 report ready
	barrier.RecordReady("w1")
	barrier.RecordReady("w2")

	// Goroutine to record w3 after 50ms
	go func() {
		time.Sleep(50 * time.Millisecond)
		barrier.RecordReady("w3")
	}()

	startAt, err := barrier.AwaitStartTimestamp(ctx, 1*time.Second)
	if err != nil {
		t.Fatalf("expected barrier to succeed, got error: %v", err)
	}

	now := time.Now()
	if !startAt.After(now) {
		t.Fatalf("expected startAt to be in the future, got %v <= %v", startAt, now)
	}
}

func TestStartBarrier_Timeout(t *testing.T) {
	assigned := []string{"w1", "w2"}
	barrier := NewStartBarrier("run-barrier-timeout", assigned, 500*time.Millisecond)

	ctx := context.Background()

	// Only w1 reports ready, w2 never reports
	barrier.RecordReady("w1")

	_, err := barrier.AwaitStartTimestamp(ctx, 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected barrier timeout error, got nil")
	}
}
