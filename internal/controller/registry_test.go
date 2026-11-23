package controller

import (
	"testing"
	"time"

	pb "api-load-testing-engine/proto"
)

func TestRegistry_RegisterAndGet(t *testing.T) {
	reg := NewRegistry()
	ch := make(chan *pb.ControllerMessage, 10)

	regMsg := &pb.Register{
		Cores:         8,
		MemMb:         16384,
		BandwidthMbps: 1000,
	}

	w := reg.Register("worker-1", regMsg, ch)
	if w.ID != "worker-1" {
		t.Fatalf("expected ID worker-1, got %s", w.ID)
	}
	if w.Cores != 8 || w.MemMB != 16384 {
		t.Fatalf("unexpected cores or mem: %+v", w)
	}
	if w.Status != "idle" {
		t.Fatalf("expected status idle, got %s", w.Status)
	}

	wCopy, ok := reg.GetWorker("worker-1")
	if !ok {
		t.Fatal("worker not found")
	}
	if wCopy.ID != "worker-1" {
		t.Fatalf("expected ID worker-1, got %s", wCopy.ID)
	}

	workers := reg.ListWorkers()
	if len(workers) != 1 {
		t.Fatalf("expected 1 worker, got %d", len(workers))
	}
}

func TestRegistry_HeartbeatAndLease(t *testing.T) {
	reg := NewRegistry()
	ch := make(chan *pb.ControllerMessage, 10)

	reg.Register("worker-1", &pb.Register{Cores: 4}, ch)

	// Update heartbeat
	hb := &pb.Heartbeat{
		CpuPct:          45.5,
		MemPct:          60.2,
		SchedulerLagMs: 0.5,
		ActiveRunId:     "run-123",
	}
	updated := reg.UpdateHeartbeat("worker-1", hb)
	if !updated {
		t.Fatal("heartbeat update failed")
	}

	w, _ := reg.GetWorker("worker-1")
	if w.CPUPct != 45.5 || w.ActiveRunID != "run-123" {
		t.Fatalf("heartbeat fields mismatch: %+v", w)
	}

	// Manually age the heartbeat to test lease timeout
	reg.mu.Lock()
	reg.workers["worker-1"].LastHeartbeat = time.Now().Add(-10 * time.Second)
	reg.mu.Unlock()

	suspects, dead := reg.CheckLeases(2*time.Second, 8*time.Second)
	if len(dead) != 1 || dead[0] != "worker-1" {
		t.Fatalf("expected worker-1 to be dead, got suspects=%v dead=%v", suspects, dead)
	}

	w, _ = reg.GetWorker("worker-1")
	if w.Status != "dead" {
		t.Fatalf("expected status dead, got %s", w.Status)
	}
}
