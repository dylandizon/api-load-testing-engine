package controller

import (
	"sync"
	"time"

	pb "api-load-testing-engine/proto"
)

// Worker represents a connected worker node.
type Worker struct {
	ID              string
	Cores           int32
	MemMB           int32
	BandwidthMBPS   int32
	Status          string // "idle", "busy", "suspect", "dead"
	LastHeartbeat   time.Time
	CPUPct          float64
	MemPct          float64
	SchedulerLagMS  float64
	ActiveRunID     string
	MsgChan         chan *pb.ControllerMessage
	RegisteredAt    time.Time
}

// Registry maintains active worker connections and their statuses.
type Registry struct {
	mu      sync.RWMutex
	workers map[string]*Worker
}

// NewRegistry creates an empty worker registry.
func NewRegistry() *Registry {
	return &Registry{
		workers: make(map[string]*Worker),
	}
}

// Register adds or updates a worker in the registry.
func (r *Registry) Register(id string, reg *pb.Register, msgChan chan *pb.ControllerMessage) *Worker {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, exists := r.workers[id]
	if !exists {
		w = &Worker{
			ID:           id,
			RegisteredAt: time.Now(),
		}
		r.workers[id] = w
	}

	w.Cores = reg.Cores
	w.MemMB = reg.MemMb
	w.BandwidthMBPS = reg.BandwidthMbps
	w.Status = "idle"
	w.LastHeartbeat = time.Now()
	w.MsgChan = msgChan

	return w
}

// UpdateHeartbeat updates metrics and heartbeat timestamp for a worker.
func (r *Registry) UpdateHeartbeat(id string, hb *pb.Heartbeat) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	w, exists := r.workers[id]
	if !exists {
		return false
	}

	w.CPUPct = hb.CpuPct
	w.MemPct = hb.MemPct
	w.SchedulerLagMS = hb.SchedulerLagMs
	w.ActiveRunID = hb.ActiveRunId
	w.LastHeartbeat = time.Now()
	if w.Status == "suspect" {
		w.Status = "idle"
	}
	return true
}

// SetStatus updates a worker's status string.
func (r *Registry) SetStatus(id string, status string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if w, exists := r.workers[id]; exists {
		w.Status = status
	}
}

// GetWorker returns a copy of a worker by ID.
func (r *Registry) GetWorker(id string) (*Worker, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	w, exists := r.workers[id]
	if !exists {
		return nil, false
	}
	// Return shallow copy
	copy := *w
	return &copy, true
}

// ListWorkers returns a list of all currently known workers.
func (r *Registry) ListWorkers() []*Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]*Worker, 0, len(r.workers))
	for _, w := range r.workers {
		copy := *w
		list = append(list, &copy)
	}
	return list
}

// RemoveWorker deletes a worker from registry.
func (r *Registry) RemoveWorker(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.workers, id)
}

// CheckLeases checks for workers that haven't sent a heartbeat within timeout.
// Returns slice of worker IDs that became suspect or dead.
func (r *Registry) CheckLeases(suspectTimeout, deadTimeout time.Duration) (suspects []string, dead []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for id, w := range r.workers {
		elapsed := now.Sub(w.LastHeartbeat)
		if elapsed > deadTimeout && w.Status != "dead" {
			w.Status = "dead"
			dead = append(dead, id)
		} else if elapsed > suspectTimeout && w.Status == "idle" {
			w.Status = "suspect"
			suspects = append(suspects, id)
		}
	}
	return suspects, dead
}
