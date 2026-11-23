package objstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"api-load-testing-engine/internal/store"
)

// ObjectStore defines operations for storing and retrieving failure traces and datasets.
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte, contentType string) error
	Get(ctx context.Context, key string) ([]byte, error)
	PresignGet(ctx context.Context, key string, expiry time.Duration) (string, error)
}

// MemObjectStore provides an in-memory object store.
type MemObjectStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

// NewMemObjectStore creates a new in-memory object store.
func NewMemObjectStore() *MemObjectStore {
	return &MemObjectStore{
		objects: make(map[string][]byte),
	}
}

func (m *MemObjectStore) Put(ctx context.Context, key string, data []byte, contentType string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]byte, len(data))
	copy(copied, data)
	m.objects[key] = copied
	return nil
}

func (m *MemObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, exists := m.objects[key]
	if !exists {
		return nil, fmt.Errorf("object %s not found", key)
	}
	copied := make([]byte, len(data))
	copy(copied, data)
	return copied, nil
}

func (m *MemObjectStore) PresignGet(ctx context.Context, key string, expiry time.Duration) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, exists := m.objects[key]; !exists {
		return "", fmt.Errorf("object %s not found", key)
	}
	return fmt.Sprintf("http://localhost:9000/loadtest/%s?expires=%d", key, time.Now().Add(expiry).Unix()), nil
}

// FailureTrace defines a detailed error trace captured on failure.
type FailureTrace struct {
	RunID        string            `json:"run_id"`
	WorkerID     string            `json:"worker_id"`
	Timestamp    time.Time         `json:"timestamp"`
	ErrorType    string            `json:"error_type"`
	URL          string            `json:"url"`
	Method       string            `json:"method"`
	StatusCode   int               `json:"status_code"`
	Headers      map[string]string `json:"headers"`
	ResponseBody string            `json:"response_body"`
}

// TraceSampler limits trace sampling to maxTracesPerSec (default: 10/sec).
type TraceSampler struct {
	objStore        ObjectStore
	metadataStore   store.Store
	maxPerSec       int64
	currentSec      int64
	sampledInCurSec int64
}

// NewTraceSampler creates a rate-limited failure trace sampler.
func NewTraceSampler(objStore ObjectStore, metadataStore store.Store, maxPerSec int64) *TraceSampler {
	if maxPerSec <= 0 {
		maxPerSec = 10
	}
	return &TraceSampler{
		objStore:      objStore,
		metadataStore: metadataStore,
		maxPerSec:     maxPerSec,
	}
}

// ShouldSample checks if a failure trace should be captured under rate limits.
func (s *TraceSampler) ShouldSample() bool {
	nowSec := time.Now().Unix()
	curSec := atomic.LoadInt64(&s.currentSec)

	if nowSec != curSec {
		if atomic.CompareAndSwapInt64(&s.currentSec, curSec, nowSec) {
			atomic.StoreInt64(&s.sampledInCurSec, 0)
		}
	}

	sampled := atomic.AddInt64(&s.sampledInCurSec, 1)
	return sampled <= s.maxPerSec
}

// RecordTrace samples and asynchronously uploads the failure trace.
func (s *TraceSampler) RecordTrace(trace FailureTrace) bool {
	if !s.ShouldSample() {
		return false
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		data, err := json.Marshal(trace)
		if err != nil {
			return
		}

		key := fmt.Sprintf("traces/%s/%s-%d.json", trace.RunID, trace.WorkerID, trace.Timestamp.UnixNano())
		if s.objStore != nil {
			_ = s.objStore.Put(ctx, key, data, "application/json")
		}

		if s.metadataStore != nil {
			_ = s.metadataStore.SaveFailureTrace(ctx, store.FailureTraceRecord{
				RunID:        trace.RunID,
				WorkerID:     trace.WorkerID,
				Timestamp:    trace.Timestamp,
				ErrorType:    trace.ErrorType,
				URL:          trace.URL,
				Method:       trace.Method,
				StatusCode:   trace.StatusCode,
				ResponseBody: trace.ResponseBody,
				ObjectKey:    key,
			})
		}
	}()

	return true
}
