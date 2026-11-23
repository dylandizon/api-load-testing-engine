package worker

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	pb "api-load-testing-engine/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClientConfig holds configuration for a worker.
type ClientConfig struct {
	WorkerID          string
	ControllerAddr    string
	Cores             int32
	MemMB             int32
	BandwidthMBPS     int32
	HeartbeatInterval time.Duration
	ResumeRunID       string
}

// Handler handles events from the controller.
type Handler interface {
	OnAssign(assign *pb.Assign) error
	OnStart(start *pb.Start) error
	OnStageChange(change *pb.StageChange) error
	OnAbort(abort *pb.Abort) error
	OnAck(ack *pb.Ack)
}

// MetricsBuffer holds metrics batches during network/controller disconnections.
type MetricsBuffer struct {
	mu      sync.Mutex
	maxSize int
	batches []*pb.MetricsBatch
}

// NewMetricsBuffer creates a bounded queue for up to maxSize batches.
func NewMetricsBuffer(maxSize int) *MetricsBuffer {
	if maxSize <= 0 {
		maxSize = 60 // 60 seconds of 1-second batches
	}
	return &MetricsBuffer{
		maxSize: maxSize,
		batches: make([]*pb.MetricsBatch, 0, maxSize),
	}
}

// Enqueue adds a batch, dropping oldest if full.
func (mb *MetricsBuffer) Enqueue(batch *pb.MetricsBatch) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if len(mb.batches) >= mb.maxSize {
		mb.batches = mb.batches[1:]
	}
	mb.batches = append(mb.batches, batch)
}

// Drain returns and clears all buffered batches.
func (mb *MetricsBuffer) Drain() []*pb.MetricsBatch {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if len(mb.batches) == 0 {
		return nil
	}
	res := mb.batches
	mb.batches = make([]*pb.MetricsBatch, 0, mb.maxSize)
	return res
}

// Len returns the count of currently buffered batches.
func (mb *MetricsBuffer) Len() int {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return len(mb.batches)
}

// Client manages connection to the controller.
type Client struct {
	cfg           ClientConfig
	handler       Handler
	metricsBuffer *MetricsBuffer

	mu            sync.Mutex
	conn          *grpc.ClientConn
	stream        pb.Coordinator_ConnectClient
	outbox        chan *pb.WorkerMessage
	cancel        context.CancelFunc
	activeRun     string
	connected     bool
}

// NewClient creates a new worker gRPC client.
func NewClient(cfg ClientConfig, handler Handler) *Client {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 1 * time.Second
	}
	return &Client{
		cfg:           cfg,
		handler:       handler,
		metricsBuffer: NewMetricsBuffer(60),
		outbox:        make(chan *pb.WorkerMessage, 128),
	}
}

// GetMetricsBuffer returns the internal metrics buffer for testing.
func (c *Client) GetMetricsBuffer() *MetricsBuffer {
	return c.metricsBuffer
}

// QueueMetrics enqueues a metrics batch into the buffer and drains to stream if connected.
func (c *Client) QueueMetrics(batch *pb.MetricsBatch) {
	if batch == nil {
		return
	}
	c.metricsBuffer.Enqueue(batch)
	c.drainMetricsBuffer()
}

func (c *Client) drainMetricsBuffer() {
	c.mu.Lock()
	connected := c.connected
	c.mu.Unlock()

	if !connected {
		return
	}

	for _, batch := range c.metricsBuffer.Drain() {
		_ = c.Send(&pb.WorkerMessage{
			Msg: &pb.WorkerMessage_Metrics{
				Metrics: batch,
			},
		})
	}
}

// SetActiveRun updates the active run ID reported in heartbeats.
func (c *Client) SetActiveRun(runID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.activeRun = runID
}

// Send queues a message to be sent to the controller.
func (c *Client) Send(msg *pb.WorkerMessage) error {
	msg.WorkerId = c.cfg.WorkerID
	select {
	case c.outbox <- msg:
		return nil
	default:
		return fmt.Errorf("worker outbox full")
	}
}

// Start begins connecting to the controller and processing messages with reconnect.
func (c *Client) Start(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := c.connectAndRun(ctx)
		if err != nil && ctx.Err() == nil {
			log.Printf("[worker %s] connection error: %v. Reconnecting in 1s...", c.cfg.WorkerID, err)
			select {
			case <-time.After(1 * time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// Stop closes the client connection.
func (c *Client) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.mu.Unlock()
}

func (c *Client) connectAndRun(ctx context.Context) error {
	conn, err := grpc.DialContext(ctx, c.cfg.ControllerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(3*time.Second),
	)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	defer conn.Close()

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	client := pb.NewCoordinatorClient(conn)
	stream, err := client.Connect(ctx)
	if err != nil {
		return fmt.Errorf("stream connect failed: %w", err)
	}

	c.mu.Lock()
	c.stream = stream
	c.mu.Unlock()

	log.Printf("[worker %s] connected to controller at %s", c.cfg.WorkerID, c.cfg.ControllerAddr)

	c.mu.Lock()
	resumeID := c.activeRun
	if resumeID == "" {
		resumeID = c.cfg.ResumeRunID
	}
	c.mu.Unlock()

	// Send Register message immediately
	err = stream.Send(&pb.WorkerMessage{
		WorkerId: c.cfg.WorkerID,
		Msg: &pb.WorkerMessage_Register{
			Register: &pb.Register{
				Cores:         c.cfg.Cores,
				MemMb:         c.cfg.MemMB,
				BandwidthMbps: c.cfg.BandwidthMBPS,
				ResumeRunId:   resumeID,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to send register: %w", err)
	}

	c.mu.Lock()
	c.connected = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.connected = false
		c.mu.Unlock()
	}()

	// Drain any metrics buffered during network disconnect
	c.drainMetricsBuffer()

	errCh := make(chan error, 3)

	// Goroutine 1: Send queued outbox messages
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-c.outbox:
				if err := stream.Send(msg); err != nil {
					errCh <- fmt.Errorf("send error: %w", err)
					return
				}
			}
		}
	}()

	// Goroutine 2: Heartbeat ticker
	go func() {
		ticker := time.NewTicker(c.cfg.HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.mu.Lock()
				runID := c.activeRun
				c.mu.Unlock()

				hbMsg := &pb.WorkerMessage{
					WorkerId: c.cfg.WorkerID,
					Msg: &pb.WorkerMessage_Heartbeat{
						Heartbeat: &pb.Heartbeat{
							CpuPct:          10.0, // Mock stats for skeleton
							MemPct:          25.0,
							SchedulerLagMs: 0.1,
							ActiveRunId:     runID,
						},
					},
				}
				if err := c.Send(hbMsg); err != nil {
					log.Printf("[worker %s] failed to queue heartbeat: %v", c.cfg.WorkerID, err)
				}
			}
		}
	}()

	// Goroutine 3: Read incoming messages from controller
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				errCh <- fmt.Errorf("server closed stream")
				return
			}
			if err != nil {
				errCh <- fmt.Errorf("recv error: %w", err)
				return
			}

			if c.handler != nil {
				switch m := in.Msg.(type) {
				case *pb.ControllerMessage_Assign:
					if err := c.handler.OnAssign(m.Assign); err != nil {
						log.Printf("[worker %s] OnAssign error: %v", c.cfg.WorkerID, err)
					}
				case *pb.ControllerMessage_Start:
					if err := c.handler.OnStart(m.Start); err != nil {
						log.Printf("[worker %s] OnStart error: %v", c.cfg.WorkerID, err)
					}
				case *pb.ControllerMessage_StageChange:
					if err := c.handler.OnStageChange(m.StageChange); err != nil {
						log.Printf("[worker %s] OnStageChange error: %v", c.cfg.WorkerID, err)
					}
				case *pb.ControllerMessage_Abort:
					if err := c.handler.OnAbort(m.Abort); err != nil {
						log.Printf("[worker %s] OnAbort error: %v", c.cfg.WorkerID, err)
					}
				case *pb.ControllerMessage_Ack:
					c.handler.OnAck(m.Ack)
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
