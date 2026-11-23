package controller

import (
	"fmt"
	"io"
	"log"
	"sync"

	pb "api-load-testing-engine/proto"
)

// ServerHooks allows registering callbacks for events.
type ServerHooks struct {
	OnRegister     func(workerID string, reg *pb.Register)
	OnHeartbeat    func(workerID string, hb *pb.Heartbeat)
	OnReady        func(workerID string, ready *pb.Ready)
	OnMetricsBatch func(workerID string, batch *pb.MetricsBatch)
	OnStateChange  func(workerID string, state *pb.StateChange)
}

// CoordinatorServer implements pb.CoordinatorServer.
type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServer
	registry *Registry
	hooks    ServerHooks
	mu       sync.Mutex
	workerCh map[string]chan *pb.ControllerMessage
}

// NewCoordinatorServer creates a new Coordinator gRPC server instance.
func NewCoordinatorServer(registry *Registry, hooks ServerHooks) *CoordinatorServer {
	return &CoordinatorServer{
		registry: registry,
		hooks:    hooks,
		workerCh: make(map[string]chan *pb.ControllerMessage),
	}
}

// Connect handles the bidirectional stream between worker and controller.
func (s *CoordinatorServer) Connect(stream pb.Coordinator_ConnectServer) error {
	ctx := stream.Context()
	var workerID string
	msgChan := make(chan *pb.ControllerMessage, 64)

	// Goroutine to send messages from msgChan to the worker stream
	sendErrCh := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgChan:
				if !ok {
					return
				}
				if err := stream.Send(msg); err != nil {
					sendErrCh <- err
					return
				}
			}
		}
	}()

	defer func() {
		if workerID != "" {
			s.mu.Lock()
			delete(s.workerCh, workerID)
			s.mu.Unlock()
			s.registry.SetStatus(workerID, "dead")
			log.Printf("[controller] worker %s disconnected", workerID)
		}
		close(msgChan)
	}()

	for {
		select {
		case err := <-sendErrCh:
			return fmt.Errorf("error sending to worker %s: %w", workerID, err)
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if in.WorkerId == "" {
			continue
		}

		if workerID == "" {
			workerID = in.WorkerId
			s.mu.Lock()
			s.workerCh[workerID] = msgChan
			s.mu.Unlock()
		}

		switch m := in.Msg.(type) {
		case *pb.WorkerMessage_Register:
			s.registry.Register(workerID, m.Register, msgChan)
			log.Printf("[controller] registered worker %s (cores: %d, mem: %dMB)", workerID, m.Register.Cores, m.Register.MemMb)
			if s.hooks.OnRegister != nil {
				s.hooks.OnRegister(workerID, m.Register)
			}

		case *pb.WorkerMessage_Heartbeat:
			s.registry.UpdateHeartbeat(workerID, m.Heartbeat)
			if s.hooks.OnHeartbeat != nil {
				s.hooks.OnHeartbeat(workerID, m.Heartbeat)
			}

		case *pb.WorkerMessage_Ready:
			log.Printf("[controller] worker %s ready for run %s", workerID, m.Ready.RunId)
			if s.hooks.OnReady != nil {
				s.hooks.OnReady(workerID, m.Ready)
			}

		case *pb.WorkerMessage_Metrics:
			// Automatically send Ack for metrics batch seq
			_ = s.SendMessage(workerID, &pb.ControllerMessage{
				Msg: &pb.ControllerMessage_Ack{
					Ack: &pb.Ack{
						RunId: m.Metrics.RunId,
						Seq:   m.Metrics.Seq,
					},
				},
			})
			if s.hooks.OnMetricsBatch != nil {
				s.hooks.OnMetricsBatch(workerID, m.Metrics)
			}

		case *pb.WorkerMessage_State:
			log.Printf("[controller] worker %s state changed: %v (%s)", workerID, m.State.State, m.State.Detail)
			if s.hooks.OnStateChange != nil {
				s.hooks.OnStateChange(workerID, m.State)
			}
		}
	}
}

// SendMessage sends a ControllerMessage to a connected worker.
func (s *CoordinatorServer) SendMessage(workerID string, msg *pb.ControllerMessage) error {
	s.mu.Lock()
	ch, exists := s.workerCh[workerID]
	s.mu.Unlock()

	if !exists {
		return fmt.Errorf("worker %s not connected", workerID)
	}

	select {
	case ch <- msg:
		return nil
	default:
		return fmt.Errorf("worker %s message buffer full", workerID)
	}
}

// Broadcast sends a message to all connected workers.
func (s *CoordinatorServer) Broadcast(msg *pb.ControllerMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, ch := range s.workerCh {
		select {
		case ch <- msg:
		default:
			log.Printf("[controller] worker %s message buffer full on broadcast", id)
		}
	}
}
