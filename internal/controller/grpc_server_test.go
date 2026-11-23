package controller

import (
	"context"
	"net"
	"testing"
	"time"

	pb "api-load-testing-engine/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func setupTestServer(t *testing.T, hooks ServerHooks) (*Registry, *bufconn.Listener, func()) {
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	reg := NewRegistry()
	srv := NewCoordinatorServer(reg, hooks)
	pb.RegisterCoordinatorServer(s, srv)

	go func() {
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("serve error: %v", err)
		}
	}()

	cleanup := func() {
		s.Stop()
		lis.Close()
	}

	return reg, lis, cleanup
}

func dialTestServer(ctx context.Context, lis *bufconn.Listener) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func TestCoordinatorServer_ConnectAndRegister(t *testing.T) {
	regCh := make(chan string, 1)
	hooks := ServerHooks{
		OnRegister: func(workerID string, reg *pb.Register) {
			regCh <- workerID
		},
	}

	reg, lis, cleanup := setupTestServer(t, hooks)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := dialTestServer(ctx, lis)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := pb.NewCoordinatorClient(conn)
	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	// Send Register
	err = stream.Send(&pb.WorkerMessage{
		WorkerId: "test-worker-1",
		Msg: &pb.WorkerMessage_Register{
			Register: &pb.Register{
				Cores:         4,
				MemMb:         8192,
				BandwidthMbps: 1000,
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to send register: %v", err)
	}

	select {
	case id := <-regCh:
		if id != "test-worker-1" {
			t.Fatalf("unexpected worker registered: %s", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for OnRegister")
	}

	w, ok := reg.GetWorker("test-worker-1")
	if !ok {
		t.Fatal("worker not found in registry")
	}
	if w.Cores != 4 {
		t.Fatalf("expected 4 cores, got %d", w.Cores)
	}
}
