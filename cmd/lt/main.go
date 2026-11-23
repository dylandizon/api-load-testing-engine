package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"api-load-testing-engine/internal/controller"
	"api-load-testing-engine/internal/worker"
	pb "api-load-testing-engine/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	subcommand := os.Args[1]

	switch subcommand {
	case "controller":
		runController(os.Args[2:])
	case "worker":
		runWorker(os.Args[2:])
	case "run":
		// Reserved for Milestone 5 CLI client
		fmt.Println("CLI 'run' client will be implemented in Milestone 5")
		os.Exit(0)
	default:
		fmt.Printf("Unknown subcommand: %s\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: lt <command> [flags]")
	fmt.Println("Commands:")
	fmt.Println("  controller  Run the controller service")
	fmt.Println("  worker      Run a load generator worker")
	fmt.Println("  run         Execute a load test script from CLI")
}

func runController(args []string) {
	fs := flag.NewFlagSet("controller", flag.ExitOnError)
	port := fs.Int("port", 50051, "gRPC port to listen on")
	_ = fs.Parse(args)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen on port %d: %v", *port, err)
	}

	reg := controller.NewRegistry()
	hooks := controller.ServerHooks{
		OnRegister: func(workerID string, reg *pb.Register) {
			log.Printf("[main] worker connected: %s (cores: %d, mem: %dMB)", workerID, reg.Cores, reg.MemMb)
		},
		OnHeartbeat: func(workerID string, hb *pb.Heartbeat) {
			// Heartbeat received
		},
	}

	coordServer := controller.NewCoordinatorServer(reg, hooks)
	grpcServer := grpc.NewServer()
	pb.RegisterCoordinatorServer(grpcServer, coordServer)

	// Lease checker goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				suspects, dead := reg.CheckLeases(5*time.Second, 15*time.Second)
				if len(suspects) > 0 {
					log.Printf("[lease-monitor] suspect workers: %v", suspects)
				}
				if len(dead) > 0 {
					log.Printf("[lease-monitor] dead workers: %v", dead)
				}
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down controller...")
		cancel()
		grpcServer.GracefulStop()
	}()

	log.Printf("Starting lt controller on gRPC port %d...", *port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("controller serve error: %v", err)
	}
}

func runWorker(args []string) {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	controllerAddr := fs.String("controller", "localhost:50051", "Controller gRPC address")
	workerID := fs.String("id", "", "Worker unique ID (auto-generated if empty)")
	cores := fs.Int("cores", runtime.NumCPU(), "Allocated CPU cores")
	memMB := fs.Int("mem", 4096, "Allocated memory in MB")
	target := fs.String("target", "", "Target URL for ping test")
	requests := fs.Int("requests", 10, "Number of ping requests to run")
	_ = fs.Parse(args)

	if *workerID == "" {
		*workerID = fmt.Sprintf("worker-%s", uuid.New().String()[:8])
	}

	runner := worker.NewSimpleRunner(&http.Client{Timeout: 5 * time.Second})
	cfg := worker.ClientConfig{
		WorkerID:          *workerID,
		ControllerAddr:    *controllerAddr,
		Cores:             int32(*cores),
		MemMB:             int32(*memMB),
		BandwidthMBPS:     1000,
		HeartbeatInterval: 1 * time.Second,
	}

	client := worker.NewClient(cfg, runner)
	runner.SetClient(client)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Stopping worker...")
		client.Stop()
		cancel()
	}()

	// If a target is specified in command line, queue a local run after connection
	if *target != "" {
		go func() {
			time.Sleep(1 * time.Second)
			log.Printf("Direct ping mode enabled for target %s with %d requests", *target, *requests)
			_ = runner.OnAssign(&pb.Assign{
				RunId: "direct-ping",
				ConfigSlice: []byte(fmt.Sprintf(`{"target":"%s","requests":%d}`, *target, *requests)),
			})
			_ = runner.OnStart(&pb.Start{RunId: "direct-ping"})
		}()
	}

	log.Printf("Starting lt worker %s, connecting to %s...", *workerID, *controllerAddr)
	if err := client.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("worker error: %v", err)
	}
}
