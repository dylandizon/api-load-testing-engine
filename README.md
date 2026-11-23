# Distributed API Load Testing Engine (`lt`)

[![Go Version](https://img.shields.io/badge/go-1.22%2B-blue.svg)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A high-throughput, low-overhead distributed load testing and performance benchmarking engine written in Go. A central controller coordinates a fleet of load generation workers over bidirectional gRPC streams, executing multi-stage load profiles with two-phase synchronization barriers, lossless 1-second metric rollups, automated threshold evaluation, and TimescaleDB/Grafana persistence.

---

## Key Features

- **Distributed Two-Phase Synchronization**: Workers pre-warm (compiling JS scripts and connection pools) and signal `Ready`. The controller calculates an absolute future timestamp $T = \text{now} + \Delta$ and broadcasts `Start(T)`, ensuring all workers fire load at the exact same millisecond.
- **Synchronized Stage Transitions**: Multi-stage profiles (ramp-up, constant hold, ramp-down) transition lock-step using future absolute timestamps (`StageChange`), eliminating network jitter desynchronization.
- **Dual Execution Models**:
  - **Closed Model**: Concurrency-limited Virtual Users (VUs) with think-time loops and dynamic scaling.
  - **Open Model**: Arrival-rate driven ($\lambda$ arrivals/sec) with bounded VU worker pools. Compensates for **Coordinated Omission** by scheduling on a fixed timeline and charging lag against intended start time.
- **Embedded JavaScript Runtime**: Powered by [Goja](https://github.com/dop251/goja), supporting `http.get`, `http.post`, `check()`, `sleep()`, and streaming `dataset.next()`.
- **Lossless Metrics Pipeline**: 1-second per-worker [HdrHistogram](https://github.com/HdrHistogram/hdrhistogram-go) snapshots ($1\mu\text{s}$ to $1\text{h}$, 3 sig figs) merged losslessly at the controller with sequence deduplication.
- **Automated CI Quality Gates & Abort Engine**: Threshold expressions (`p95 < 200`, `rate < 0.05`) evaluated against cumulative metrics. Breaching an `abort_on_fail` threshold instantly broadcasts `Abort` to all workers and returns deterministic exit codes (`0` = pass, `1` = threshold fail/aborted, `2` = engine error).
- **Production Resilience & Fault Tolerance**:
  - **Worker Lease Expiration**: Detects dead workers mid-run and flags the run as `DEGRADED = true` while surviving workers complete the test.
  - **Generator Saturation Tracking**: Flags runs as `DEGRADED` if worker CPU > 85% or scheduler lag > 50ms to prevent false-positive test results.
  - **Controller Crash Recovery**: Uncompleted runs persist in Postgres/store and are rehydrated on controller restart. Workers maintain a 30-second local buffer of metrics, reconnect with `resume_run_id`, and flush losslessly.
  - **Failure Trace Sampling**: Captures up to 10 failed request/response traces per second, uploading them asynchronously to S3/MinIO object storage.
  - **Streaming Datasets**: Stream-reads multi-gigabyte CSV datasets line-by-line consuming $O(1)$ memory.

---

## Architecture Overview

```
                      +--------------------------------------+
                      |             CLI / CI/CD              |
                      |   (lt run examples/checkout.yaml)    |
                      +--------------------------------------+
                                         |  HTTP REST API
                                         v
                      +--------------------------------------+
                      |              Controller              |
                      |  - Partitioner & Barrier Coordinator |
                      |  - Stage Manager (Absolute T Clocks) |
                      |  - Lossless HdrHistogram Merger      |
                      |  - Live Threshold Abort Engine       |
                      +--------------------------------------+
                              /          |          \
                             / gRPC      | gRPC      \ gRPC
                            v            v            v
                      +----------+ +----------+ +----------+
                      | Worker 1 | | Worker 2 | | Worker 3 |
                      | (Goja JS | | (Goja JS | | (Goja JS |
                      | Runtime) | | Runtime) | | Runtime) |
                      +----------+ +----------+ +----------+
                            \            |            /
                             \           |           / HTTP/1.1 & HTTP/2
                              v          v          v
                      +--------------------------------------+
                      |              Target API              |
                      +--------------------------------------+
```

---

## How to Run Locally

You can run the engine locally in two modes:

### Mode 1: Zero-Dependency Mode (Fastest)

Runs with the built-in in-memory metrics store and mock endpoint. No Docker, database, or external tools required.

Open **3 separate terminal windows** in the project directory:

#### **Terminal 1: Start the Controller**
```powershell
go run -mod=vendor ./cmd/lt controller --port 50051 --http-port 8080
```
- **Port 50051**: gRPC streaming interface for workers.
- **Port 8080**: HTTP REST API for test management, live polling, and CLI execution.

#### **Terminal 2: Start Worker(s)**
Start 1, 2, or more worker nodes:
```powershell
# Worker 1
go run -mod=vendor ./cmd/lt worker --controller localhost:50051 --id worker-1

# Worker 2 (in another terminal, optional)
go run -mod=vendor ./cmd/lt worker --controller localhost:50051 --id worker-2
```
Workers automatically register their CPU core count and memory allocation with the controller.

#### **Terminal 3: Execute a Test Run from CLI**
Run the included benchmark scenario:
```powershell
go run -mod=vendor ./cmd/lt run examples/checkout.yaml --controller http://localhost:8080
```

#### What you will see:
1. `lt run` uploads the test definition to the controller.
2. The controller partitions VUs/RPS proportionally across connected workers.
3. Workers pre-warm and coordinate on the start barrier.
4. Terminal streams real-time execution progress:
   ```
   [00:00:02] Status: RUNNING      | Stage: 0
   ```
5. Upon completion, it renders the threshold validation table and exits with deterministic exit codes:
   ```
   =================== TEST RESULT ===================
   Run ID:   run-1a62e129
   Status:   COMPLETED
   ----------------- Threshold Check -----------------
   [PASS] http_req_duration    | PASS (observed: 3.153 < target: 500.000)
   [PASS] error_rate           | PASS (observed: 0.000 < target: 0.050)
   ---------------------------------------------------
   Overall Outcome: Passed=true | Exit Code=0
   ===================================================
   ```
   Check the exit code:
   ```powershell
   echo $LASTEXITCODE # 0 = Pass, 1 = Threshold Fail/Aborted, 2 = Engine Error
   ```

---

### Mode 2: Full Production Stack (TimescaleDB + Grafana + MinIO)

If you have Docker installed and want persistent time-series metrics and live Grafana dashboards:

1. **Start the Infrastructure**:
   ```powershell
   docker compose -f deploy/docker-compose.yml up -d
   ```
   This spins up:
   - **TimescaleDB** on `localhost:5432` (`postgres:password`, database: `loadtesting`)
   - **Grafana** on `http://localhost:3000` (`admin:admin`) with pre-provisioned live dashboards
   - **MinIO S3** on `http://localhost:9000` (Console: `http://localhost:9001`)

2. **Start the Controller with the DB connection (Terminal 1)**:
   ```powershell
   go run -mod=vendor ./cmd/lt controller --port 50051 --http-port 8080 --db "postgres://postgres:password@localhost:5432/loadtesting?sslmode=disable"
   ```

3. **Start Worker(s) (Terminal 2 & optional Terminal 3)**:
   ```powershell
   go run -mod=vendor ./cmd/lt worker --controller localhost:50051 --id worker-1
   ```

4. **Execute a Test Run from CLI**:
   ```powershell
   go run -mod=vendor ./cmd/lt run examples/checkout.yaml
   ```

5. **View Live Dashboards in Grafana**:
   Open **[http://localhost:3000/d/load-testing-engine](http://localhost:3000/d/load-testing-engine)** in your browser:
   - **Login**: `admin` / `admin` (click *Skip* on first password change prompt).
   - **Live View**: Set the top-right time range to **"Last 5 minutes"** and auto-refresh to **"1s"** or **"5s"** to watch:
     - **Throughput (Requests / sec)**: Multi-worker aggregated request rate.
     - **Latency Percentiles (ms)**: Real-time p50, p90, p95, and p99 curves.
     - **Errors & Failures**: 4xx, 5xx, timeouts, and assertion failures.
     - **Active VUs & Dropped Iterations**: Concurrency and generator saturation.

---

## Writing Test Scenarios

Test scenarios are defined in clean YAML files. Example ([examples/checkout.yaml](examples/checkout.yaml)):

```yaml
name: checkout-load-test
version: 1
base_url: http://localhost:8080/mock
protocol: http1

scenarios:
  - name: checkout
    executor: closed           # "closed" (concurrency VUs) or "open" (arrival rate RPS)
    stages:
      - duration: 3s
        target: 10             # 10 VUs for 3 seconds
    think_time: 50ms           # Sleep between VU iterations
    script: |
      let res = http.get('/health');
      check(res.status == 200, 'status is 200');

thresholds:
  - metric: http_req_duration
    expr: p95 < 500            # p95 latency must stay below 500ms
    abort_on_fail: true        # Fast-fail: abort test immediately if violated
  - metric: error_rate
    expr: rate < 0.05          # Error rate must stay below 5%
```

### Supported JavaScript APIs
- `http.get(url, [options])`, `http.post(url, body, [options])`, `http.put(...)`, `http.del(...)`
- `res.status`, `res.body`, `res.json()`, `res.headers`, `res.timings`
- `check(condition, description)`
- `sleep(duration)` (e.g. `sleep('100ms')` or `sleep(500)`)
- `dataset.next()`: Streams the next row from a bound CSV dataset

---

## CLI Reference

```
Usage: lt <command> [flags]

Commands:
  controller  Run the controller service
    Flags:
      --port        gRPC port for worker connections (default: 50051)
      --http-port   HTTP REST API port (default: 8080)
      --db          Postgres/TimescaleDB connection string (in-memory if empty)

  worker      Run a load generator worker node
    Flags:
      --controller  Controller gRPC host:port (default: "localhost:50051")
      --id          Worker unique ID (auto-generated if empty)
      --cores       CPU cores allocated (default: system CPU count)
      --mem         Memory in MB allocated (default: 4096)

  run         Execute a load test script from CLI
    Flags:
      --controller  Controller HTTP REST API URL (default: "http://localhost:8080")
    Arguments:
      <test.yaml>   Path to the test definition file
```

---

## Building the Single Binary

To build a standalone executable:
```powershell
go build -mod=vendor -o lt.exe ./cmd/lt
```

Then execute directly:
```powershell
.\lt.exe controller
.\lt.exe worker
.\lt.exe run examples/checkout.yaml
```

---

## Running the Automated Test Suite

Run all unit tests, concurrency benchmarks, and integration tests:
```powershell
go test -v -mod=vendor ./...
```
