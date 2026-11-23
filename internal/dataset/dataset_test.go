package dataset_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"api-load-testing-engine/internal/dataset"
	"api-load-testing-engine/internal/metrics"
	"api-load-testing-engine/internal/protocol"
	"api-load-testing-engine/internal/worker/script"
)

func TestStreamingCSV_Iteration(t *testing.T) {
	csvData := "username,password,role\nalice,secret1,admin\nbob,secret2,user\ncharlie,secret3,viewer\n"
	ds, err := dataset.NewStreamingCSV(strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("NewStreamingCSV failed: %v", err)
	}

	headers := ds.Headers()
	if len(headers) != 3 || headers[0] != "username" || headers[1] != "password" || headers[2] != "role" {
		t.Fatalf("unexpected headers: %v", headers)
	}

	// Read row 1
	r1, err := ds.Next()
	if err != nil || r1["username"] != "alice" || r1["role"] != "admin" {
		t.Fatalf("row 1 unexpected: %v, err=%v", r1, err)
	}

	// Read row 2
	r2, err := ds.Next()
	if err != nil || r2["username"] != "bob" || r2["role"] != "user" {
		t.Fatalf("row 2 unexpected: %v, err=%v", r2, err)
	}

	// Read row 3
	r3, err := ds.Next()
	if err != nil || r3["username"] != "charlie" || r3["role"] != "viewer" {
		t.Fatalf("row 3 unexpected: %v, err=%v", r3, err)
	}

	// Read EOF
	_, err = ds.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF on 4th read, got %v", err)
	}
}

func TestDataset_GojaJSRuntimeIntegration(t *testing.T) {
	csvData := "username,token\nuser_1,tok_abc\nuser_2,tok_def\n"
	ds, err := dataset.NewStreamingCSV(strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("NewStreamingCSV failed: %v", err)
	}

	scriptSrc := `
let user = dataset.next();
if (!user || !user.username) {
    throw new Error('missing user from dataset');
}
check(user.username.startsWith('user_'), 'valid user');
`

	agg := metrics.NewLocalAggregator("test-run-ds")
	client := protocol.NewProtocolClient()
	rt, err := script.NewRuntime(scriptSrc, "http://localhost", agg, client)
	if err != nil {
		t.Fatalf("NewRuntime error: %v", err)
	}
	rt.SetDataset(ds)

	// Execute iteration 1
	vm1 := rt.NewVMInstance()
	ctx := context.Background()
	if err := vm1.ExecuteIteration(ctx, nil, time.Time{}); err != nil {
		t.Fatalf("iteration 1 failed: %v", err)
	}

	// Execute iteration 2
	vm2 := rt.NewVMInstance()
	if err := vm2.ExecuteIteration(ctx, nil, time.Time{}); err != nil {
		t.Fatalf("iteration 2 failed: %v", err)
	}

	// Iteration 3 reaches EOF -> dataset.next() returns null -> script throws
	vm3 := rt.NewVMInstance()
	err3 := vm3.ExecuteIteration(ctx, nil, time.Time{})
	if err3 == nil {
		t.Fatalf("expected iteration 3 to throw due to dataset exhaustion")
	}
}
