package script

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"api-load-testing-engine/internal/metrics"
	"api-load-testing-engine/internal/protocol"
	"github.com/dop251/goja"
)

// Response represents the HTTP response exposed to the JS runtime.
type Response struct {
	Status        int                    `json:"status"`
	Body          string                 `json:"body"`
	Headers       map[string]string      `json:"headers"`
	BytesSent     int64                  `json:"bytes_sent"`
	BytesReceived int64                  `json:"bytes_received"`
	Timings       map[string]float64     `json:"timings"`
	rawJSON       map[string]interface{}
	jsonParsed    bool
}

// Runtime compiles and executes JS test scripts for a VU.
type Runtime struct {
	baseURL    string
	program    *goja.Program
	aggregator *metrics.LocalAggregator
	client     *protocol.ProtocolClient
	mu         sync.Mutex
}

// NewRuntime compiles the user script and initializes the runtime factory.
func NewRuntime(scriptSource, baseURL string, agg *metrics.LocalAggregator, client *protocol.ProtocolClient) (*Runtime, error) {
	if client == nil {
		client = protocol.NewProtocolClient()
	}

	program, err := goja.Compile("script.js", scriptSource, true)
	if err != nil {
		return nil, fmt.Errorf("compile script error: %w", err)
	}

	return &Runtime{
		baseURL:    strings.TrimRight(baseURL, "/"),
		program:    program,
		aggregator: agg,
		client:     client,
	}, nil
}

// VMInstance wraps an isolated goja VM for a single VU or worker thread.
type VMInstance struct {
	vm              *goja.Runtime
	parent          *Runtime
	intendedArrival time.Time
	firstRequestDone bool
	ctx             context.Context
}

// NewVMInstance creates an isolated VM for a single VU.
func (r *Runtime) NewVMInstance() *VMInstance {
	inst := &VMInstance{
		vm:     goja.New(),
		parent: r,
	}
	inst.setupEnvironment()
	return inst
}

func (inst *VMInstance) setupEnvironment() {
	vm := inst.vm

	// http module
	httpObj := vm.NewObject()

	_ = httpObj.Set("get", func(call goja.FunctionCall) goja.Value {
		rawURL := call.Argument(0).String()
		var opts map[string]interface{}
		if len(call.Arguments) > 1 {
			opts, _ = call.Argument(1).Export().(map[string]interface{})
		}
		return inst.executeRequest("GET", rawURL, nil, opts)
	})

	_ = httpObj.Set("post", func(call goja.FunctionCall) goja.Value {
		rawURL := call.Argument(0).String()
		var bodyVal interface{}
		if len(call.Arguments) > 1 {
			bodyVal = call.Argument(1).Export()
		}
		var opts map[string]interface{}
		if len(call.Arguments) > 2 {
			opts, _ = call.Argument(2).Export().(map[string]interface{})
		}
		return inst.executeRequest("POST", rawURL, bodyVal, opts)
	})

	_ = httpObj.Set("put", func(call goja.FunctionCall) goja.Value {
		rawURL := call.Argument(0).String()
		var bodyVal interface{}
		if len(call.Arguments) > 1 {
			bodyVal = call.Argument(1).Export()
		}
		var opts map[string]interface{}
		if len(call.Arguments) > 2 {
			opts, _ = call.Argument(2).Export().(map[string]interface{})
		}
		return inst.executeRequest("PUT", rawURL, bodyVal, opts)
	})

	_ = httpObj.Set("del", func(call goja.FunctionCall) goja.Value {
		rawURL := call.Argument(0).String()
		var opts map[string]interface{}
		if len(call.Arguments) > 1 {
			opts, _ = call.Argument(1).Export().(map[string]interface{})
		}
		return inst.executeRequest("DELETE", rawURL, nil, opts)
	})

	_ = vm.Set("http", httpObj)

	// check(res, { 'assertion': r => ... })
	_ = vm.Set("check", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) < 2 {
			return vm.ToValue(false)
		}
		resVal := call.Argument(0)
		checksObj, ok := call.Argument(1).Export().(map[string]interface{})
		if !ok {
			return vm.ToValue(false)
		}

		allPassed := true
		for name, fnVal := range checksObj {
			callable, ok := goja.AssertFunction(vm.ToValue(fnVal))
			if !ok {
				continue
			}
			res, err := callable(goja.Undefined(), resVal)
			passed := err == nil && res.ToBoolean()
			if !passed {
				allPassed = false
				if inst.parent.aggregator != nil {
					inst.parent.aggregator.AddAssertionFailure()
				}
				_ = name // Available for tracing
			}
		}
		return vm.ToValue(allPassed)
	})

	// sleep(ms or '1s')
	_ = vm.Set("sleep", func(call goja.FunctionCall) goja.Value {
		arg := call.Argument(0).Export()
		var dur time.Duration
		switch v := arg.(type) {
		case int64:
			dur = time.Duration(v) * time.Millisecond
		case float64:
			dur = time.Duration(v) * time.Millisecond
		case string:
			parsed, err := time.ParseDuration(v)
			if err == nil {
				dur = parsed
			}
		}
		if dur > 0 {
			time.Sleep(dur)
		}
		return goja.Undefined()
	})
}

func (inst *VMInstance) resolveURL(raw string) string {
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	if inst.parent.baseURL == "" {
		return raw
	}
	if !strings.HasPrefix(raw, "/") {
		raw = "/" + raw
	}
	return inst.parent.baseURL + raw
}

func (inst *VMInstance) executeRequest(method, rawURL string, bodyVal interface{}, opts map[string]interface{}) goja.Value {
	fullURL := inst.resolveURL(rawURL)

	headers := make(map[string]string)
	if opts != nil {
		if h, ok := opts["headers"].(map[string]interface{}); ok {
			for k, v := range h {
				headers[k] = fmt.Sprintf("%v", v)
			}
		}
	}

	var bodyBytes []byte
	if bodyVal != nil {
		switch v := bodyVal.(type) {
		case string:
			bodyBytes = []byte(v)
		case []byte:
			bodyBytes = v
		default:
			// Object or map -> marshal JSON
			b, err := json.Marshal(v)
			if err == nil {
				bodyBytes = b
				if _, ok := headers["Content-Type"]; !ok {
					headers["Content-Type"] = "application/json"
				}
			}
		}
	}

	ctx := inst.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	dispatchTime := time.Now()
	timing := inst.parent.client.Do(ctx, method, fullURL, headers, bodyBytes)

	// Coordinated Omission: if this is the first request of the iteration, charge delay from intended arrival
	recordedDuration := timing.TotalDuration
	if !inst.firstRequestDone && !inst.intendedArrival.IsZero() {
		if dispatchTime.After(inst.intendedArrival) {
			lag := dispatchTime.Sub(inst.intendedArrival)
			recordedDuration += lag
		}
		inst.firstRequestDone = true
	}

	// Record metrics into aggregator
	if inst.parent.aggregator != nil {
		inst.parent.aggregator.AddRequests(1)
		inst.parent.aggregator.AddBytesSent(uint64(timing.BytesSent))
		inst.parent.aggregator.AddBytesReceived(uint64(timing.BytesReceived))

		endpointTag := "root"
		if u, err := url.Parse(fullURL); err == nil && u.Path != "" {
			endpointTag = u.Path
		}
		metricName := fmt.Sprintf("http_req_duration{endpoint=%s}", endpointTag)
		inst.parent.aggregator.RecordDuration(metricName, recordedDuration)
		inst.parent.aggregator.RecordDuration("http_req_duration", recordedDuration)

		if timing.Error != nil {
			inst.parent.aggregator.AddTimeout()
		} else if timing.StatusCode >= 400 && timing.StatusCode < 500 {
			inst.parent.aggregator.AddErrors4xx(1)
		} else if timing.StatusCode >= 500 {
			inst.parent.aggregator.AddErrors5xx(1)
		}
	}

	// Build JS response object
	respVal := inst.buildJSResponse(timing)
	return respVal
}

func (inst *VMInstance) buildJSResponse(t protocol.HTTPTiming) goja.Value {
	vm := inst.vm
	resObj := vm.NewObject()

	_ = resObj.Set("status", t.StatusCode)
	_ = resObj.Set("body", string(t.Body))

	hdrObj := vm.NewObject()
	for k, v := range t.Headers {
		if len(v) > 0 {
			_ = hdrObj.Set(k, v[0])
		}
	}
	_ = resObj.Set("headers", hdrObj)

	// json method: res.json([key])
	var parsedJSON interface{}
	var parsedOnce bool
	_ = resObj.Set("json", func(call goja.FunctionCall) goja.Value {
		if !parsedOnce {
			_ = json.Unmarshal(t.Body, &parsedJSON)
			parsedOnce = true
		}
		if parsedJSON == nil {
			return goja.Null()
		}

		if len(call.Arguments) == 0 {
			return vm.ToValue(parsedJSON)
		}

		key := call.Argument(0).String()
		if m, ok := parsedJSON.(map[string]interface{}); ok {
			if val, exists := m[key]; exists {
				return vm.ToValue(val)
			}
		}
		return goja.Undefined()
	})

	timingsObj := vm.NewObject()
	_ = timingsObj.Set("dns", float64(t.DNS.Microseconds())/1000.0)
	_ = timingsObj.Set("connect", float64(t.TCPConnect.Microseconds())/1000.0)
	_ = timingsObj.Set("tls", float64(t.TLSHandshake.Microseconds())/1000.0)
	_ = timingsObj.Set("ttfb", float64(t.TTFB.Microseconds())/1000.0)
	_ = timingsObj.Set("total", float64(t.TotalDuration.Microseconds())/1000.0)
	_ = resObj.Set("timings", timingsObj)

	return resObj
}

// ExecuteIteration runs a single iteration with the given iteration data.
func (inst *VMInstance) ExecuteIteration(ctx context.Context, data map[string]interface{}, intendedArrival time.Time) error {
	inst.ctx = ctx
	inst.intendedArrival = intendedArrival
	inst.firstRequestDone = false

	if data == nil {
		data = make(map[string]interface{})
	}
	_ = inst.vm.Set("data", data)

	_, err := inst.vm.RunProgram(inst.parent.program)
	return err
}
