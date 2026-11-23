package controller

import (
	"fmt"
	"strconv"
	"strings"

	"api-load-testing-engine/internal/metrics"
	"github.com/HdrHistogram/hdrhistogram-go"
)

// ParsedThreshold represents a parsed threshold rule.
type ParsedThreshold struct {
	Metric      string
	RawExpr     string
	Field       string
	Operator    string
	TargetVal   float64
	AbortOnFail bool
}

// ThresholdResult represents the outcome of evaluating a threshold.
type ThresholdResult struct {
	Metric      string  `json:"metric"`
	Expression  string  `json:"expression"`
	Observed    float64 `json:"observed"`
	Target      float64 `json:"target"`
	Passed      bool    `json:"passed"`
	AbortOnFail bool    `json:"abort_on_fail"`
	Detail      string  `json:"detail"`
}

// ParseThreshold parses an expression like "p95 < 200" or "rate < 0.005".
func ParseThreshold(def ThresholdDef) (*ParsedThreshold, error) {
	expr := strings.TrimSpace(def.Expr)
	var op string
	var opIdx int

	// Detect operator
	operators := []string{"<=", ">=", "==", "!=", "<", ">"}
	for _, o := range operators {
		if idx := strings.Index(expr, o); idx != -1 {
			op = o
			opIdx = idx
			break
		}
	}

	if op == "" {
		return nil, fmt.Errorf("invalid threshold expression '%s': no supported operator found", expr)
	}

	field := strings.ToLower(strings.TrimSpace(expr[:opIdx]))
	valStr := strings.TrimSpace(expr[opIdx+len(op):])
	targetVal, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid target value '%s' in threshold '%s': %w", valStr, expr, err)
	}

	return &ParsedThreshold{
		Metric:      def.Metric,
		RawExpr:     def.Expr,
		Field:       field,
		Operator:    op,
		TargetVal:   targetVal,
		AbortOnFail: def.AbortOnFail,
	}, nil
}

// EvaluateThreshold checks whether metrics satisfy the threshold rule.
func EvaluateThreshold(t *ParsedThreshold, hist *hdrhistogram.Histogram, counters *metrics.RunCountersRecord) ThresholdResult {
	res := ThresholdResult{
		Metric:      t.Metric,
		Expression:  t.RawExpr,
		Target:      t.TargetVal,
		AbortOnFail: t.AbortOnFail,
	}

	var observed float64
	var validSample bool

	if t.Field == "rate" || t.Metric == "error_rate" {
		if counters != nil && counters.Requests > 0 {
			totalErrors := counters.Errors4xx + counters.Errors5xx + counters.Timeouts
			observed = float64(totalErrors) / float64(counters.Requests)
			validSample = true
		} else {
			// Not enough samples yet
			res.Passed = true
			res.Detail = "insufficient requests for rate evaluation"
			return res
		}
	} else if t.Field == "count" {
		if counters != nil {
			observed = float64(counters.Requests)
			validSample = true
		}
	} else {
		// Latency percentile fields
		if hist != nil && hist.TotalCount() > 0 {
			summary := metrics.ComputeSummary(hist)
			switch t.Field {
			case "p50":
				observed = summary.P50
			case "p90":
				observed = summary.P90
			case "p95":
				observed = summary.P95
			case "p99":
				observed = summary.P99
			case "p999":
				observed = summary.P999
			case "min":
				observed = summary.Min
			case "max":
				observed = summary.Max
			case "mean", "avg":
				observed = summary.Mean
			default:
				observed = summary.P95
			}
			validSample = true
		} else {
			res.Passed = true
			res.Detail = "insufficient samples for latency evaluation"
			return res
		}
	}

	if !validSample {
		res.Passed = true
		res.Detail = "no samples recorded"
		return res
	}

	res.Observed = observed
	passed := false
	switch t.Operator {
	case "<":
		passed = observed < t.TargetVal
	case "<=":
		passed = observed <= t.TargetVal
	case ">":
		passed = observed > t.TargetVal
	case ">=":
		passed = observed >= t.TargetVal
	case "==":
		passed = observed == t.TargetVal
	case "!=":
		passed = observed != t.TargetVal
	}

	res.Passed = passed
	if passed {
		res.Detail = fmt.Sprintf("PASS (observed: %.3f %s target: %.3f)", observed, t.Operator, t.TargetVal)
	} else {
		res.Detail = fmt.Sprintf("FAIL (observed: %.3f breached target: %.3f)", observed, t.TargetVal)
	}

	return res
}
