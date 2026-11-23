package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"api-load-testing-engine/internal/metrics"
	_ "github.com/lib/pq"
)

// PostgresStore implements Store against a PostgreSQL + TimescaleDB instance.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore establishes a connection pool to Postgres.
func NewPostgresStore(connStr string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	return &PostgresStore{db: db}, nil
}

// InitSchema executes the database creation statements.
func (s *PostgresStore) InitSchema(ctx context.Context, schemaSQL string) error {
	_, err := s.db.ExecContext(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("init schema error: %w", err)
	}
	return nil
}

// SaveRollup persists a merged metric rollup into metric_rollups hypertable.
func (s *PostgresStore) SaveRollup(r metrics.MetricRollup) error {
	query := `
		INSERT INTO metric_rollups (
			ts, run_id, metric, count, sum, min, max, p50, p90, p95, p99, p999, histogram
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (run_id, metric, ts) DO UPDATE SET
			count = EXCLUDED.count,
			sum = EXCLUDED.sum,
			min = EXCLUDED.min,
			max = EXCLUDED.max,
			p50 = EXCLUDED.p50,
			p90 = EXCLUDED.p90,
			p95 = EXCLUDED.p95,
			p99 = EXCLUDED.p99,
			p999 = EXCLUDED.p999,
			histogram = EXCLUDED.histogram;
	`
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := s.db.ExecContext(ctx, query,
		r.Timestamp, r.RunID, r.Metric, r.Count, r.Sum, r.Min, r.Max,
		r.P50, r.P90, r.P95, r.P99, r.P999, r.Histogram,
	)
	return err
}

// SaveCounters persists a merged run counter row into run_counters hypertable.
func (s *PostgresStore) SaveCounters(c metrics.RunCountersRecord) error {
	query := `
		INSERT INTO run_counters (
			ts, run_id, requests, errors_4xx, errors_5xx, timeouts, conn_resets,
			assertion_failures, bytes_sent, bytes_received, active_vus, dropped_iterations
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (run_id, ts) DO UPDATE SET
			requests = EXCLUDED.requests,
			errors_4xx = EXCLUDED.errors_4xx,
			errors_5xx = EXCLUDED.errors_5xx,
			timeouts = EXCLUDED.timeouts,
			conn_resets = EXCLUDED.conn_resets,
			assertion_failures = EXCLUDED.assertion_failures,
			bytes_sent = EXCLUDED.bytes_sent,
			bytes_received = EXCLUDED.bytes_received,
			active_vus = EXCLUDED.active_vus,
			dropped_iterations = EXCLUDED.dropped_iterations;
	`
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := s.db.ExecContext(ctx, query,
		c.Timestamp, c.RunID, c.Requests, c.Errors4xx, c.Errors5xx, c.Timeouts,
		c.ConnResets, c.AssertionFailures, c.BytesSent, c.BytesReceived,
		c.ActiveVUs, c.DroppedIterations,
	)
	return err
}

// GetRollups retrieves historical rollups for a run from TimescaleDB.
func (s *PostgresStore) GetRollups(ctx context.Context, runID string, metric string) ([]metrics.MetricRollup, error) {
	query := `
		SELECT ts, run_id, metric, count, sum, min, max, p50, p90, p95, p99, p999, histogram
		FROM metric_rollups
		WHERE run_id = $1 AND ($2 = '' OR metric = $2)
		ORDER BY ts ASC;
	`
	rows, err := s.db.QueryContext(ctx, query, runID, metric)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []metrics.MetricRollup
	for rows.Next() {
		var r metrics.MetricRollup
		err := rows.Scan(
			&r.Timestamp, &r.RunID, &r.Metric, &r.Count, &r.Sum, &r.Min, &r.Max,
			&r.P50, &r.P90, &r.P95, &r.P99, &r.P999, &r.Histogram,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// GetCounters retrieves all counter records for a run.
func (s *PostgresStore) GetCounters(ctx context.Context, runID string) ([]metrics.RunCountersRecord, error) {
	query := `
		SELECT ts, run_id, requests, errors_4xx, errors_5xx, timeouts, conn_resets,
		       assertion_failures, bytes_sent, bytes_received, active_vus, dropped_iterations
		FROM run_counters
		WHERE run_id = $1
		ORDER BY ts ASC;
	`
	rows, err := s.db.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []metrics.RunCountersRecord
	for rows.Next() {
		var c metrics.RunCountersRecord
		err := rows.Scan(
			&c.Timestamp, &c.RunID, &c.Requests, &c.Errors4xx, &c.Errors5xx, &c.Timeouts,
			&c.ConnResets, &c.AssertionFailures, &c.BytesSent, &c.BytesReceived,
			&c.ActiveVUs, &c.DroppedIterations,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

// SaveRun stores a run record in postgres.
func (s *PostgresStore) SaveRun(ctx context.Context, r RunRecord) error {
	query := `
		INSERT INTO runs (id, test_id, definition, status, current_stage, degraded, exit_code, failure_reason, created_at, started_at, ended_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			current_stage = EXCLUDED.current_stage,
			degraded = EXCLUDED.degraded,
			exit_code = EXCLUDED.exit_code,
			failure_reason = EXCLUDED.failure_reason,
			started_at = EXCLUDED.started_at,
			ended_at = EXCLUDED.ended_at;
	`
	defBytes := r.Definition
	if len(defBytes) == 0 {
		defBytes = []byte("{}")
	}
	_, err := s.db.ExecContext(ctx, query,
		r.ID, r.TestID, defBytes, r.Status, r.CurrentStage, r.Degraded,
		r.ExitCode, r.FailureReason, r.CreatedAt, r.StartedAt, r.EndedAt,
	)
	return err
}

// UpdateRun updates state and status of a run.
func (s *PostgresStore) UpdateRun(ctx context.Context, runID string, status string, currentStage int, degraded bool, exitCode int, failureReason string) error {
	query := `
		UPDATE runs
		SET status = $2, current_stage = $3, degraded = $4, exit_code = $5, failure_reason = $6,
		    ended_at = CASE WHEN $2 IN ('COMPLETED', 'ABORTED', 'FAILED') THEN now() ELSE ended_at END,
		    started_at = CASE WHEN $2 = 'RUNNING' AND started_at IS NULL THEN now() ELSE started_at END
		WHERE id = $1;
	`
	_, err := s.db.ExecContext(ctx, query, runID, status, currentStage, degraded, exitCode, failureReason)
	return err
}

// GetRun retrieves a run record by ID.
func (s *PostgresStore) GetRun(ctx context.Context, runID string) (*RunRecord, error) {
	query := `
		SELECT id, test_id, definition, status, current_stage, degraded, exit_code,
		       COALESCE(failure_reason, ''), created_at,
		       COALESCE(started_at, '0001-01-01 00:00:00Z'),
		       COALESCE(ended_at, '0001-01-01 00:00:00Z')
		FROM runs WHERE id = $1;
	`
	var r RunRecord
	err := s.db.QueryRowContext(ctx, query, runID).Scan(
		&r.ID, &r.TestID, &r.Definition, &r.Status, &r.CurrentStage, &r.Degraded,
		&r.ExitCode, &r.FailureReason, &r.CreatedAt, &r.StartedAt, &r.EndedAt,
	)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// GetActiveRuns retrieves runs in non-terminal states.
func (s *PostgresStore) GetActiveRuns(ctx context.Context) ([]*RunRecord, error) {
	query := `
		SELECT id, test_id, definition, status, current_stage, degraded, exit_code,
		       COALESCE(failure_reason, ''), created_at,
		       COALESCE(started_at, '0001-01-01 00:00:00Z'),
		       COALESCE(ended_at, '0001-01-01 00:00:00Z')
		FROM runs
		WHERE status IN ('PENDING', 'ASSIGNING', 'WAITING_READY', 'RUNNING', 'DRAINING')
		ORDER BY created_at ASC;
	`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*RunRecord
	for rows.Next() {
		var r RunRecord
		err := rows.Scan(
			&r.ID, &r.TestID, &r.Definition, &r.Status, &r.CurrentStage, &r.Degraded,
			&r.ExitCode, &r.FailureReason, &r.CreatedAt, &r.StartedAt, &r.EndedAt,
		)
		if err != nil {
			return nil, err
		}
		runs = append(runs, &r)
	}
	return runs, rows.Err()
}

// SaveFailureTrace stores a failure trace entry.
func (s *PostgresStore) SaveFailureTrace(ctx context.Context, trace FailureTraceRecord) error {
	query := `
		INSERT INTO failure_traces (run_id, worker_id, ts, error_type, object_key)
		VALUES ($1, $2, $3, $4, $5);
	`
	_, err := s.db.ExecContext(ctx, query, trace.RunID, trace.WorkerID, trace.Timestamp, trace.ErrorType, trace.ObjectKey)
	return err
}

// GetFailureTraces retrieves sampled traces for a run.
func (s *PostgresStore) GetFailureTraces(ctx context.Context, runID string) ([]FailureTraceRecord, error) {
	query := `
		SELECT id, run_id, worker_id, ts, error_type, object_key
		FROM failure_traces
		WHERE run_id = $1
		ORDER BY ts ASC;
	`
	rows, err := s.db.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var traces []FailureTraceRecord
	for rows.Next() {
		var t FailureTraceRecord
		err := rows.Scan(&t.ID, &t.RunID, &t.WorkerID, &t.Timestamp, &t.ErrorType, &t.ObjectKey)
		if err != nil {
			return nil, err
		}
		traces = append(traces, t)
	}
	return traces, rows.Err()
}

// Close closes the underlying database pool.
func (s *PostgresStore) Close() error {
	return s.db.Close()
}
