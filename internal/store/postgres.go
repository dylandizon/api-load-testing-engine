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

// Close closes the underlying database pool.
func (s *PostgresStore) Close() error {
	return s.db.Close()
}
