CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS tests (
  id          uuid PRIMARY KEY,
  name        text NOT NULL,
  version     int  NOT NULL DEFAULT 1,
  definition  jsonb NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS datasets (
  id          uuid PRIMARY KEY,
  name        text NOT NULL,
  object_key  text NOT NULL,
  row_count   bigint,
  created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runs (
  id              uuid PRIMARY KEY,
  test_id         uuid NOT NULL REFERENCES tests(id),
  definition      jsonb NOT NULL,
  status          text NOT NULL,
  current_stage   int,
  degraded        boolean NOT NULL DEFAULT false,
  exit_code       int,
  failure_reason  text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  started_at      timestamptz,
  ended_at        timestamptz
);

CREATE TABLE IF NOT EXISTS workers (
  id               uuid PRIMARY KEY,
  hostname         text,
  cores            int,
  mem_mb           int,
  bandwidth_mbps   int,
  status           text NOT NULL,
  last_heartbeat   timestamptz,
  registered_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS run_workers (
  run_id        uuid REFERENCES runs(id),
  worker_id     uuid REFERENCES workers(id),
  vus_share     int,
  rps_share     numeric,
  dataset_range int8range,
  state         text,
  PRIMARY KEY (run_id, worker_id)
);

CREATE TABLE IF NOT EXISTS threshold_results (
  run_id      uuid REFERENCES runs(id),
  expression  text NOT NULL,
  scope       text NOT NULL DEFAULT 'global',
  observed    double precision,
  passed      boolean NOT NULL,
  PRIMARY KEY (run_id, expression, scope)
);

CREATE TABLE IF NOT EXISTS failure_traces (
  id          bigserial PRIMARY KEY,
  run_id      uuid REFERENCES runs(id),
  worker_id   uuid REFERENCES workers(id),
  ts          timestamptz NOT NULL,
  error_type  text,
  object_key  text NOT NULL
);

-- Hypertables
CREATE TABLE IF NOT EXISTS metric_rollups (
  ts          timestamptz NOT NULL,
  run_id      uuid NOT NULL,
  metric      text NOT NULL,
  count       bigint,
  sum         double precision,
  min         double precision,
  max         double precision,
  p50         double precision,
  p90         double precision,
  p95         double precision,
  p99         double precision,
  p999        double precision,
  histogram   bytea,
  PRIMARY KEY (run_id, metric, ts)
);
SELECT create_hypertable('metric_rollups', 'ts', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS run_counters (
  ts                 timestamptz NOT NULL,
  run_id             uuid NOT NULL,
  requests           bigint,
  errors_4xx         bigint,
  errors_5xx         bigint,
  timeouts           bigint,
  conn_resets        bigint,
  assertion_failures bigint,
  bytes_sent         bigint,
  bytes_received     bigint,
  active_vus         int,
  dropped_iterations bigint,
  PRIMARY KEY (run_id, ts)
);
SELECT create_hypertable('run_counters', 'ts', if_not_exists => TRUE);
