CREATE SCHEMA IF NOT EXISTS `obs`;

CREATE TABLE IF NOT EXISTS `obs.run_log` (
  run_id STRING,
  job STRING,              -- ex.: ingest_susep | ingest_fipe | build_gold
  status STRING,           -- success | error
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  error_message STRING
) PARTITION BY DATE(started_at);