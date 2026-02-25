-- Idempotent schema migration for new telemetry/alerting features

ALTER TABLE IF EXISTS anomaly_sessions
    ADD COLUMN IF NOT EXISTS event_id VARCHAR(64),
    ADD COLUMN IF NOT EXISTS event_ts BIGINT,
    ADD COLUMN IF NOT EXISTS kafka_ingest_ts TIMESTAMP,
    ADD COLUMN IF NOT EXISTS spark_process_ts TIMESTAMP,
    ADD COLUMN IF NOT EXISTS db_write_ts TIMESTAMP,
    ADD COLUMN IF NOT EXISTS source VARCHAR(100);

CREATE INDEX IF NOT EXISTS idx_anomaly_event_id ON anomaly_sessions(event_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE tablename = 'anomaly_sessions'
          AND indexname = 'idx_anomaly_dedup_unique'
    ) THEN
        CREATE UNIQUE INDEX idx_anomaly_dedup_unique
        ON anomaly_sessions(session_id, window_start, anomaly_type, detected_at);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS alert_history (
    id SERIAL PRIMARY KEY,
    alert_hash VARCHAR(128) NOT NULL,
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('WARNING', 'CRITICAL')),
    topic_name VARCHAR(200) NOT NULL,
    partition_offsets JSONB,
    message TEXT,
    notified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    time_bucket TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_history_hash_bucket
ON alert_history(alert_hash, time_bucket);
