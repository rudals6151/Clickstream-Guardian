-- Clickstream Guardian Database Initialization Script

-- Extension for UUID generation (optional)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- REAL-TIME ANOMALY DETECTION TABLES
-- ============================================================================

-- Anomaly Sessions Table with Daily Partitioning
-- Using SERIAL id as PRIMARY KEY to allow multiple detections per session across different windows
-- Partitioned by detected_at date for efficient time-based queries and data management
CREATE TABLE IF NOT EXISTS anomaly_sessions (
    id BIGSERIAL NOT NULL,
    event_id VARCHAR(64),
    event_ts BIGINT,
    session_id BIGINT NOT NULL,
    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    click_count INTEGER NOT NULL CHECK (click_count > 0),
    unique_items INTEGER NOT NULL CHECK (unique_items >= 0),
    anomaly_score FLOAT NOT NULL CHECK (anomaly_score >= 0),
    anomaly_type VARCHAR(50) NOT NULL CHECK (anomaly_type IN ('HIGH_FREQUENCY', 'BOT_LIKE', 'SPAM', 'OTHER')),
    kafka_ingest_ts TIMESTAMP,
    spark_process_ts TIMESTAMP,
    db_write_ts TIMESTAMP,
    source VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, detected_at)
    -- Note: UNIQUE constraint removed due to partitioning requirements
    -- Duplicate prevention is handled at application level (Spark) via ON CONFLICT on window_start
) PARTITION BY RANGE (detected_at);

-- Default partition for dates outside defined ranges (required for initial setup)
CREATE TABLE IF NOT EXISTS anomaly_sessions_default PARTITION OF anomaly_sessions DEFAULT;

-- Partitions will be created dynamically using create_initial_partitions() function below

-- Indexes for anomaly_sessions (automatically created on all partitions)
CREATE INDEX IF NOT EXISTS idx_anomaly_session_id ON anomaly_sessions(session_id);
CREATE INDEX IF NOT EXISTS idx_anomaly_detected_at ON anomaly_sessions(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_type ON anomaly_sessions(anomaly_type);
CREATE INDEX IF NOT EXISTS idx_anomaly_score ON anomaly_sessions(anomaly_score DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_window ON anomaly_sessions(window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_anomaly_event_id ON anomaly_sessions(event_id);

-- Composite index for duplicate detection (replaces UNIQUE constraint)
-- This helps application-level duplicate prevention without enforcing database constraint
CREATE UNIQUE INDEX IF NOT EXISTS idx_anomaly_dedup_unique
ON anomaly_sessions(session_id, window_start, anomaly_type, detected_at);

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_anomaly_sessions_updated_at 
    BEFORE UPDATE ON anomaly_sessions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- BATCH ANALYTICS TABLES
-- ============================================================================

-- Daily Metrics Table
CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date DATE PRIMARY KEY,
    total_clicks BIGINT NOT NULL DEFAULT 0 CHECK (total_clicks >= 0),
    total_purchases BIGINT NOT NULL DEFAULT 0 CHECK (total_purchases >= 0),
    unique_sessions BIGINT NOT NULL DEFAULT 0 CHECK (unique_sessions >= 0),
    unique_items BIGINT NOT NULL DEFAULT 0 CHECK (unique_items >= 0),
    unique_users BIGINT DEFAULT 0,
    conversion_rate FLOAT CHECK (conversion_rate >= 0 AND conversion_rate <= 1),
    avg_session_duration_sec FLOAT CHECK (avg_session_duration_sec >= 0),
    avg_clicks_per_session FLOAT CHECK (avg_clicks_per_session >= 0),
    total_revenue NUMERIC(15,2) DEFAULT 0 CHECK (total_revenue >= 0),
    avg_order_value FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for daily_metrics
CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics(metric_date DESC);

CREATE TRIGGER update_daily_metrics_updated_at 
    BEFORE UPDATE ON daily_metrics
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Session Funnel Table
CREATE TABLE IF NOT EXISTS session_funnel (
    metric_date DATE NOT NULL,
    funnel_stage VARCHAR(50) NOT NULL CHECK (funnel_stage IN ('VIEW', 'MULTI_VIEW', 'ADD_TO_CART', 'PURCHASE')),
    session_count BIGINT NOT NULL DEFAULT 0 CHECK (session_count >= 0),
    percentage FLOAT CHECK (percentage >= 0 AND percentage <= 100),
    drop_rate FLOAT CHECK (drop_rate >= 0 AND drop_rate <= 1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_date, funnel_stage)
);

-- Index for session_funnel
CREATE INDEX IF NOT EXISTS idx_funnel_date ON session_funnel(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_funnel_stage ON session_funnel(funnel_stage);

-- Popular Items Table
CREATE TABLE IF NOT EXISTS popular_items (
    metric_date DATE NOT NULL,
    item_id BIGINT NOT NULL,
    category VARCHAR(100),
    click_count INTEGER NOT NULL DEFAULT 0 CHECK (click_count >= 0),
    purchase_count INTEGER NOT NULL DEFAULT 0 CHECK (purchase_count >= 0),
    revenue NUMERIC(15,2) NOT NULL DEFAULT 0 CHECK (revenue >= 0),
    rank INTEGER NOT NULL CHECK (rank > 0),
    click_to_purchase_ratio FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_date, item_id)
);

-- Indexes for popular_items
CREATE INDEX IF NOT EXISTS idx_popular_items_date ON popular_items(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_popular_items_rank ON popular_items(metric_date, rank);
CREATE INDEX IF NOT EXISTS idx_popular_items_category ON popular_items(category);
CREATE INDEX IF NOT EXISTS idx_popular_items_revenue ON popular_items(revenue DESC);

-- Popular Categories Table
CREATE TABLE IF NOT EXISTS popular_categories (
    metric_date DATE NOT NULL,
    category VARCHAR(100) NOT NULL,
    click_count INTEGER NOT NULL DEFAULT 0,
    purchase_count INTEGER NOT NULL DEFAULT 0,
    unique_items INTEGER NOT NULL DEFAULT 0,
    revenue NUMERIC(15,2) NOT NULL DEFAULT 0,
    rank INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (metric_date, category)
);

-- Index for popular_categories
CREATE INDEX IF NOT EXISTS idx_popular_categories_date ON popular_categories(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_popular_categories_rank ON popular_categories(metric_date, rank);

-- ============================================================================
-- SESSION ANALYTICS TABLES
-- ============================================================================

-- Session Summary Table (for detailed analysis)
CREATE TABLE IF NOT EXISTS session_summary (
    session_id BIGINT PRIMARY KEY,
    metric_date DATE NOT NULL,
    first_event_ts TIMESTAMP NOT NULL,
    last_event_ts TIMESTAMP NOT NULL,
    duration_sec INTEGER,
    click_count INTEGER NOT NULL DEFAULT 0,
    purchase_count INTEGER NOT NULL DEFAULT 0,
    unique_items_viewed INTEGER DEFAULT 0,
    unique_items_purchased INTEGER DEFAULT 0,
    total_spent BIGINT DEFAULT 0,
    is_converted BOOLEAN DEFAULT FALSE,
    is_anomaly BOOLEAN DEFAULT FALSE,
    categories_viewed TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for session_summary
CREATE INDEX IF NOT EXISTS idx_session_summary_date ON session_summary(metric_date DESC);
CREATE INDEX IF NOT EXISTS idx_session_summary_converted ON session_summary(is_converted);
CREATE INDEX IF NOT EXISTS idx_session_summary_anomaly ON session_summary(is_anomaly);

-- ============================================================================
-- DATA QUALITY AND MONITORING TABLES
-- ============================================================================

-- Data Quality Checks Table
CREATE TABLE IF NOT EXISTS data_quality_checks (
    id SERIAL PRIMARY KEY,
    check_date DATE NOT NULL,
    check_type VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    check_result VARCHAR(20) NOT NULL CHECK (check_result IN ('PASS', 'FAIL', 'WARNING')),
    expected_value FLOAT,
    actual_value FLOAT,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for data_quality_checks
CREATE INDEX IF NOT EXISTS idx_dq_checks_date ON data_quality_checks(check_date DESC);
CREATE INDEX IF NOT EXISTS idx_dq_checks_result ON data_quality_checks(check_result);

-- Pipeline Run History Table
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    run_date DATE NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED')),
    records_processed BIGINT DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for pipeline_runs
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name ON pipeline_runs(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_date ON pipeline_runs(run_date DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);

-- Alert History Table (for DLQ alert deduplication)
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
CREATE INDEX IF NOT EXISTS idx_alert_history_notified_at ON alert_history(notified_at DESC);

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View: Recent Anomalies (Last 24 Hours)
CREATE OR REPLACE VIEW v_recent_anomalies AS
SELECT 
    id,
    session_id,
    detected_at,
    window_start,
    window_end,
    click_count,
    unique_items,
    anomaly_type,
    anomaly_score,
    ROUND((EXTRACT(EPOCH FROM (window_end - window_start)) / 60.0)::numeric, 2) as window_duration_minutes
FROM anomaly_sessions
WHERE detected_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY detected_at DESC;

-- View: Daily Metrics Summary (Last 30 Days)
CREATE OR REPLACE VIEW v_daily_metrics_summary AS
SELECT 
    metric_date,
    total_clicks,
    total_purchases,
    unique_sessions,
    ROUND(conversion_rate::numeric * 100, 2) as conversion_rate_pct,
    ROUND(avg_clicks_per_session::numeric, 2) as avg_clicks_per_session,
    ROUND((avg_session_duration_sec / 60.0)::numeric, 2) as avg_session_duration_minutes,
    total_revenue,
    CASE 
        WHEN total_purchases > 0 
        THEN ROUND((total_revenue::numeric / total_purchases), 2)
        ELSE 0 
    END as avg_order_value
FROM daily_metrics
WHERE metric_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY metric_date DESC;

-- View: Top Items by Revenue (Last 7 Days)
CREATE OR REPLACE VIEW v_top_items_revenue AS
SELECT 
    item_id,
    category,
    SUM(click_count) as total_clicks,
    SUM(purchase_count) as total_purchases,
    SUM(revenue) as total_revenue,
    ROUND(AVG(rank)::numeric, 1) as avg_rank
FROM popular_items
WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY item_id, category
ORDER BY total_revenue DESC
LIMIT 50;

-- View: Conversion Funnel (Latest Date)
CREATE OR REPLACE VIEW v_latest_funnel AS
SELECT 
    funnel_stage,
    session_count,
    ROUND(percentage::numeric, 2) as percentage,
    ROUND(drop_rate::numeric * 100, 2) as drop_rate_pct
FROM session_funnel
WHERE metric_date = (SELECT MAX(metric_date) FROM session_funnel)
ORDER BY 
    CASE funnel_stage
        WHEN 'VIEW' THEN 1
        WHEN 'MULTI_VIEW' THEN 2
        WHEN 'ADD_TO_CART' THEN 3
        WHEN 'PURCHASE' THEN 4
    END;

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

-- Grant permissions to admin user (if needed)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO admin;

-- ============================================================================
-- SAMPLE DATA (FOR TESTING)
-- ============================================================================

-- Sample data will be inserted after partitions are created in the completion block

-- ============================================================================
-- UTILITY FUNCTIONS
-- ============================================================================

-- Function to create daily partition for anomaly_sessions
CREATE OR REPLACE FUNCTION create_anomaly_partition(target_date DATE)
RETURNS TEXT AS $$
DECLARE
    partition_name TEXT;
    start_ts TIMESTAMP;
    end_ts TIMESTAMP;
BEGIN
    -- Generate partition name: anomaly_sessions_YYYY_MM_DD
    partition_name := 'anomaly_sessions_' || TO_CHAR(target_date, 'YYYY_MM_DD');
    start_ts := target_date::TIMESTAMP;
    end_ts := (target_date + INTERVAL '1 day')::TIMESTAMP;
    
    -- Check if partition already exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = partition_name
        AND n.nspname = 'public'
    ) THEN
        -- Create partition
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF anomaly_sessions FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_ts, end_ts
        );
        RETURN 'Created partition: ' || partition_name || ' (' || start_ts || ' to ' || end_ts || ')';
    ELSE
        RETURN 'Partition already exists: ' || partition_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to create partitions for next N days
CREATE OR REPLACE FUNCTION create_future_partitions(days_ahead INTEGER DEFAULT 7)
RETURNS TEXT AS $$
DECLARE
    result TEXT := '';
    i INTEGER;
    partition_result TEXT;
BEGIN
    FOR i IN 0..days_ahead LOOP
        SELECT create_anomaly_partition(CURRENT_DATE + i) INTO partition_result;
        result := result || partition_result || E'\n';
    END LOOP;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Function to create initial partitions (for database initialization)
CREATE OR REPLACE FUNCTION create_initial_partitions()
RETURNS TEXT AS $$
DECLARE
    result TEXT;
BEGIN
    -- Create partitions for today + next 7 days
    SELECT create_future_partitions(7) INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Function to clean old anomaly records (keep last 30 days)
-- Now also drops old partitions for better performance
CREATE OR REPLACE FUNCTION cleanup_old_anomalies(days_to_keep INTEGER DEFAULT 30)
RETURNS TEXT AS $$
DECLARE
    partition_record RECORD;
    cutoff_date DATE;
    partition_date DATE;
    result TEXT := '';
    dropped_count INTEGER := 0;
BEGIN
    cutoff_date := CURRENT_DATE - days_to_keep;
    
    -- Drop old partitions (except default partition)
    FOR partition_record IN 
        SELECT c.relname as partition_name
        FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = 'anomaly_sessions'
        AND c.relname LIKE 'anomaly_sessions______%'
        AND c.relname != 'anomaly_sessions_default'
    LOOP
        -- Parse date from partition name: anomaly_sessions_YYYY_MM_DD
        BEGIN
            partition_date := TO_DATE(
                SUBSTRING(partition_record.partition_name FROM 'anomaly_sessions_(\d{4}_\d{2}_\d{2})'),
                'YYYY_MM_DD'
            );
            
            -- Drop if older than cutoff date
            IF partition_date < cutoff_date THEN
                EXECUTE format('DROP TABLE IF EXISTS %I', partition_record.partition_name);
                result := result || 'Dropped partition: ' || partition_record.partition_name || 
                         ' (date: ' || partition_date || ')' || E'\n';
                dropped_count := dropped_count + 1;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                result := result || 'Warning: Could not parse date from ' || partition_record.partition_name || E'\n';
        END;
    END LOOP;
    
    -- Summary
    IF dropped_count = 0 THEN
        result := result || 'No old partitions to drop (keeping last ' || days_to_keep || ' days, cutoff: ' || cutoff_date || ')';
    ELSE
        result := result || 'Total partitions dropped: ' || dropped_count;
    END IF;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Function to get system health
CREATE OR REPLACE FUNCTION get_system_health()
RETURNS TABLE(
    table_name TEXT,
    row_count BIGINT,
    last_update TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'anomaly_sessions'::TEXT,
        COUNT(*)::BIGINT,
        MAX(detected_at)
    FROM anomaly_sessions
    UNION ALL
    SELECT 
        'daily_metrics'::TEXT,
        COUNT(*)::BIGINT,
        MAX(created_at)
    FROM daily_metrics
    UNION ALL
    SELECT 
        'popular_items'::TEXT,
        COUNT(*)::BIGINT,
        MAX(created_at)
    FROM popular_items;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
DECLARE
    partition_result TEXT;
BEGIN
    -- Create initial partitions dynamically
    SELECT create_initial_partitions() INTO partition_result;
    
    -- Insert sample data after partitions are created
    INSERT INTO anomaly_sessions (
        session_id, detected_at, window_start, window_end, 
        click_count, unique_items, anomaly_score, anomaly_type
    ) VALUES (
        999999, 
        CURRENT_TIMESTAMP, 
        CURRENT_TIMESTAMP - INTERVAL '10 seconds', 
        CURRENT_TIMESTAMP,
        150, 
        5, 
        3.0, 
        'HIGH_FREQUENCY'
    );
    
    INSERT INTO daily_metrics (
        metric_date, total_clicks, total_purchases, unique_sessions,
        unique_items, conversion_rate, avg_clicks_per_session,
        avg_session_duration_sec, total_revenue
    ) VALUES (
        CURRENT_DATE - INTERVAL '1 day',
        1000000,
        35000,
        50000,
        10000,
        0.035,
        20.0,
        300.0,
        5000000
    ) ON CONFLICT (metric_date) DO NOTHING;
    
    -- Display initialization summary
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Clickstream Guardian Database Initialized Successfully!';
    RAISE NOTICE '========================================';
    RAISE NOTICE '';
    RAISE NOTICE '📊 Tables Created:';
    RAISE NOTICE '  - anomaly_sessions (PARTITIONED by detected_at)';
    RAISE NOTICE '  - daily_metrics, session_funnel, popular_items';
    RAISE NOTICE '  - popular_categories, session_summary';
    RAISE NOTICE '';
    RAISE NOTICE '📈 Views Created:';
    RAISE NOTICE '  - v_recent_anomalies, v_daily_metrics_summary';
    RAISE NOTICE '  - v_top_items_revenue, v_latest_funnel';
    RAISE NOTICE '';
    RAISE NOTICE '🔧 Functions Created:';
    RAISE NOTICE '  - create_anomaly_partition(date) - Create single partition';
    RAISE NOTICE '  - create_future_partitions(days) - Create multiple partitions';
    RAISE NOTICE '  - create_initial_partitions() - Create initial partitions';
    RAISE NOTICE '  - cleanup_old_anomalies(days) - Drop old partitions safely';
    RAISE NOTICE '  - get_system_health() - Check system status';
    RAISE NOTICE '';
    RAISE NOTICE '🗂️  Initial Partitions Created:';
    RAISE NOTICE '%', partition_result;
    RAISE NOTICE '';
    RAISE NOTICE '💡 Usage Examples:';
    RAISE NOTICE '  - Create partitions: SELECT create_future_partitions(7);';
    RAISE NOTICE '  - Cleanup old data: SELECT cleanup_old_anomalies(30);';
    RAISE NOTICE '  - Check health: SELECT * FROM get_system_health();';
    RAISE NOTICE '  - List partitions: SELECT tablename FROM pg_tables WHERE tablename LIKE ''anomaly_sessions%%'';';
    RAISE NOTICE '';
    RAISE NOTICE '⚠️  Remember: Run create_future_partitions() periodically to create upcoming partitions';
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
END $$;
