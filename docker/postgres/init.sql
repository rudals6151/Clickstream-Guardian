-- Clickstream Guardian Database Initialization Script

-- Extension for UUID generation (optional)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- REAL-TIME ANOMALY DETECTION TABLES
-- ============================================================================

-- Anomaly Sessions Table
CREATE TABLE IF NOT EXISTS anomaly_sessions (
    session_id BIGINT PRIMARY KEY,
    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    click_count INTEGER NOT NULL CHECK (click_count > 0),
    unique_items INTEGER NOT NULL CHECK (unique_items >= 0),
    anomaly_score FLOAT NOT NULL CHECK (anomaly_score >= 0),
    anomaly_type VARCHAR(50) NOT NULL CHECK (anomaly_type IN ('HIGH_FREQUENCY', 'BOT_LIKE', 'SPAM', 'OTHER')),
    category_distribution JSONB,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for anomaly_sessions
CREATE INDEX IF NOT EXISTS idx_anomaly_detected_at ON anomaly_sessions(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_type ON anomaly_sessions(anomaly_type);
CREATE INDEX IF NOT EXISTS idx_anomaly_score ON anomaly_sessions(anomaly_score DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_window ON anomaly_sessions(window_start, window_end);

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
    total_revenue BIGINT DEFAULT 0 CHECK (total_revenue >= 0),
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
    revenue BIGINT NOT NULL DEFAULT 0 CHECK (revenue >= 0),
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
    revenue BIGINT NOT NULL DEFAULT 0,
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

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View: Recent Anomalies (Last 24 Hours)
CREATE OR REPLACE VIEW v_recent_anomalies AS
SELECT 
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

-- Insert sample anomaly session
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
) ON CONFLICT (session_id) DO NOTHING;

-- Insert sample daily metric
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

-- ============================================================================
-- UTILITY FUNCTIONS
-- ============================================================================

-- Function to clean old anomaly records (keep last 30 days)
CREATE OR REPLACE FUNCTION cleanup_old_anomalies()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM anomaly_sessions
    WHERE detected_at < CURRENT_TIMESTAMP - INTERVAL '30 days';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
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
BEGIN
    RAISE NOTICE 'Clickstream Guardian database initialized successfully!';
    RAISE NOTICE 'Tables created: anomaly_sessions, daily_metrics, session_funnel, popular_items, popular_categories, session_summary';
    RAISE NOTICE 'Views created: v_recent_anomalies, v_daily_metrics_summary, v_top_items_revenue, v_latest_funnel';
    RAISE NOTICE 'Functions created: cleanup_old_anomalies(), get_system_health()';
END $$;
