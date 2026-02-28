import argparse
import json
import os
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor


def percentile_sql(column: str, p: float) -> str:
    return f"percentile_cont({p}) within group (order by {column})"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate SLO report from anomaly_sessions")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window in hours")
    args = parser.parse_args()

    pg_password = os.getenv("POSTGRES_PASSWORD")
    if not pg_password:
        raise EnvironmentError("필수 환경변수 'POSTGRES_PASSWORD'이(가) 설정되지 않았습니다.")
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "clickstream"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=pg_password,
        cursor_factory=RealDictCursor,
    )

    query = f"""
    with base as (
      select
        extract(epoch from (kafka_ingest_ts - to_timestamp(event_ts / 1000.0))) * 1000 as produce_to_kafka_ms,
        extract(epoch from (spark_process_ts - kafka_ingest_ts)) * 1000 as kafka_to_spark_ms,
        extract(epoch from (db_write_ts - spark_process_ts)) * 1000 as spark_to_db_ms,
        extract(epoch from (db_write_ts - to_timestamp(event_ts / 1000.0))) * 1000 as e2e_ms
      from anomaly_sessions
      where detected_at >= now() - (%s * interval '1 hour')
        and kafka_ingest_ts is not null
        and spark_process_ts is not null
        and db_write_ts is not null
    )
    select
      count(*) as samples,
      {percentile_sql("produce_to_kafka_ms", 0.5)} as p50_produce_to_kafka_ms,
      {percentile_sql("kafka_to_spark_ms", 0.5)} as p50_kafka_to_spark_ms,
      {percentile_sql("spark_to_db_ms", 0.5)} as p50_spark_to_db_ms,
      {percentile_sql("e2e_ms", 0.5)} as p50_e2e_ms,
      {percentile_sql("e2e_ms", 0.95)} as p95_e2e_ms,
      {percentile_sql("e2e_ms", 0.99)} as p99_e2e_ms
    from base;
    """

    with conn, conn.cursor() as cur:
        cur.execute(query, (args.hours,))
        report = cur.fetchone()

    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "window_hours": args.hours,
        "report": report,
    }
    print(json.dumps(output, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
