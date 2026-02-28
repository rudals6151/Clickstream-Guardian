"""
파티션 관리 DAG

anomaly_sessions 테이블의 파티션을 자동으로 관리합니다:
1. 향후 7일치 파티션 사전 생성
2. 30일 이전 파티션 자동 정리
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


def _get_conn():
    pg_password = os.getenv("POSTGRES_PASSWORD")
    if not pg_password:
        raise EnvironmentError("필수 환경변수 'POSTGRES_PASSWORD'이(가) 설정되지 않았습니다.")
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "clickstream"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=pg_password,
    )


def create_future_partitions(**context):
    """향후 7일치 파티션을 사전 생성합니다."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT create_future_partitions(7)")
            result = cur.fetchone()[0]
        conn.commit()
        print(f"파티션 생성 결과:\n{result}")
    finally:
        conn.close()


def cleanup_old_partitions(**context):
    """30일 이전 파티션을 정리합니다."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT cleanup_old_anomalies(30)")
            result = cur.fetchone()[0]
        conn.commit()
        print(f"파티션 정리 결과:\n{result}")
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "partition_manager",
    default_args=default_args,
    description="anomaly_sessions 파티션 자동 생성 및 정리",
    schedule_interval="0 0 * * *",  # 매일 자정 (UTC)
    start_date=datetime(2026, 2, 22),
    catchup=False,
    tags=["maintenance", "partition"],
) as dag:

    create_task = PythonOperator(
        task_id="create_future_partitions",
        python_callable=create_future_partitions,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_old_partitions",
        python_callable=cleanup_old_partitions,
    )

    create_task >> cleanup_task
