"""
Spark Batch Processing DAG

일일 배치 잡으로 클릭스트림 분석 지표를 산출합니다.
SparkSubmitOperator를 통해 Spark 잡을 오케스트레이션하며,
실행 결과를 pipeline_runs 테이블에 기록합니다.
"""
import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

SPARK_PACKAGES = ",".join([
    "org.postgresql:postgresql:42.6.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.367",
])


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


def record_pipeline_start(**context):
    """pipeline_runs 테이블에 실행 시작을 기록합니다."""
    conn = _get_conn()
    try:
        target_date = (context["logical_date"] - timedelta(days=1)).strftime("%Y-%m-%d")
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (pipeline_name, run_date, start_time, status)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                ("spark_batch_processing", target_date, datetime.utcnow(), "RUNNING"),
            )
            run_id = cur.fetchone()[0]
        conn.commit()
        context["ti"].xcom_push(key="pipeline_run_id", value=run_id)
    finally:
        conn.close()


def record_pipeline_end(**context):
    """pipeline_runs 테이블에 실행 완료를 기록합니다."""
    run_id = context["ti"].xcom_pull(task_ids="record_start", key="pipeline_run_id")
    if not run_id:
        return
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_runs
                SET end_time = %s, status = %s
                WHERE id = %s
                """,
                (datetime.utcnow(), "SUCCESS", run_id),
            )
        conn.commit()
    finally:
        conn.close()


# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'spark_batch_processing',
    default_args=default_args,
    description='일일 Spark 배치 처리 (지표, 인기 아이템, 전환 퍼널)',
    schedule_interval='0 18 * * *',  # 매일 KST 새벽 3시 (UTC 18시, 전날)
    start_date=datetime(2026, 1, 11),
    catchup=False,
    tags=['spark', 'batch', 'analytics'],
) as dag:

    start = EmptyOperator(task_id='start')

    record_start = PythonOperator(
        task_id='record_start',
        python_callable=record_pipeline_start,
    )

    # Task 1: Daily Metrics
    daily_metrics = SparkSubmitOperator(
        task_id='daily_metrics',
        application='/opt/airflow/spark-batch/daily_metrics.py',
        conn_id='spark_default',
        application_args=["{{ (logical_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"],
        packages=SPARK_PACKAGES,
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.submit.deployMode': 'client',
        },
        driver_memory='2g',
        executor_memory='2g',
    )

    # Task 2: Popular Items Analysis
    popular_items = SparkSubmitOperator(
        task_id='popular_items',
        application='/opt/airflow/spark-batch/popular_items.py',
        conn_id='spark_default',
        application_args=["{{ (logical_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"],
        packages=SPARK_PACKAGES,
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.submit.deployMode': 'client',
        },
        driver_memory='2g',
        executor_memory='2g',
    )

    # Task 3: Session Funnel Analysis
    session_funnel = SparkSubmitOperator(
        task_id='session_funnel',
        application='/opt/airflow/spark-batch/session_funnel.py',
        conn_id='spark_default',
        application_args=["{{ (logical_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"],
        packages=SPARK_PACKAGES,
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.submit.deployMode': 'client',
        },
        driver_memory='2g',
        executor_memory='2g',
    )

    record_end = PythonOperator(
        task_id='record_end',
        python_callable=record_pipeline_end,
        trigger_rule='all_success',
    )

    end = EmptyOperator(task_id='end')

    # 순서: start -> record_start -> daily_metrics -> [popular_items, session_funnel] -> record_end -> end
    start >> record_start >> daily_metrics >> [popular_items, session_funnel] >> record_end >> end
