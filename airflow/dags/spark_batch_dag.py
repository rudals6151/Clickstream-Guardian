"""
Spark Batch Processing DAG
Runs daily batch jobs for clickstream analytics using SparkSubmitOperator.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator

SPARK_PACKAGES = ",".join([
    "org.postgresql:postgresql:42.6.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.367",
])

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
    description='Daily Spark batch processing for clickstream data',
    schedule_interval='0 18 * * *',  # 매일 KST 새벽 3시 (UTC 18시, 전날)
    start_date=datetime(2026, 1, 11),
    catchup=False,
    tags=['spark', 'batch', 'analytics'],
) as dag:

    # Start dummy operator
    start = DummyOperator(
        task_id='start',
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

    # End dummy operator
    end = DummyOperator(
        task_id='end',
    )

    # 순서 설정: start -> daily_metrics -> [popular_items, session_funnel] -> end
    start >> daily_metrics >> [popular_items, session_funnel] >> end
