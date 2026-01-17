"""
Spark Batch Processing DAG
Runs daily batch jobs for clickstream analytics using docker exec to spark-master
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

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
    daily_metrics = BashOperator(
        task_id='daily_metrics',
        bash_command='''
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --driver-memory 2g \
          --executor-memory 2g \
          /opt/spark-batch/daily_metrics.py {{ (logical_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}
        ''',
    )

    # Task 2: Popular Items Analysis
    popular_items = BashOperator(
        task_id='popular_items',
        bash_command='''
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --driver-memory 2g \
          --executor-memory 2g \
          /opt/spark-batch/popular_items.py {{ (logical_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}
        ''',
    )

    # Task 3: Session Funnel Analysis
    session_funnel = BashOperator(
        task_id='session_funnel',
        bash_command='''
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          --driver-memory 2g \
          --executor-memory 2g \
          /opt/spark-batch/session_funnel.py {{ (logical_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}
        ''',
    )

    # End dummy operator
    end = DummyOperator(
        task_id='end',
    )

    # 순서 설정: start -> daily_metrics -> [popular_items, session_funnel] -> end
    start >> daily_metrics >> [popular_items, session_funnel] >> end