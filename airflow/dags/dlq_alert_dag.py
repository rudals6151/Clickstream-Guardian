"""
DLQ alerting DAG

Flow:
1) PythonSensor checks DLQ topic offset growth
2) Classify warning/critical based on threshold
3) Send Slack / Email notifications
4) Deduplicate notifications with alert_history table
"""
from __future__ import annotations

import hashlib
import json
import os
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from urllib import request

import psycopg2
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.python import PythonSensor
from kafka import KafkaConsumer, TopicPartition


def _get_var(name: str, default):
    return Variable.get(name, default_var=default)


def _get_dlq_offsets(topic: str, bootstrap_servers: str) -> dict[str, int]:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers.split(","),
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
        request_timeout_ms=5000,
        api_version_auto_timeout_ms=5000,
    )
    partitions = consumer.partitions_for_topic(topic) or set()
    if not partitions:
        consumer.close()
        return {}

    tp_list = [TopicPartition(topic, p) for p in sorted(partitions)]
    end_offsets = consumer.end_offsets(tp_list)
    consumer.close()
    return {f"{tp.topic}:{tp.partition}": int(end_offsets[tp]) for tp in tp_list}


def _sum_offsets(offsets: dict[str, int]) -> int:
    return sum(offsets.values()) if offsets else 0


def detect_dlq_growth(**context) -> bool:
    topic = _get_var("DLQ_TOPIC", "km.events.dlq.v1")
    bootstrap = _get_var(
        "KAFKA_BOOTSTRAP_SERVERS",
        os.getenv("KAFKA_INTERNAL_SERVERS", "kafka-1:29092,kafka-2:29093,kafka-3:29094"),
    )

    current_offsets = _get_dlq_offsets(topic, bootstrap)
    previous_offsets = json.loads(_get_var("DLQ_LAST_OFFSETS", "{}"))

    current_total = _sum_offsets(current_offsets)
    previous_total = _sum_offsets(previous_offsets)
    growth = max(0, current_total - previous_total)

    context["ti"].xcom_push(key="dlq_growth", value=growth)
    context["ti"].xcom_push(key="dlq_offsets", value=current_offsets)
    context["ti"].xcom_push(key="dlq_topic", value=topic)

    Variable.set("DLQ_LAST_OFFSETS", json.dumps(current_offsets))
    return growth > 0


def classify_severity(**context) -> str:
    growth = int(context["ti"].xcom_pull(task_ids="detect_dlq_growth", key="dlq_growth") or 0)
    warning_threshold = int(_get_var("DLQ_WARNING_THRESHOLD", os.getenv("DLQ_WARNING_THRESHOLD", "1")))
    critical_threshold = int(_get_var("DLQ_CRITICAL_THRESHOLD", os.getenv("DLQ_CRITICAL_THRESHOLD", "20")))

    if growth >= critical_threshold:
        severity = "CRITICAL"
    elif growth >= warning_threshold:
        severity = "WARNING"
    else:
        severity = "NONE"

    context["ti"].xcom_push(key="severity", value=severity)
    return severity


def branch_by_severity(**context) -> str:
    severity = context["ti"].xcom_pull(task_ids="classify_severity", key="severity")
    if severity == "CRITICAL":
        return "notify_critical"
    if severity == "WARNING":
        return "notify_warning"
    return "no_alert"


def _insert_alert_if_new(severity: str, topic: str, offsets: dict[str, int], message: str) -> bool:
    suppress_minutes = int(_get_var("DLQ_SUPPRESS_MINUTES", os.getenv("DLQ_SUPPRESS_MINUTES", "10")))
    bucket = datetime.utcnow().replace(second=0, microsecond=0)
    bucket = bucket - timedelta(minutes=(bucket.minute % suppress_minutes))

    payload = f"{severity}|{topic}|{json.dumps(offsets, sort_keys=True)}"
    alert_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    pg_password = os.getenv("POSTGRES_PASSWORD")
    if not pg_password:
        raise EnvironmentError("필수 환경변수 'POSTGRES_PASSWORD'이(가) 설정되지 않았습니다.")
    with psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "clickstream"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=pg_password,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO alert_history (alert_hash, severity, topic_name, partition_offsets, message, time_bucket)
                VALUES (%s, %s, %s, %s::jsonb, %s, %s)
                ON CONFLICT (alert_hash, time_bucket) DO NOTHING
                """,
                (alert_hash, severity, topic, json.dumps(offsets), message, bucket),
            )
            inserted = cur.rowcount == 1
        conn.commit()
    return inserted


def _send_slack(text: str) -> None:
    webhook = _get_var("SLACK_WEBHOOK_URL", os.getenv("SLACK_WEBHOOK_URL", ""))
    if not webhook:
        return
    payload = json.dumps({"text": text}).encode("utf-8")
    req = request.Request(webhook, data=payload, headers={"Content-Type": "application/json"})
    request.urlopen(req, timeout=10).read()


def _send_email(subject: str, body: str) -> None:
    sender = _get_var("EMAIL_SENDER", os.getenv("EMAIL_SENDER", ""))
    password = _get_var("EMAIL_PASSWORD", os.getenv("EMAIL_PASSWORD", ""))
    recipient = _get_var("EMAIL_RECIPIENT", os.getenv("EMAIL_RECIPIENT", ""))
    server = _get_var("EMAIL_SMTP_SERVER", os.getenv("EMAIL_SMTP_SERVER", "smtp.gmail.com"))
    port = int(_get_var("EMAIL_SMTP_PORT", os.getenv("EMAIL_SMTP_PORT", "587")))
    if not (sender and password and recipient):
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    with smtplib.SMTP(server, port) as smtp:
        smtp.starttls()
        smtp.login(sender, password)
        smtp.sendmail(sender, [recipient], msg.as_string())


def notify_warning(**context) -> None:
    growth = int(context["ti"].xcom_pull(task_ids="detect_dlq_growth", key="dlq_growth") or 0)
    offsets = context["ti"].xcom_pull(task_ids="detect_dlq_growth", key="dlq_offsets") or {}
    topic = context["ti"].xcom_pull(task_ids="detect_dlq_growth", key="dlq_topic") or "km.events.dlq.v1"
    text = f"[WARNING] DLQ growth detected: +{growth} events on {topic}. offsets={offsets}"

    if _insert_alert_if_new("WARNING", topic, offsets, text):
        _send_slack(text)


def notify_critical(**context) -> None:
    growth = int(context["ti"].xcom_pull(task_ids="detect_dlq_growth", key="dlq_growth") or 0)
    offsets = context["ti"].xcom_pull(task_ids="detect_dlq_growth", key="dlq_offsets") or {}
    topic = context["ti"].xcom_pull(task_ids="detect_dlq_growth", key="dlq_topic") or "km.events.dlq.v1"
    text = f"[CRITICAL] DLQ growth detected: +{growth} events on {topic}. offsets={offsets}"

    if _insert_alert_if_new("CRITICAL", topic, offsets, text):
        _send_slack(text)
        _send_email("[CRITICAL] Clickstream DLQ Alert", text)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "dlq_alert_dag",
    default_args=default_args,
    description="DLQ offset growth sensor and alerting DAG",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2026, 2, 22),
    catchup=False,
    tags=["dlq", "alerting", "sensor"],
) as dag:
    detect = PythonSensor(
        task_id="detect_dlq_growth",
        python_callable=detect_dlq_growth,
        poke_interval=30,
        timeout=50,
        mode="poke",
    )

    classify = PythonOperator(
        task_id="classify_severity",
        python_callable=classify_severity,
    )

    branch = BranchPythonOperator(
        task_id="branch_by_severity",
        python_callable=branch_by_severity,
    )

    warning = PythonOperator(
        task_id="notify_warning",
        python_callable=notify_warning,
    )

    critical = PythonOperator(
        task_id="notify_critical",
        python_callable=notify_critical,
    )

    no_alert = EmptyOperator(task_id="no_alert")
    done = EmptyOperator(task_id="done")

    detect >> classify >> branch
    branch >> warning >> done
    branch >> critical >> done
    branch >> no_alert >> done
