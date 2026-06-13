import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT_DIR / "docs" / "testing" / "results"
PROGRESS_FILE = RESULTS_DIR / "pipeline_capacity_progress.jsonl"
QUERY_NAMES = {"late_events", "high_frequency", "bot_like"}


def run(
    command: list[str],
    timeout: int = 30,
    check: bool = True,
) -> subprocess.CompletedProcess:
    return subprocess.run(
        command,
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=check,
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def inspect_spark() -> dict[str, Any]:
    result = run(
        [
            "docker",
            "inspect",
            "--format",
            "{{json .State}}",
            "spark-streaming-anomaly",
        ]
    )
    state = json.loads(result.stdout)
    return {
        "status": state["Status"],
        "running": state["Running"],
        "started_at": state["StartedAt"],
        "finished_at": state["FinishedAt"],
        "restart_count": int(
            run(
                [
                    "docker",
                    "inspect",
                    "--format",
                    "{{.RestartCount}}",
                    "spark-streaming-anomaly",
                ]
            ).stdout.strip()
        ),
        "oom_killed": state["OOMKilled"],
        "exit_code": state["ExitCode"],
    }


def kafka_offsets() -> dict[str, int]:
    result = run(
        [
            "docker",
            "exec",
            "kafka-1",
            "kafka-get-offsets",
            "--bootstrap-server",
            "kafka-1:29092",
            "--topic",
            "km.clicks.raw.v1",
        ],
        timeout=60,
    )
    offsets: dict[str, int] = {}
    for line in result.stdout.splitlines():
        topic, partition, offset = line.strip().split(":")
        if topic == "km.clicks.raw.v1":
            offsets[partition] = int(offset)
    return offsets


def postgres_scalar(sql: str) -> int:
    result = run(
        [
            "docker",
            "exec",
            "postgres",
            "psql",
            "-U",
            "admin",
            "-d",
            "clickstream",
            "-t",
            "-A",
            "-c",
            sql,
        ],
        timeout=60,
    )
    return int(result.stdout.strip())


def read_progress(start_offset: int) -> list[dict[str, Any]]:
    if not PROGRESS_FILE.exists():
        return []
    with PROGRESS_FILE.open("rb") as stream:
        stream.seek(start_offset)
        payload = stream.read().decode("utf-8", errors="replace")
    records = []
    for line in payload.splitlines():
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def source_offsets(record: dict[str, Any], field: str) -> dict[str, int]:
    sources = record.get("sources", [])
    if not sources:
        return {}
    topic_offsets = sources[0].get(field, {}).get("km.clicks.raw.v1", {})
    return {str(partition): int(offset) for partition, offset in topic_offsets.items()}


def offsets_reached(record: dict[str, Any], target: dict[str, int]) -> bool:
    current = source_offsets(record, "endOffset")
    return bool(current) and all(current.get(partition, -1) >= offset for partition, offset in target.items())


def progress_completion(record: dict[str, Any]) -> datetime:
    started = parse_timestamp(record["timestamp"])
    duration_ms = int(record.get("durationMs", {}).get("triggerExecution", 0))
    return started + timedelta(milliseconds=duration_ms)


def summarize_progress(
    records: list[dict[str, Any]],
    final_offsets: dict[str, int],
    fault_at: datetime,
) -> dict[str, Any]:
    started = [
        record
        for record in records
        if record.get("event") == "query_started"
        and record.get("name") in QUERY_NAMES
        and parse_timestamp(record["timestamp"]) > fault_at
    ]
    new_runs = {record["name"]: record["run_id"] for record in started}
    progress = [
        record
        for record in records
        if record.get("event") == "query_progress"
        and record.get("name") in QUERY_NAMES
    ]

    query_results = {}
    full_recovery_times = []
    for name in sorted(QUERY_NAMES):
        rows = [record for record in progress if record.get("name") == name]
        resumed = [
            record
            for record in rows
            if record.get("runId") == new_runs.get(name)
            and int(record.get("numInputRows", 0)) > 0
        ]
        recovered = [
            record
            for record in rows
            if record.get("runId") == new_runs.get(name)
            and offsets_reached(record, final_offsets)
            and int(
                record.get("sources", [{}])[0]
                .get("metrics", {})
                .get("maxOffsetsBehindLatest", 0)
            )
            == 0
        ]
        recovery_record = min(recovered, key=progress_completion) if recovered else None
        if recovery_record:
            full_recovery_times.append(progress_completion(recovery_record))
        query_results[name] = {
            "new_run_id": new_runs.get(name),
            "batches_observed": len(rows),
            "input_rows_total": sum(int(record.get("numInputRows", 0)) for record in rows),
            "max_lag": max(
                (
                    int(
                        record.get("sources", [{}])[0]
                        .get("metrics", {})
                        .get("maxOffsetsBehindLatest", 0)
                    )
                    for record in rows
                    if record.get("sources")
                ),
                default=0,
            ),
            "first_resumed_batch_completed_at": (
                min(progress_completion(record) for record in resumed).isoformat()
                if resumed
                else None
            ),
            "final_offsets_reached_at": (
                progress_completion(recovery_record).isoformat()
                if recovery_record
                else None
            ),
            "final_lag": (
                int(
                    recovery_record.get("sources", [{}])[0]
                    .get("metrics", {})
                    .get("maxOffsetsBehindLatest", 0)
                )
                if recovery_record
                else None
            ),
        }

    return {
        "query_started": {
            record["name"]: {
                "run_id": record["run_id"],
                "timestamp": record["timestamp"],
            }
            for record in started
        },
        "queries": query_results,
        "all_queries_recovered_at": (
            max(full_recovery_times).isoformat()
            if len(full_recovery_times) == len(QUERY_NAMES)
            else None
        ),
        "terminated_events": [
            record for record in records if record.get("event") == "query_terminated"
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test Spark application recovery")
    parser.add_argument("--eps", type=int, default=12000)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--fault-after", type=int, default=20)
    parser.add_argument("--recovery-timeout", type=int, default=300)
    parser.add_argument("--session-id-base", type=int, default=90_000_000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    progress_offset = PROGRESS_FILE.stat().st_size if PROGRESS_FILE.exists() else 0
    result_path = RESULTS_DIR / "02_01_spark_application_failure.json"
    producer_path = RESULTS_DIR / "02_01_spark_application_failure_producer.json"

    spark_before = inspect_spark()
    if not spark_before["running"]:
        raise RuntimeError("spark-streaming-anomaly is not running")

    kafka_before = kafka_offsets()
    db_before = postgres_scalar("SELECT COUNT(*) FROM anomaly_sessions;")
    test_started = utc_now()

    producer_command = [
        sys.executable,
        str(ROOT_DIR / "scripts" / "producer_throughput_test.py"),
        "--click-eps",
        str(args.eps),
        "--purchase-eps",
        "0",
        "--duration",
        str(args.duration),
        "--bootstrap",
        "localhost:9092,localhost:9093,localhost:9094",
        "--session-id-base",
        str(args.session_id_base),
        "--output",
        str(producer_path),
    ]
    producer = subprocess.Popen(
        producer_command,
        cwd=ROOT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    time.sleep(args.fault_after)
    fault_at = utc_now()
    kill_result = run(
        [
            "docker",
            "exec",
            "spark-streaming-anomaly",
            "pkill",
            "-9",
            "-f",
            "^python3 /opt/spark-streaming/anomaly_detector.py",
        ],
        timeout=30,
        check=False,
    )

    restart_observed_at = None
    restart_state = None
    restart_deadline = time.monotonic() + 90
    while time.monotonic() < restart_deadline:
        state = inspect_spark()
        if (
            state["running"]
            and state["restart_count"] > spark_before["restart_count"]
            and state["started_at"] != spark_before["started_at"]
        ):
            restart_observed_at = utc_now()
            restart_state = state
            break
        time.sleep(1)

    producer_stdout, producer_stderr = producer.communicate(
        timeout=args.duration + 60
    )
    producer_result = json.loads(producer_path.read_text(encoding="utf-8"))
    kafka_after = kafka_offsets()
    produced_offset_delta = sum(kafka_after.values()) - sum(kafka_before.values())

    recovery_deadline = time.monotonic() + args.recovery_timeout
    progress_summary = {}
    while time.monotonic() < recovery_deadline:
        records = read_progress(progress_offset)
        progress_summary = summarize_progress(records, kafka_after, fault_at)
        if progress_summary.get("all_queries_recovered_at"):
            break
        time.sleep(2)

    test_finished = utc_now()
    records = read_progress(progress_offset)
    progress_summary = summarize_progress(records, kafka_after, fault_at)
    db_after = postgres_scalar("SELECT COUNT(*) FROM anomaly_sessions;")
    duplicate_business_keys = postgres_scalar(
        """
        SELECT COUNT(*) FROM (
            SELECT session_id, window_start, anomaly_type, detected_at
            FROM anomaly_sessions
            GROUP BY session_id, window_start, anomaly_type, detected_at
            HAVING COUNT(*) > 1
        ) duplicates;
        """
    )
    spark_after = inspect_spark()

    # The producer sends one warm-up click before the measured load.
    expected_kafka_records = producer_result["click"]["delivered"] + 1
    query_rows_match = all(
        query["input_rows_total"] == expected_kafka_records
        for query in progress_summary.get("queries", {}).values()
    )
    recovered_at_value = progress_summary.get("all_queries_recovered_at")
    recovered_at = parse_timestamp(recovered_at_value) if recovered_at_value else None
    passed = all(
        [
            producer_result["click"]["failed"] == 0,
            produced_offset_delta == expected_kafka_records,
            restart_state is not None,
            recovered_at is not None,
            query_rows_match,
            duplicate_business_keys == 0,
            db_after > db_before,
            spark_after["running"],
            not spark_after["oom_killed"],
        ]
    )

    failure_reasons = []
    if producer_result["click"]["failed"]:
        failure_reasons.append("producer delivery failures occurred")
    if produced_offset_delta != expected_kafka_records:
        failure_reasons.append("Kafka offset delta did not match delivered messages")
    if restart_state is None:
        failure_reasons.append("Spark container automatic restart was not observed")
    if recovered_at is None:
        failure_reasons.append("all Spark queries did not reach final Kafka offsets")
    if not query_rows_match:
        failure_reasons.append("query input totals did not match delivered messages")
    if duplicate_business_keys:
        failure_reasons.append("duplicate PostgreSQL business keys were found")
    if db_after <= db_before:
        failure_reasons.append("PostgreSQL anomaly rows did not increase")
    if not spark_after["running"] or spark_after["oom_killed"]:
        failure_reasons.append("Spark container was not healthy after recovery")

    result = {
        "test": {
            "name": "Spark application forced termination and checkpoint recovery",
            "target_eps": args.eps,
            "load_duration_seconds": args.duration,
            "fault_after_seconds": args.fault_after,
            "recovery_timeout_seconds": args.recovery_timeout,
            "started_at": test_started.isoformat(),
            "fault_at": fault_at.isoformat(),
            "restart_observed_at": (
                restart_observed_at.isoformat() if restart_observed_at else None
            ),
            "finished_at": test_finished.isoformat(),
            "fault_command": (
                "docker exec spark-streaming-anomaly pkill -9 -f "
                "'^python3 /opt/spark-streaming/anomaly_detector.py'"
            ),
            "kill_return_code": kill_result.returncode,
            "kill_output": (kill_result.stdout + kill_result.stderr).strip(),
        },
        "producer": producer_result,
        "producer_stdout": producer_stdout,
        "producer_stderr": producer_stderr,
        "kafka": {
            "offsets_before": kafka_before,
            "offsets_after": kafka_after,
            "offset_delta": produced_offset_delta,
            "expected_records_including_warmup": expected_kafka_records,
        },
        "spark": {
            "before": spark_before,
            "after_restart": restart_state,
            "after_test": spark_after,
            "progress": progress_summary,
            "rto_seconds": (
                round((recovered_at - fault_at).total_seconds(), 3)
                if recovered_at
                else None
            ),
        },
        "postgres": {
            "rows_before": db_before,
            "rows_after": db_after,
            "row_delta": db_after - db_before,
            "duplicate_business_keys": duplicate_business_keys,
        },
        "assertions": {
            "producer_failures_zero": producer_result["click"]["failed"] == 0,
            "kafka_offset_delta_matches_delivered": (
                produced_offset_delta == expected_kafka_records
            ),
            "automatic_restart_observed": restart_state is not None,
            "all_queries_reached_final_offsets": recovered_at is not None,
            "each_query_input_matches_delivered": query_rows_match,
            "postgres_business_key_duplicates_zero": duplicate_business_keys == 0,
            "postgres_rows_increased": db_after > db_before,
            "spark_running_after_recovery": spark_after["running"],
        },
        "passed": passed,
        "failure_reasons": failure_reasons,
    }
    result_path.write_text(
        json.dumps(result, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "passed": passed,
                "delivered": producer_result["click"]["delivered"],
                "kafka_offset_delta": produced_offset_delta,
                "restart_count": spark_after["restart_count"],
                "rto_seconds": result["spark"]["rto_seconds"],
                "query_rows_match": query_rows_match,
                "postgres_delta": db_after - db_before,
                "result": str(result_path),
                "failure_reasons": failure_reasons,
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
