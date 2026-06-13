import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from spark_worker_failure_test import (
    inspect_container,
    kafka_offsets,
    parse_timestamp,
    postgres_scalar,
    read_progress,
    run,
    summarize_progress,
    utc_now,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT_DIR / "docs" / "testing" / "results"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test PostgreSQL outage recovery")
    parser.add_argument("--eps", type=int, default=12000)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--fault-after", type=int, default=20)
    parser.add_argument("--outage-seconds", type=int, default=15)
    parser.add_argument("--recovery-timeout", type=int, default=300)
    parser.add_argument("--session-id-base", type=int, default=190_000_000)
    return parser.parse_args()


def postgres_health() -> dict[str, Any]:
    state = json.loads(
        run(
            ["docker", "inspect", "--format", "{{json .State}}", "postgres"]
        ).stdout
    )
    health = state.get("Health", {})
    return {
        "running": bool(state.get("Running")),
        "status": state.get("Status"),
        "health": health.get("Status"),
        "started_at": state.get("StartedAt"),
        "exit_code": state.get("ExitCode"),
        "oom_killed": state.get("OOMKilled"),
    }


def container_restart_policy(name: str) -> str:
    return json.loads(
        run(
            [
                "docker",
                "inspect",
                "--format",
                "{{json .HostConfig.RestartPolicy}}",
                name,
            ]
        ).stdout
    ).get("Name", "")


def wait_for_postgres(timeout: int) -> datetime | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            state = postgres_health()
            if state["running"] and state["health"] == "healthy":
                postgres_scalar("SELECT 1;")
                return utc_now()
        except (subprocess.SubprocessError, ValueError, json.JSONDecodeError):
            pass
        time.sleep(1)
    return None


def wait_for_db_write(previous_count: int, timeout: int) -> tuple[datetime | None, int]:
    deadline = time.monotonic() + timeout
    latest_count = previous_count
    while time.monotonic() < deadline:
        try:
            latest_count = postgres_scalar(
                "SELECT COUNT(*) FROM anomaly_sessions;"
            )
            if latest_count > previous_count:
                return utc_now(), latest_count
        except (subprocess.SubprocessError, ValueError):
            pass
        time.sleep(2)
    return None, latest_count


def relevant_streaming_logs(since: datetime) -> list[str]:
    result = run(
        [
            "docker",
            "logs",
            "--since",
            since.isoformat(),
            "--timestamps",
            "spark-streaming-anomaly",
        ],
        timeout=60,
        check=False,
    )
    lines = (result.stdout + "\n" + result.stderr).splitlines()
    keywords = (
        "postgres",
        "psycopg",
        "connection refused",
        "query terminated",
        "streamingqueryexception",
        "error writing batch",
    )
    return [
        line
        for line in lines
        if any(keyword in line.lower() for keyword in keywords)
    ][-60:]


def main() -> int:
    args = parse_args()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    progress_file = RESULTS_DIR / "pipeline_capacity_progress.jsonl"
    progress_offset = progress_file.stat().st_size if progress_file.exists() else 0
    result_path = RESULTS_DIR / "02_04_postgres_failure.json"
    producer_path = RESULTS_DIR / "02_04_postgres_failure_producer.json"

    postgres_before = postgres_health()
    postgres_restart_policy = container_restart_policy("postgres")
    streaming_before = inspect_container("spark-streaming-anomaly")
    if (
        not postgres_before["running"]
        or postgres_before["health"] != "healthy"
        or not streaming_before["running"]
    ):
        raise RuntimeError("PostgreSQL or Spark Streaming is not ready")

    kafka_before = kafka_offsets()
    db_before = postgres_scalar("SELECT COUNT(*) FROM anomaly_sessions;")
    test_started = utc_now()
    producer = None
    stop_result = None
    start_result = None
    fault_at = None
    recovery_started_at = None

    try:
        producer = subprocess.Popen(
            [
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
            ],
            cwd=ROOT_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

        time.sleep(args.fault_after)
        fault_at = utc_now()
        stop_result = run(
            ["docker", "stop", "--timeout", "0", "postgres"],
            timeout=60,
            check=False,
        )
        time.sleep(args.outage_seconds)
        recovery_started_at = utc_now()
        start_result = run(
            ["docker", "start", "postgres"],
            timeout=60,
            check=False,
        )
    finally:
        try:
            if not postgres_health()["running"]:
                if recovery_started_at is None:
                    recovery_started_at = utc_now()
                start_result = run(
                    ["docker", "start", "postgres"],
                    timeout=60,
                    check=False,
                )
        except (subprocess.SubprocessError, ValueError, json.JSONDecodeError):
            pass

    postgres_healthy_at = wait_for_postgres(args.recovery_timeout)

    producer_stdout = ""
    producer_stderr = ""
    if producer is not None:
        producer_stdout, producer_stderr = producer.communicate(
            timeout=args.duration + 120
        )
    if not producer_path.exists():
        raise RuntimeError("Producer result file was not created")

    producer_result = json.loads(producer_path.read_text(encoding="utf-8"))
    kafka_after = kafka_offsets()
    expected_records = producer_result["click"]["delivered"] + 1
    offset_delta = sum(kafka_after.values()) - sum(kafka_before.values())

    query_recovery_deadline = time.monotonic() + args.recovery_timeout
    progress_summary = {}
    while time.monotonic() < query_recovery_deadline:
        progress_summary = summarize_progress(
            read_progress(progress_offset),
            kafka_before,
            kafka_after,
            fault_at,
        )
        if progress_summary.get("all_queries_recovered_at"):
            break
        time.sleep(2)

    progress_summary = summarize_progress(
        read_progress(progress_offset),
        kafka_before,
        kafka_after,
        fault_at,
    )
    query_recovered_value = progress_summary.get("all_queries_recovered_at")
    query_recovered_at = (
        parse_timestamp(query_recovered_value)
        if query_recovered_value
        else None
    )
    db_write_resumed_at, observed_db_count = wait_for_db_write(
        db_before,
        args.recovery_timeout,
    )

    db_after = postgres_scalar("SELECT COUNT(*) FROM anomaly_sessions;")
    duplicates = postgres_scalar(
        """
        SELECT COUNT(*) FROM (
            SELECT session_id, window_start, anomaly_type, detected_at
            FROM anomaly_sessions
            GROUP BY session_id, window_start, anomaly_type, detected_at
            HAVING COUNT(*) > 1
        ) duplicates;
        """
    )
    postgres_after = postgres_health()
    streaming_after = inspect_container("spark-streaming-anomaly")

    recovery_times = [
        value
        for value in (
            postgres_healthy_at,
            query_recovered_at,
            db_write_resumed_at,
        )
        if value is not None
    ]
    fully_recovered_at = (
        max(recovery_times) if len(recovery_times) == 3 else None
    )
    query_offsets_match = all(
        query["logical_offset_span"] == expected_records
        and query["offsets_contiguous"]
        for query in progress_summary.get("queries", {}).values()
    )
    restart_delta = (
        streaming_after["restart_count"] - streaming_before["restart_count"]
    )

    assertions = {
        "producer_failures_zero": producer_result["click"]["failed"] == 0,
        "producer_achieved_target": producer_result["click"]["passed"],
        "kafka_offset_delta_matches_delivered": offset_delta == expected_records,
        "postgres_outage_command_succeeded": (
            stop_result is not None and stop_result.returncode == 0
        ),
        "postgres_start_command_succeeded": (
            start_result is not None and start_result.returncode == 0
        ),
        "postgres_healthy_after_recovery": postgres_healthy_at is not None,
        "all_queries_reached_final_offsets": query_recovered_at is not None,
        "each_query_offsets_cover_delivered_without_gap": query_offsets_match,
        "postgres_write_resumed": db_write_resumed_at is not None,
        "postgres_rows_increased": db_after > db_before,
        "postgres_business_key_duplicates_zero": duplicates == 0,
        "streaming_running_after_recovery": streaming_after["running"],
        "streaming_container_did_not_restart": restart_delta == 0,
        "queries_did_not_terminate": (
            len(progress_summary.get("query_terminated_events", [])) == 0
        ),
    }
    passed = all(assertions.values())
    failure_reasons = [
        name for name, value in assertions.items() if not value
    ]

    result = {
        "test": {
            "name": "PostgreSQL outage and Spark sink recovery",
            "target_eps": args.eps,
            "load_duration_seconds": args.duration,
            "fault_after_seconds": args.fault_after,
            "outage_seconds": args.outage_seconds,
            "started_at": test_started.isoformat(),
            "fault_at": fault_at.isoformat(),
            "recovery_started_at": recovery_started_at.isoformat(),
            "postgres_healthy_at": (
                postgres_healthy_at.isoformat() if postgres_healthy_at else None
            ),
            "db_write_resumed_at": (
                db_write_resumed_at.isoformat()
                if db_write_resumed_at
                else None
            ),
            "fully_recovered_at": (
                fully_recovered_at.isoformat() if fully_recovered_at else None
            ),
            "finished_at": utc_now().isoformat(),
            "fault_command": (
                "docker stop --timeout 0 postgres; "
                f"wait {args.outage_seconds}s; docker start postgres"
            ),
            "stop_return_code": (
                stop_result.returncode if stop_result is not None else None
            ),
            "start_return_code": (
                start_result.returncode if start_result is not None else None
            ),
        },
        "producer": producer_result,
        "producer_stdout": producer_stdout,
        "producer_stderr": producer_stderr,
        "kafka": {
            "offsets_before": kafka_before,
            "offsets_after": kafka_after,
            "offset_delta": offset_delta,
            "expected_records_including_warmup": expected_records,
        },
        "spark": {
            "streaming_before": streaming_before,
            "streaming_after": streaming_after,
            "restart_count_delta": restart_delta,
            "progress": progress_summary,
            "relevant_logs": relevant_streaming_logs(fault_at),
        },
        "postgres": {
            "restart_policy": postgres_restart_policy,
            "before": postgres_before,
            "after": postgres_after,
            "rows_before": db_before,
            "rows_observed_after_resume": observed_db_count,
            "rows_after": db_after,
            "row_delta": db_after - db_before,
            "duplicate_business_keys": duplicates,
            "rto_seconds": (
                round((fully_recovered_at - fault_at).total_seconds(), 3)
                if fully_recovered_at
                else None
            ),
        },
        "assertions": assertions,
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
                "failed": producer_result["click"]["failed"],
                "producer_p95_ms": producer_result["click"]["latency_ms"]["p95"],
                "kafka_offset_delta": offset_delta,
                "spark_restart_count_delta": restart_delta,
                "query_terminations": len(
                    progress_summary.get("query_terminated_events", [])
                ),
                "rto_seconds": result["postgres"]["rto_seconds"],
                "query_offsets_match": query_offsets_match,
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
