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


def inspect_container(name: str) -> dict[str, Any]:
    state = json.loads(
        run(
            ["docker", "inspect", "--format", "{{json .State}}", name]
        ).stdout
    )
    restart_count = int(
        run(
            ["docker", "inspect", "--format", "{{.RestartCount}}", name]
        ).stdout.strip()
    )
    return {
        "status": state["Status"],
        "running": state["Running"],
        "started_at": state["StartedAt"],
        "finished_at": state["FinishedAt"],
        "restart_count": restart_count,
        "oom_killed": state["OOMKilled"],
        "exit_code": state["ExitCode"],
    }


def spark_master_state() -> dict[str, Any]:
    result = run(
        [
            "docker",
            "exec",
            "spark-master",
            "sh",
            "-c",
            "wget -qO- http://localhost:8080/json/",
        ],
        timeout=30,
    )
    payload = json.loads(result.stdout)
    workers = payload.get("workers", [])
    apps = payload.get("activeapps", [])
    return {
        "alive_workers": int(payload.get("aliveworkers", 0)),
        "workers": [
            {
                "id": worker.get("id"),
                "state": worker.get("state"),
                "cores": worker.get("cores"),
                "cores_used": worker.get("coresused"),
                "memory": worker.get("memory"),
                "memory_used": worker.get("memoryused"),
            }
            for worker in workers
        ],
        "active_apps": [
            {
                "id": app.get("id"),
                "name": app.get("name"),
                "state": app.get("state"),
                "cores": app.get("cores"),
            }
            for app in apps
        ],
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
    return bool(current) and all(
        current.get(partition, -1) >= offset
        for partition, offset in target.items()
    )


def progress_completion(record: dict[str, Any]) -> datetime:
    started = parse_timestamp(record["timestamp"])
    duration_ms = int(record.get("durationMs", {}).get("triggerExecution", 0))
    return started + timedelta(milliseconds=duration_ms)


def summarize_progress(
    records: list[dict[str, Any]],
    initial_offsets: dict[str, int],
    final_offsets: dict[str, int],
    fault_at: datetime,
) -> dict[str, Any]:
    progress = [
        record
        for record in records
        if record.get("event") == "query_progress"
        and record.get("name") in QUERY_NAMES
    ]
    query_results = {}
    recovery_times = []
    run_ids = {}

    for name in sorted(QUERY_NAMES):
        rows = [record for record in progress if record.get("name") == name]
        rows.sort(key=lambda record: int(record.get("batchId", -1)))
        if rows:
            run_ids[name] = sorted({record.get("runId") for record in rows})
        offset_ranges = [
            (
                source_offsets(record, "startOffset"),
                source_offsets(record, "endOffset"),
            )
            for record in rows
            if source_offsets(record, "startOffset")
            and source_offsets(record, "endOffset")
        ]
        offsets_contiguous = bool(offset_ranges)
        if offset_ranges:
            offsets_contiguous = offset_ranges[0][0] == initial_offsets
            offsets_contiguous = offsets_contiguous and all(
                previous[1] == current[0]
                for previous, current in zip(offset_ranges, offset_ranges[1:])
            )
        logical_offset_span = (
            sum(offset_ranges[-1][1].values())
            - sum(offset_ranges[0][0].values())
            if offset_ranges
            else 0
        )
        input_rows_total = sum(
            int(record.get("numInputRows", 0)) for record in rows
        )
        resumed = [
            record
            for record in rows
            if progress_completion(record) > fault_at
            and int(record.get("numInputRows", 0)) > 0
        ]
        recovered = [
            record
            for record in rows
            if offsets_reached(record, final_offsets)
            and int(
                record.get("sources", [{}])[0]
                .get("metrics", {})
                .get("maxOffsetsBehindLatest", 0)
            )
            == 0
        ]
        recovery_record = min(recovered, key=progress_completion) if recovered else None
        if recovery_record:
            recovery_times.append(progress_completion(recovery_record))
        query_results[name] = {
            "run_ids": run_ids.get(name, []),
            "batches_observed": len(rows),
            "input_rows_metric_total": input_rows_total,
            "logical_offset_span": logical_offset_span,
            "input_rows_retry_overcount": input_rows_total - logical_offset_span,
            "offsets_contiguous": offsets_contiguous,
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
            "first_batch_after_fault_completed_at": (
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
        "queries": query_results,
        "all_queries_recovered_at": (
            max(recovery_times).isoformat()
            if len(recovery_times) == len(QUERY_NAMES)
            else None
        ),
        "query_started_events": [
            record
            for record in records
            if record.get("event") == "query_started"
        ],
        "query_terminated_events": [
            record
            for record in records
            if record.get("event") == "query_terminated"
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test Spark Worker recovery")
    parser.add_argument("--eps", type=int, default=12000)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--fault-after", type=int, default=20)
    parser.add_argument("--outage-seconds", type=int, default=15)
    parser.add_argument("--recovery-timeout", type=int, default=300)
    parser.add_argument("--session-id-base", type=int, default=130_000_000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    progress_offset = PROGRESS_FILE.stat().st_size if PROGRESS_FILE.exists() else 0
    result_path = RESULTS_DIR / "02_02_spark_worker_failure.json"
    producer_path = RESULTS_DIR / "02_02_spark_worker_failure_producer.json"

    worker_before = inspect_container("spark-worker")
    streaming_before = inspect_container("spark-streaming-anomaly")
    master_before = spark_master_state()
    if not worker_before["running"] or master_before["alive_workers"] != 1:
        raise RuntimeError("Spark Worker is not ready")

    kafka_before = kafka_offsets()
    db_before = postgres_scalar("SELECT COUNT(*) FROM anomaly_sessions;")
    test_started = utc_now()

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
        [
            "docker",
            "stop",
            "--timeout",
            "0",
            "spark-worker",
        ],
        timeout=60,
        check=False,
    )
    time.sleep(args.outage_seconds)
    recovery_started_at = utc_now()
    start_result = run(
        ["docker", "start", "spark-worker"],
        timeout=60,
        check=False,
    )

    restart_observed_at = None
    executor_reallocated_at = None
    worker_after_restart = None
    restart_deadline = time.monotonic() + 120
    while time.monotonic() < restart_deadline:
        worker_state = inspect_container("spark-worker")
        if (
            worker_state["running"]
            and worker_state["started_at"] != worker_before["started_at"]
        ):
            if restart_observed_at is None:
                restart_observed_at = utc_now()
                worker_after_restart = worker_state
            master_state = spark_master_state()
            workers = master_state.get("workers", [])
            active_workers = [
                worker
                for worker in workers
                if worker.get("state") == "ALIVE"
            ]
            if (
                master_state["alive_workers"] == 1
                and active_workers
                and active_workers[0].get("cores_used") == 2
                and active_workers[0].get("memory_used") == 2048
            ):
                executor_reallocated_at = utc_now()
                break
        time.sleep(1)

    producer_stdout, producer_stderr = producer.communicate(
        timeout=args.duration + 60
    )
    producer_result = json.loads(producer_path.read_text(encoding="utf-8"))
    kafka_after = kafka_offsets()
    expected_records = producer_result["click"]["delivered"] + 1
    offset_delta = sum(kafka_after.values()) - sum(kafka_before.values())

    recovery_deadline = time.monotonic() + args.recovery_timeout
    progress_summary = {}
    while time.monotonic() < recovery_deadline:
        progress_summary = summarize_progress(
            read_progress(progress_offset), kafka_before, kafka_after, fault_at
        )
        if progress_summary.get("all_queries_recovered_at"):
            break
        time.sleep(2)

    progress_summary = summarize_progress(
        read_progress(progress_offset), kafka_before, kafka_after, fault_at
    )
    recovered_value = progress_summary.get("all_queries_recovered_at")
    recovered_at = parse_timestamp(recovered_value) if recovered_value else None
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
    worker_after = inspect_container("spark-worker")
    streaming_after = inspect_container("spark-streaming-anomaly")
    master_after = spark_master_state()
    query_offsets_match = all(
        query["logical_offset_span"] == expected_records
        and query["offsets_contiguous"]
        for query in progress_summary.get("queries", {}).values()
    )
    query_runs_unchanged = all(
        len(query["run_ids"]) == 1
        for query in progress_summary.get("queries", {}).values()
    )

    assertions = {
        "producer_failures_zero": producer_result["click"]["failed"] == 0,
        "kafka_offset_delta_matches_delivered": offset_delta == expected_records,
        "worker_restart_after_controlled_outage_observed": (
            restart_observed_at is not None
        ),
        "executor_reallocated": executor_reallocated_at is not None,
        "all_queries_reached_final_offsets": recovered_at is not None,
        "each_query_offsets_cover_delivered_without_gap": query_offsets_match,
        "query_runs_remained_active": query_runs_unchanged,
        "streaming_container_did_not_restart": (
            streaming_after["restart_count"] == streaming_before["restart_count"]
        ),
        "postgres_business_key_duplicates_zero": duplicates == 0,
        "postgres_rows_increased": db_after > db_before,
        "worker_running_after_recovery": worker_after["running"],
        "one_worker_alive_after_recovery": master_after["alive_workers"] == 1,
    }
    passed = all(assertions.values())
    failure_reasons = [
        name for name, value in assertions.items() if not value
    ]

    result = {
        "test": {
            "name": "Spark Worker forced termination and executor recovery",
            "target_eps": args.eps,
            "load_duration_seconds": args.duration,
            "fault_after_seconds": args.fault_after,
            "outage_seconds": args.outage_seconds,
            "recovery_timeout_seconds": args.recovery_timeout,
            "started_at": test_started.isoformat(),
            "fault_at": fault_at.isoformat(),
            "worker_restart_observed_at": (
                restart_observed_at.isoformat() if restart_observed_at else None
            ),
            "recovery_started_at": recovery_started_at.isoformat(),
            "executor_reallocated_at": (
                executor_reallocated_at.isoformat()
                if executor_reallocated_at
                else None
            ),
            "finished_at": utc_now().isoformat(),
            "fault_command": (
                "docker stop --timeout 0 spark-worker; "
                f"wait {args.outage_seconds}s; docker start spark-worker"
            ),
            "stop_return_code": stop_result.returncode,
            "stop_output": (stop_result.stdout + stop_result.stderr).strip(),
            "start_return_code": start_result.returncode,
            "start_output": (start_result.stdout + start_result.stderr).strip(),
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
            "worker_before": worker_before,
            "worker_after_restart": worker_after_restart,
            "worker_after_test": worker_after,
            "streaming_before": streaming_before,
            "streaming_after": streaming_after,
            "master_before": master_before,
            "master_after": master_after,
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
            "duplicate_business_keys": duplicates,
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
                "kafka_offset_delta": offset_delta,
                "worker_restart_count": worker_after["restart_count"],
                "streaming_restart_count": streaming_after["restart_count"],
                "rto_seconds": result["spark"]["rto_seconds"],
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
