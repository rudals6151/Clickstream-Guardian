import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

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
TOPIC = "km.clicks.raw.v1"


def topic_state() -> dict:
    result = run(
        [
            "docker",
            "exec",
            "kafka-1",
            "kafka-topics",
            "--bootstrap-server",
            "kafka-1:29092",
            "--describe",
            "--topic",
            TOPIC,
        ],
        timeout=60,
    )
    partitions = {}
    for line in result.stdout.splitlines():
        if "\tPartition:" not in line:
            continue
        fields = {}
        for field in line.strip().split("\t"):
            if ": " in field:
                key, value = field.split(": ", 1)
                fields[key] = value
        partition = str(fields["Partition"])
        replicas = [int(value) for value in fields["Replicas"].split(",")]
        isr = [int(value) for value in fields["Isr"].split(",") if value]
        partitions[partition] = {
            "leader": int(fields["Leader"]),
            "replicas": replicas,
            "isr": isr,
            "under_replicated": len(isr) < len(replicas),
        }
    return {
        "timestamp": utc_now().isoformat(),
        "partitions": partitions,
        "all_replicas_in_sync": bool(partitions)
        and all(not value["under_replicated"] for value in partitions.values()),
        "raw": result.stdout.strip(),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test one Kafka broker outage")
    parser.add_argument("--eps", type=int, default=12000)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--fault-after", type=int, default=20)
    parser.add_argument("--outage-seconds", type=int, default=15)
    parser.add_argument("--recovery-timeout", type=int, default=300)
    parser.add_argument("--broker", default="kafka-3")
    parser.add_argument("--session-id-base", type=int, default=160_000_000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    progress_file = RESULTS_DIR / "pipeline_capacity_progress.jsonl"
    progress_offset = progress_file.stat().st_size if progress_file.exists() else 0
    result_path = RESULTS_DIR / "02_03_kafka_broker_failure.json"
    producer_path = RESULTS_DIR / "02_03_kafka_broker_failure_producer.json"

    broker_before = inspect_container(args.broker)
    topic_before = topic_state()
    if not broker_before["running"] or not topic_before["all_replicas_in_sync"]:
        raise RuntimeError("Kafka cluster is not ready")

    streaming_before = inspect_container("spark-streaming-anomaly")
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
        ["docker", "stop", "--timeout", "0", args.broker],
        timeout=60,
        check=False,
    )

    degraded_state = None
    degraded_observation_seconds = max(1, args.outage_seconds - 3)
    degraded_deadline = time.monotonic() + degraded_observation_seconds
    while time.monotonic() < degraded_deadline:
        try:
            state = topic_state()
            if not state["all_replicas_in_sync"]:
                degraded_state = state
                break
        except (subprocess.SubprocessError, ValueError):
            pass
        time.sleep(1)

    remaining_outage = max(
        0,
        args.outage_seconds - (utc_now() - fault_at).total_seconds(),
    )
    time.sleep(remaining_outage)
    recovery_started_at = utc_now()
    start_result = run(
        ["docker", "start", args.broker],
        timeout=60,
        check=False,
    )

    broker_started_at = None
    isr_recovered_at = None
    topic_after_recovery = None
    recovery_deadline = time.monotonic() + args.recovery_timeout
    while time.monotonic() < recovery_deadline:
        broker_state = inspect_container(args.broker)
        if (
            broker_state["running"]
            and broker_state["started_at"] != broker_before["started_at"]
            and broker_started_at is None
        ):
            broker_started_at = utc_now()
        try:
            state = topic_state()
            if state["all_replicas_in_sync"]:
                isr_recovered_at = utc_now()
                topic_after_recovery = state
                break
        except (subprocess.SubprocessError, ValueError):
            pass
        time.sleep(1)

    producer_stdout, producer_stderr = producer.communicate(
        timeout=args.duration + 60
    )
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
    recovered_times = [
        value
        for value in (query_recovered_at, isr_recovered_at)
        if value is not None
    ]
    fully_recovered_at = (
        max(recovered_times)
        if len(recovered_times) == 2
        else None
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
    broker_after = inspect_container(args.broker)
    streaming_after = inspect_container("spark-streaming-anomaly")
    topic_final = topic_state()
    query_offsets_match = all(
        query["logical_offset_span"] == expected_records
        and query["offsets_contiguous"]
        for query in progress_summary.get("queries", {}).values()
    )
    query_runs_unchanged = all(
        len(query["run_ids"]) == 1
        for query in progress_summary.get("queries", {}).values()
    )
    failover_state = degraded_state or topic_after_recovery or topic_final
    leader_changed_during_outage = bool(failover_state) and any(
        failover_state["partitions"][partition]["leader"]
        != topic_before["partitions"][partition]["leader"]
        for partition in topic_before["partitions"]
    )

    assertions = {
        "producer_failures_zero": producer_result["click"]["failed"] == 0,
        "producer_achieved_target": producer_result["click"]["passed"],
        "kafka_offset_delta_matches_delivered": offset_delta == expected_records,
        "broker_outage_observed_in_isr": degraded_state is not None,
        "leader_failover_observed": leader_changed_during_outage,
        "broker_restarted": broker_started_at is not None,
        "full_isr_recovered": isr_recovered_at is not None,
        "all_queries_reached_final_offsets": query_recovered_at is not None,
        "each_query_offsets_cover_delivered_without_gap": query_offsets_match,
        "query_runs_remained_active": query_runs_unchanged,
        "streaming_container_did_not_restart": (
            streaming_after["restart_count"] == streaming_before["restart_count"]
        ),
        "postgres_business_key_duplicates_zero": duplicates == 0,
        "postgres_rows_increased": db_after > db_before,
        "broker_running_after_recovery": broker_after["running"],
        "topic_fully_replicated_after_recovery": (
            topic_final["all_replicas_in_sync"]
        ),
    }
    passed = all(assertions.values())
    failure_reasons = [
        name for name, value in assertions.items() if not value
    ]

    result = {
        "test": {
            "name": "Kafka broker outage, leader failover, and ISR recovery",
            "broker": args.broker,
            "target_eps": args.eps,
            "load_duration_seconds": args.duration,
            "fault_after_seconds": args.fault_after,
            "outage_seconds": args.outage_seconds,
            "started_at": test_started.isoformat(),
            "fault_at": fault_at.isoformat(),
            "recovery_started_at": recovery_started_at.isoformat(),
            "broker_started_at": (
                broker_started_at.isoformat() if broker_started_at else None
            ),
            "isr_recovered_at": (
                isr_recovered_at.isoformat() if isr_recovered_at else None
            ),
            "fully_recovered_at": (
                fully_recovered_at.isoformat()
                if fully_recovered_at
                else None
            ),
            "finished_at": utc_now().isoformat(),
            "fault_command": (
                f"docker stop --timeout 0 {args.broker}; "
                f"wait {args.outage_seconds}s; docker start {args.broker}"
            ),
            "stop_return_code": stop_result.returncode,
            "start_return_code": start_result.returncode,
        },
        "producer": producer_result,
        "producer_stdout": producer_stdout,
        "producer_stderr": producer_stderr,
        "kafka": {
            "min_insync_replicas": 1,
            "min_insync_replicas_source": (
                "Kafka broker default; no dynamic topic or broker override"
            ),
            "offsets_before": kafka_before,
            "offsets_after": kafka_after,
            "offset_delta": offset_delta,
            "expected_records_including_warmup": expected_records,
            "broker_before": broker_before,
            "broker_after": broker_after,
            "topic_before": topic_before,
            "topic_degraded": degraded_state,
            "topic_after_recovery": topic_after_recovery,
            "topic_final": topic_final,
            "leader_changed_during_outage": leader_changed_during_outage,
            "rto_seconds": (
                round((fully_recovered_at - fault_at).total_seconds(), 3)
                if fully_recovered_at
                else None
            ),
        },
        "spark": {
            "streaming_before": streaming_before,
            "streaming_after": streaming_after,
            "progress": progress_summary,
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
                "failed": producer_result["click"]["failed"],
                "producer_p95_ms": producer_result["click"]["latency_ms"]["p95"],
                "kafka_offset_delta": offset_delta,
                "leader_failover": leader_changed_during_outage,
                "rto_seconds": result["kafka"]["rto_seconds"],
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
