import argparse
import json
import math
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PROGRESS = ROOT_DIR / "docs" / "testing" / "results" / "pipeline_capacity_progress.jsonl"
RESULTS_DIR = ROOT_DIR / "docs" / "testing" / "results"
MONITORED_CONTAINERS = [
    "spark-streaming-anomaly",
    "spark-worker",
    "kafka-1",
    "kafka-2",
    "kafka-3",
    "postgres",
]


def percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * percent / 100
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def run(command: list[str], timeout: int = 30, check: bool = True) -> subprocess.CompletedProcess:
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


def postgres_count() -> int:
    command = [
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
        "SELECT COUNT(*) FROM anomaly_sessions;",
    ]
    last_error = None
    for attempt in range(3):
        try:
            result = run(command, timeout=60)
            return int(result.stdout.strip())
        except (subprocess.SubprocessError, ValueError) as error:
            last_error = error
            if attempt < 2:
                time.sleep(5)
    raise RuntimeError(f"PostgreSQL count failed after 3 attempts: {last_error}")


def restart_counts() -> dict[str, int]:
    result = {}
    for container in ("spark-streaming-anomaly", "spark-worker"):
        inspected = run(
            ["docker", "inspect", "--format", "{{.RestartCount}}", container]
        )
        result[container] = int(inspected.stdout.strip())
    return result


def parse_percent(value: str) -> float:
    return float(value.strip().rstrip("%"))


def parse_memory_mib(value: str) -> float:
    raw = value.split("/")[0].strip()
    number = float(raw[:-3])
    unit = raw[-3:].lower()
    factors = {"kib": 1 / 1024, "mib": 1, "gib": 1024}
    return number * factors[unit]


def sample_resources(stop_event: threading.Event, samples: list[dict[str, Any]]) -> None:
    while not stop_event.is_set():
        try:
            completed = run(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}",
                    *MONITORED_CONTAINERS,
                ],
                timeout=20,
            )
            timestamp = datetime.now(timezone.utc).isoformat()
            for line in completed.stdout.splitlines():
                name, cpu, memory = line.split("|", 2)
                samples.append(
                    {
                        "timestamp": timestamp,
                        "container": name,
                        "cpu_pct": parse_percent(cpu),
                        "memory_mib": round(parse_memory_mib(memory), 2),
                    }
                )
        except Exception as error:
            samples.append(
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "error": str(error),
                }
            )
        stop_event.wait(5)


def read_progress(path: Path, start_offset: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("rb") as stream:
        stream.seek(start_offset)
        data = stream.read().decode("utf-8", errors="replace")
    records = []
    for line in data.splitlines():
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def summarize_queries(records: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {}
    for name in ("late_events", "high_frequency", "bot_like"):
        progress = [
            record
            for record in records
            if record.get("event") == "query_progress" and record.get("name") == name
        ]
        trigger_ms = [
            float(record.get("durationMs", {}).get("triggerExecution", 0))
            for record in progress
        ]
        lags = [
            int(record.get("sources", [{}])[0].get("metrics", {}).get("maxOffsetsBehindLatest", 0))
            for record in progress
            if record.get("sources")
        ]
        state_rows = [
            sum(int(operator.get("numRowsTotal", 0)) for operator in record.get("stateOperators", []))
            for record in progress
        ]
        summary[name] = {
            "batches": len(progress),
            "input_rows": sum(int(record.get("numInputRows", 0)) for record in progress),
            "processed_rows_per_second_max": round(
                max((float(record.get("processedRowsPerSecond", 0)) for record in progress), default=0),
                2,
            ),
            "trigger_ms_p95": round(percentile(trigger_ms, 95), 2),
            "trigger_ms_max": round(max(trigger_ms, default=0), 2),
            "lag_max": max(lags, default=0),
            "lag_end": lags[-1] if lags else 0,
            "state_rows_end": state_rows[-1] if state_rows else 0,
        }
    return summary


def resource_summary(samples: list[dict[str, Any]]) -> dict[str, Any]:
    result = {}
    for container in MONITORED_CONTAINERS:
        rows = [sample for sample in samples if sample.get("container") == container]
        result[container] = {
            "samples": len(rows),
            "cpu_pct_max": round(max((row["cpu_pct"] for row in rows), default=0), 2),
            "cpu_pct_avg": round(
                sum(row["cpu_pct"] for row in rows) / len(rows) if rows else 0,
                2,
            ),
            "memory_mib_max": round(max((row["memory_mib"] for row in rows), default=0), 2),
        }
    return result


def evaluate(
    producer: dict[str, Any],
    queries: dict[str, Any],
    restarts_before: dict[str, int],
    restarts_after: dict[str, int],
    db_delta: int,
    records: list[dict[str, Any]],
) -> tuple[bool, list[str]]:
    reasons = []
    if not producer.get("passed"):
        reasons.append("producer did not achieve at least 95% of target EPS")
    producer_p95 = float(producer.get("click", {}).get("latency_ms", {}).get("p95", 0))
    if producer_p95 > 1000:
        reasons.append(f"producer delivery latency p95 was {producer_p95:.2f} ms")
    if any(record.get("event") == "query_terminated" for record in records):
        reasons.append("a Spark streaming query terminated")
    for name in ("late_events", "high_frequency", "bot_like"):
        query = queries[name]
        if query["batches"] == 0:
            reasons.append(f"{name} produced no progress record")
        if query["lag_end"] > 0:
            reasons.append(f"{name} ended with Kafka lag {query['lag_end']}")
        if name != "late_events" and query["trigger_ms_p95"] > 5000:
            reasons.append(
                f"{name} trigger p95 was {query['trigger_ms_p95']:.2f} ms (> 5000 ms)"
            )
    for container, before in restarts_before.items():
        if restarts_after.get(container, before) > before:
            reasons.append(f"{container} restarted")
    if db_delta <= 0:
        reasons.append("no anomaly row was written to PostgreSQL")
    return not reasons, reasons


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one 60-second pipeline capacity stage")
    parser.add_argument("--eps", type=int, required=True)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--drain-seconds", type=int, default=15)
    parser.add_argument("--progress-file", type=Path, default=DEFAULT_PROGRESS)
    parser.add_argument("--session-id-base", type=int, default=20_000_000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    progress_offset = args.progress_file.stat().st_size if args.progress_file.exists() else 0
    db_before = postgres_count()
    restarts_before = restart_counts()
    stage_started = datetime.now(timezone.utc)
    result_path = RESULTS_DIR / f"capacity_{args.eps:06d}_eps.json"
    producer_path = RESULTS_DIR / f"capacity_{args.eps:06d}_eps_producer.json"

    resource_samples: list[dict[str, Any]] = []
    stop_event = threading.Event()
    monitor = threading.Thread(
        target=sample_resources,
        args=(stop_event, resource_samples),
        daemon=True,
    )
    monitor.start()

    command = [
        sys.executable,
        str(ROOT_DIR / "scripts" / "producer_throughput_test.py"),
        "--click-eps",
        str(args.eps),
        "--purchase-eps",
        "0",
        "--duration",
        str(args.duration),
        "--session-id-base",
        str(args.session_id_base + args.eps * 1000),
        "--output",
        str(producer_path),
    ]
    print(f"[STAGE] {args.eps} EPS for {args.duration} seconds")
    producer_process = subprocess.run(
        command,
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    print(producer_process.stdout)
    if producer_process.stderr:
        print(producer_process.stderr, file=sys.stderr)

    print(f"[DRAIN] observing recovery for {args.drain_seconds} seconds")
    time.sleep(args.drain_seconds)
    stop_event.set()
    monitor.join(timeout=10)

    producer = (
        json.loads(producer_path.read_text(encoding="utf-8"))
        if producer_path.exists()
        else {"passed": False, "error": "producer result file missing"}
    )
    records = read_progress(args.progress_file, progress_offset)
    queries = summarize_queries(records)
    db_after = postgres_count()
    restarts_after = restart_counts()
    passed, reasons = evaluate(
        producer,
        queries,
        restarts_before,
        restarts_after,
        db_after - db_before,
        records,
    )
    result = {
        "stage": {
            "target_eps": args.eps,
            "duration_seconds": args.duration,
            "drain_seconds": args.drain_seconds,
            "started_at": stage_started.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        },
        "producer": producer,
        "spark_queries": queries,
        "resources": resource_summary(resource_samples),
        "postgres": {
            "rows_before": db_before,
            "rows_after": db_after,
            "row_delta": db_after - db_before,
        },
        "restart_counts": {
            "before": restarts_before,
            "after": restarts_after,
        },
        "passed": passed,
        "failure_reasons": reasons,
    }
    result_path.write_text(
        json.dumps(result, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "target_eps": args.eps,
                "actual_eps": producer.get("combined", {}).get("actual_eps", 0),
                "query_summary": queries,
                "postgres_delta": db_after - db_before,
                "passed": passed,
                "failure_reasons": reasons,
                "result": str(result_path),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
