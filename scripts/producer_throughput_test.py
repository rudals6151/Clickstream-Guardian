import argparse
import hashlib
import json
import math
import random
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


ROOT_DIR = Path(__file__).resolve().parents[1]
CLICK_SCHEMA_PATH = ROOT_DIR / "schemas" / "click-event.avsc"
PURCHASE_SCHEMA_PATH = ROOT_DIR / "schemas" / "purchase-event.avsc"


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


@dataclass
class EventStats:
    target_eps: int
    attempted: int = 0
    delivered: int = 0
    failed: int = 0
    latencies_ms: deque[float] = field(
        default_factory=lambda: deque(maxlen=1_000_000)
    )

    def result(self, duration: float, minimum_target_ratio: float) -> dict[str, Any]:
        success_rate = self.delivered / self.attempted * 100 if self.attempted else 0
        actual_eps = self.delivered / duration if duration else 0
        target_ratio = actual_eps / self.target_eps if self.target_eps else 1
        return {
            "target_eps": self.target_eps,
            "actual_eps": round(actual_eps, 2),
            "target_achievement_pct": round(target_ratio * 100, 2),
            "attempted": self.attempted,
            "delivered": self.delivered,
            "failed": self.failed,
            "success_rate_pct": round(success_rate, 4),
            "passed": self.target_eps == 0
            or (self.failed == 0 and target_ratio >= minimum_target_ratio),
            "latency_ms": {
                "p50": round(percentile(self.latencies_ms, 50), 2),
                "p95": round(percentile(self.latencies_ms, 95), 2),
                "p99": round(percentile(self.latencies_ms, 99), 2),
            },
        }


class EpsLoadTest:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.producer = Producer(
            {
                "bootstrap.servers": args.bootstrap,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": args.compression,
                "linger.ms": args.linger_ms,
                "batch.size": args.batch_size,
                "max.in.flight.requests.per.connection": 5,
                "retries": 3,
                "retry.backoff.ms": 100,
            }
        )
        registry = SchemaRegistryClient({"url": args.schema_registry})
        self.serializers = {
            "click": AvroSerializer(registry, CLICK_SCHEMA_PATH.read_text(encoding="utf-8")),
            "purchase": AvroSerializer(registry, PURCHASE_SCHEMA_PATH.read_text(encoding="utf-8")),
        }
        self.topics = {
            "click": args.click_topic,
            "purchase": args.purchase_topic,
        }
        self.stats = {
            "click": EventStats(args.click_eps),
            "purchase": EventStats(args.purchase_eps),
        }
        self.sequence = 0
        self.last_click_event = None
        self.profile_counts = {
            "normal": 0,
            "anomaly": 0,
            "duplicate": 0,
            "late": 0,
        }

    def make_event(self, event_type: str) -> dict[str, Any]:
        self.sequence += 1
        now_ms = int(time.time() * 1000)

        if (
            event_type == "click"
            and self.last_click_event is not None
            and random.random() < self.args.duplicate_ratio
        ):
            self.profile_counts["duplicate"] += 1
            return dict(self.last_click_event)

        is_anomaly = (
            event_type == "click"
            and random.random() < self.args.anomaly_ratio
        )
        if is_anomaly:
            session_id = (
                self.args.session_id_base
                + self.args.anomaly_session_offset
                + (self.sequence % self.args.anomaly_session_pool)
            )
            item_id = 1 + (self.sequence % self.args.anomaly_item_pool)
            self.profile_counts["anomaly"] += 1
        else:
            session_id = self.args.session_id_base + (
                (self.sequence - 1) // self.args.events_per_session
            )
            item_id = random.randint(1, 100_000)
            self.profile_counts["normal"] += 1

        event_ts = now_ms
        if event_type == "click" and random.random() < self.args.late_ratio:
            event_ts -= self.args.late_minutes * 60 * 1000
            self.profile_counts["late"] += 1

        event_id = hashlib.sha256(
            f"eps-test|{event_type}|{self.sequence}|{now_ms}".encode("utf-8")
        ).hexdigest()

        common = {
            "session_id": session_id,
            "event_ts": event_ts,
            "item_id": item_id,
            "event_type": event_type,
            "event_id": event_id,
            "ingest_ts": now_ms,
            "source": "producer_throughput_test",
        }
        if event_type == "click":
            event = {
                **common,
                "category": "S" if is_anomaly else random.choice(["1", "2", "3", "S", None]),
            }
            self.last_click_event = dict(event)
            return event
        return {
            **common,
            "price": round(random.uniform(1, 500), 2),
            "quantity": random.randint(1, 5),
        }

    def delivery_callback(self, event_type: str, started_at: float):
        def callback(error, _message):
            stats = self.stats[event_type]
            if error is not None:
                stats.failed += 1
                return
            stats.delivered += 1
            stats.latencies_ms.append((time.perf_counter() - started_at) * 1000)

        return callback

    def send(self, event_type: str) -> None:
        topic = self.topics[event_type]
        event = self.make_event(event_type)
        payload = self.serializers[event_type](
            event,
            SerializationContext(topic, MessageField.VALUE),
        )
        stats = self.stats[event_type]
        stats.attempted += 1
        started_at = time.perf_counter()

        while True:
            try:
                self.producer.produce(
                    topic=topic,
                    key=str(event["session_id"]).encode("utf-8"),
                    value=payload,
                    on_delivery=self.delivery_callback(event_type, started_at),
                )
                return
            except BufferError:
                self.producer.poll(0.01)

    def warm_up(self, event_types: list[str]) -> None:
        print("[WARMUP] Loading metadata and sending one event per active topic")
        self.producer.list_topics(timeout=10)
        for event_type in event_types:
            topic = self.topics[event_type]
            event = self.make_event(event_type)
            event["source"] = "producer_throughput_test_warmup"
            payload = self.serializers[event_type](
                event,
                SerializationContext(topic, MessageField.VALUE),
            )
            self.producer.produce(
                topic=topic,
                key=str(event["session_id"]).encode("utf-8"),
                value=payload,
            )
        remaining = self.producer.flush(self.args.flush_timeout)
        if remaining:
            raise RuntimeError(f"Warm-up failed: {remaining} message(s) were not delivered")

    def run(self) -> dict[str, Any]:
        rates = {
            "click": self.args.click_eps,
            "purchase": self.args.purchase_eps,
        }
        intervals = {
            event_type: 1 / rate
            for event_type, rate in rates.items()
            if rate > 0
        }
        self.warm_up(list(intervals))

        started_at = time.perf_counter()
        deadline = started_at + self.args.duration
        next_send = {event_type: started_at for event_type in intervals}
        next_progress = started_at + self.args.progress_interval

        print(
            f"[START] duration={self.args.duration}s "
            f"click={self.args.click_eps} EPS purchase={self.args.purchase_eps} EPS"
        )

        while True:
            now = time.perf_counter()
            if now >= deadline:
                break

            sent = False
            for event_type, interval in intervals.items():
                while next_send[event_type] <= now and next_send[event_type] < deadline:
                    self.send(event_type)
                    next_send[event_type] += interval
                    sent = True

            self.producer.poll(0)

            if now >= next_progress:
                elapsed = now - started_at
                click_actual = self.stats["click"].delivered / elapsed
                purchase_actual = self.stats["purchase"].delivered / elapsed
                print(
                    f"[PROGRESS] {elapsed:.0f}s "
                    f"click={click_actual:.1f} EPS purchase={purchase_actual:.1f} EPS"
                )
                next_progress += self.args.progress_interval

            if not sent:
                next_due = min(next_send.values(), default=deadline)
                time.sleep(max(0, min(next_due - time.perf_counter(), 0.001)))

        remaining = self.producer.flush(self.args.flush_timeout)
        if remaining:
            print(f"[WARN] {remaining} message(s) were not delivered before flush timeout")
            for stats in self.stats.values():
                undelivered = stats.attempted - stats.delivered - stats.failed
                stats.failed += max(0, undelivered)

        duration = self.args.duration
        results = {
            "configuration": {
                "duration_seconds": duration,
                "bootstrap_servers": self.args.bootstrap,
                "schema_registry_url": self.args.schema_registry,
                "acks": "all",
                "idempotence": True,
                "compression": self.args.compression,
                "linger_ms": self.args.linger_ms,
                "batch_size": self.args.batch_size,
                "event_profile": {
                    "anomaly_ratio": self.args.anomaly_ratio,
                    "anomaly_session_pool": self.args.anomaly_session_pool,
                    "anomaly_item_pool": self.args.anomaly_item_pool,
                    "duplicate_ratio": self.args.duplicate_ratio,
                    "late_ratio": self.args.late_ratio,
                    "late_minutes": self.args.late_minutes,
                },
            },
            "click": self.stats["click"].result(
                duration, self.args.minimum_target_ratio
            ),
            "purchase": self.stats["purchase"].result(
                duration, self.args.minimum_target_ratio
            ),
        }
        results["combined"] = {
            "target_eps": self.args.click_eps + self.args.purchase_eps,
            "actual_eps": round(
                (self.stats["click"].delivered + self.stats["purchase"].delivered)
                / duration,
                2,
            ),
            "attempted": self.stats["click"].attempted + self.stats["purchase"].attempted,
            "delivered": self.stats["click"].delivered + self.stats["purchase"].delivered,
            "failed": self.stats["click"].failed + self.stats["purchase"].failed,
        }
        results["generated_profiles"] = self.profile_counts
        results["passed"] = results["click"]["passed"] and results["purchase"]["passed"]
        return results


def print_results(results: dict[str, Any]) -> None:
    print("\n[RESULT]")
    print(
        f"{'type':<10} {'target':>10} {'actual':>10} {'delivered':>12} "
        f"{'failed':>8} {'success':>10} {'p50':>10} {'p95':>10} {'p99':>10} {'status':>8}"
    )
    for event_type in ("click", "purchase"):
        row = results[event_type]
        latency = row["latency_ms"]
        print(
            f"{event_type:<10} {row['target_eps']:>10.2f} {row['actual_eps']:>10.2f} "
            f"{row['delivered']:>12} {row['failed']:>8} "
            f"{row['success_rate_pct']:>9.2f}% {latency['p50']:>9.2f}ms "
            f"{latency['p95']:>9.2f}ms {latency['p99']:>9.2f}ms "
            f"{'PASS' if row['passed'] else 'FAIL':>8}"
        )
    combined = results["combined"]
    print(
        f"{'combined':<10} {combined['target_eps']:>10.2f} "
        f"{combined['actual_eps']:>10.2f} {combined['delivered']:>12} "
        f"{combined['failed']:>8}"
    )
    print(f"\n[STATUS] {'PASS' if results['passed'] else 'FAIL'}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send synthetic click and purchase events at controlled EPS"
    )
    parser.add_argument("--click-eps", type=int, required=True)
    parser.add_argument("--purchase-eps", type=int, required=True)
    parser.add_argument("--duration", type=int, required=True)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--schema-registry", default="http://localhost:8081")
    parser.add_argument("--click-topic", default="km.clicks.raw.v1")
    parser.add_argument("--purchase-topic", default="km.purchases.raw.v1")
    parser.add_argument("--compression", default="lz4")
    parser.add_argument("--linger-ms", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=32768)
    parser.add_argument(
        "--events-per-session",
        type=int,
        default=20,
        help="Number of sequential events assigned to one session (default: 20)",
    )
    parser.add_argument(
        "--session-id-base",
        type=int,
        default=10_000_000,
        help="First synthetic session id used by the test",
    )
    parser.add_argument("--anomaly-ratio", type=float, default=0.10)
    parser.add_argument("--anomaly-session-pool", type=int, default=4)
    parser.add_argument("--anomaly-session-offset", type=int, default=1_000_000_000)
    parser.add_argument("--anomaly-item-pool", type=int, default=3)
    parser.add_argument("--duplicate-ratio", type=float, default=0.001)
    parser.add_argument("--late-ratio", type=float, default=0.001)
    parser.add_argument("--late-minutes", type=int, default=11)
    parser.add_argument("--flush-timeout", type=int, default=30)
    parser.add_argument("--progress-interval", type=int, default=10)
    parser.add_argument(
        "--minimum-target-ratio",
        type=float,
        default=0.95,
        help="Minimum actual/target EPS ratio required to pass (default: 0.95)",
    )
    parser.add_argument("--output", help="Optional JSON result path")
    args = parser.parse_args()
    if args.click_eps < 0 or args.purchase_eps < 0:
        parser.error("EPS values must be zero or greater")
    if args.click_eps == 0 and args.purchase_eps == 0:
        parser.error("At least one EPS value must be greater than zero")
    if args.duration <= 0:
        parser.error("duration must be greater than zero")
    if args.events_per_session <= 0:
        parser.error("events-per-session must be greater than zero")
    if args.anomaly_session_pool <= 0 or args.anomaly_item_pool <= 0:
        parser.error("anomaly pools must be greater than zero")
    for name in ("anomaly_ratio", "duplicate_ratio", "late_ratio"):
        value = getattr(args, name)
        if not 0 <= value < 1:
            parser.error(f"{name.replace('_', '-')} must be at least 0 and less than 1")
    if args.late_minutes <= 0:
        parser.error("late-minutes must be greater than zero")
    if not 0 < args.minimum_target_ratio <= 1:
        parser.error("minimum-target-ratio must be greater than 0 and at most 1")
    return args


def main() -> int:
    args = parse_args()
    test = EpsLoadTest(args)
    results = test.run()
    print_results(results)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(results, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"\n[INFO] JSON result written to {output_path}")

    return 0 if results["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
