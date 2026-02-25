import argparse
import subprocess
import time
from kafka import KafkaConsumer, TopicPartition


def get_topic_total_offset(topic: str, bootstrap_servers: str) -> int:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers.split(","),
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
    )
    partitions = consumer.partitions_for_topic(topic) or set()
    if not partitions:
        consumer.close()
        return 0
    tps = [TopicPartition(topic, p) for p in partitions]
    offsets = consumer.end_offsets(tps)
    consumer.close()
    return int(sum(offsets.values()))


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure producer throughput using Kafka offsets delta")
    parser.add_argument("--topic", default="km.clicks.raw.v1")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument(
        "--producer-cmd",
        default="python producers/producer_clicks.py --anomaly-interval 20 --max-events 200000",
        help="Command to run the producer workload",
    )
    args = parser.parse_args()

    start_offset = get_topic_total_offset(args.topic, args.bootstrap)
    print(f"[INFO] start offset total={start_offset}")

    proc = subprocess.Popen(args.producer_cmd, shell=True)
    time.sleep(args.duration)
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()

    end_offset = get_topic_total_offset(args.topic, args.bootstrap)
    delta = max(0, end_offset - start_offset)
    rate = delta / args.duration if args.duration > 0 else 0
    print(f"[INFO] end offset total={end_offset}")
    print(f"[RESULT] produced={delta} events in {args.duration}s => {rate:.2f} evt/s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
