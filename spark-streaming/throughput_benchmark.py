"""Spark Structured Streaming throughput benchmark for click events."""

import argparse
import json
import logging
import os
import time
from pathlib import Path
from threading import Lock
from urllib import request

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, length, sha2, concat_ws, trim, when
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import TimestampType

from common.kafka_utils import get_kafka_options


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_click_avro_schema():
    registry_url = os.getenv(
        "SCHEMA_REGISTRY_URL", "http://schema-registry:8081"
    ).rstrip("/")
    subject = os.getenv("CLICK_SCHEMA_SUBJECT", "km.clicks.raw.v1-value")
    try:
        req = request.Request(
            f"{registry_url}/subjects/{subject}/versions/latest",
            headers={"Accept": "application/vnd.schemaregistry.v1+json"},
        )
        with request.urlopen(req, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload["schema"], f"registry:{subject}"
    except Exception as exc:
        logger.warning("Schema Registry lookup failed, using local schema: %s", exc)

    schema_path = Path(
        os.getenv("CLICK_AVRO_SCHEMA_PATH", "/opt/schemas/click-event.avsc")
    )
    return schema_path.read_text(encoding="utf-8"), f"file:{schema_path}"


class JsonProgressListener(StreamingQueryListener):
    def __init__(self, output_path):
        super().__init__()
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = Lock()

    def _append(self, payload):
        with self.lock:
            with self.output_path.open("a", encoding="utf-8") as output:
                output.write(json.dumps(payload, ensure_ascii=True) + "\n")

    def onQueryStarted(self, event):
        self._append(
            {
                "event": "query_started",
                "id": str(event.id),
                "run_id": str(event.runId),
                "name": event.name,
                "timestamp": event.timestamp,
            }
        )

    def onQueryProgress(self, event):
        payload = json.loads(event.progress.json)
        payload["event"] = "query_progress"
        self._append(payload)

    def onQueryTerminated(self, event):
        self._append(
            {
                "event": "query_terminated",
                "id": str(event.id),
                "run_id": str(event.runId),
                "exception": event.exception,
                "error_class": event.errorClassOnException,
            }
        )

    def onQueryIdle(self, event):
        return None


def append_json_line(path, payload):
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(payload, ensure_ascii=True) + "\n")


def parse_clicks(kafka_df, avro_schema, deduplicate):
    parsed = kafka_df.select(
        from_avro(
            expr("substring(value, 6, length(value))"),
            avro_schema,
        ).alias("data"),
        col("timestamp").alias("kafka_ingest_ts"),
    ).select("data.*", "kafka_ingest_ts")

    if not isinstance(parsed.schema["event_ts"].dataType, TimestampType):
        parsed = parsed.withColumn(
            "event_ts", (col("event_ts") / 1000).cast("timestamp")
        )

    parsed = parsed.withColumn(
        "event_id",
        when(
            col("event_id").isNull() | (length(trim(col("event_id"))) == 0),
            sha2(
                concat_ws(
                    "|",
                    col("session_id").cast("string"),
                    col("item_id").cast("string"),
                    col("event_ts").cast("string"),
                    col("event_type"),
                ),
                256,
            ),
        ).otherwise(col("event_id")),
    )

    parsed = parsed.filter(col("session_id").isNotNull())
    if deduplicate:
        return (
            parsed.withWatermark("event_ts", "10 minutes")
            .dropDuplicates(["event_id"])
        )
    return parsed


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--kafka-servers",
        default="kafka-1:29092,kafka-2:29093,kafka-3:29094",
    )
    parser.add_argument("--topic", default="km.spark.throughput.test.v1")
    parser.add_argument(
        "--checkpoint",
        default="/opt/spark-checkpoint/throughput-benchmark",
    )
    parser.add_argument(
        "--progress-output",
        default="/opt/test-results/02_spark_throughput_progress.jsonl",
    )
    parser.add_argument(
        "--batch-output",
        default="/opt/test-results/02_spark_throughput_batches.jsonl",
    )
    parser.add_argument("--trigger-seconds", type=int, default=5)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=1_000_000)
    parser.add_argument("--shuffle-partitions", type=int, default=3)
    parser.add_argument(
        "--deduplicate",
        action="store_true",
        help="Include the production watermark and event_id deduplication state",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    schema, schema_source = load_click_avro_schema()
    spark = (
        SparkSession.builder.appName("SparkThroughputBenchmark")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage", "true")
        .config("spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB", "512")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(JsonProgressListener(args.progress_output))

    options = get_kafka_options(
        args.kafka_servers,
        args.topic,
        starting_offsets="latest",
        max_offsets_per_trigger=args.max_offsets_per_trigger,
    )
    kafka_df = spark.readStream.format("kafka").options(**options).load()
    clicks_df = parse_clicks(kafka_df, schema, args.deduplicate)

    logger.info(
        "Starting benchmark topic=%s trigger=%ss maxOffsets=%s deduplicate=%s schema=%s",
        args.topic,
        args.trigger_seconds,
        args.max_offsets_per_trigger,
        args.deduplicate,
        schema_source,
    )

    def consume_batch(batch_df, batch_id):
        started_at = time.perf_counter()
        row_count = batch_df.count()
        append_json_line(
            args.batch_output,
            {
                "batch_id": batch_id,
                "rows": row_count,
                "sink_action_ms": round(
                    (time.perf_counter() - started_at) * 1000, 2
                ),
                "recorded_at_epoch_ms": int(time.time() * 1000),
            },
        )

    query = (
        clicks_df.writeStream.foreachBatch(consume_batch)
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
