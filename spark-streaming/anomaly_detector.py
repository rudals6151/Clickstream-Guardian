"""
Real-time Anomaly Detection using Spark Structured Streaming

This job monitors click events from Kafka and detects anomalous sessions
based on patterns like:
- High frequency clicks (potential bot)
- Repetitive behavior
- Unusual session duration
"""
import sys
import builtins
import logging
import os
import json
import time
from pathlib import Path
from threading import Lock
from urllib import request
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, lit, when, length, trim, sha2,
    concat_ws, window, min, max, current_timestamp,
    unix_timestamp, expr, to_json, struct,
    approx_count_distinct,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.streaming import StreamingQueryListener
import psycopg2
from psycopg2.extras import execute_values

from common.kafka_utils import get_click_schema, get_kafka_options
from common.postgres_utils import get_postgres_properties, append_to_postgres


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class JsonProgressListener(StreamingQueryListener):
    """Persist progress for every streaming query as JSON Lines."""

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
        self._append({
            "event": "query_started",
            "id": str(event.id),
            "run_id": str(event.runId),
            "name": event.name,
            "timestamp": event.timestamp,
        })

    def onQueryProgress(self, event):
        payload = json.loads(event.progress.json)
        payload["event"] = "query_progress"
        self._append(payload)

    def onQueryTerminated(self, event):
        self._append({
            "event": "query_terminated",
            "id": str(event.id),
            "run_id": str(event.runId),
            "exception": event.exception,
            "error_class": event.errorClassOnException,
        })

    def onQueryIdle(self, event):
        return None


def load_click_avro_schema():
    """Load click-event Avro schema from Schema Registry; fallback to file."""
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081").rstrip("/")
    subject = os.getenv("CLICK_SCHEMA_SUBJECT", "km.clicks.raw.v1-value")
    latest_schema_url = f"{schema_registry_url}/subjects/{subject}/versions/latest"

    try:
        req = request.Request(
            latest_schema_url,
            headers={"Accept": "application/vnd.schemaregistry.v1+json"},
        )
        with request.urlopen(req, timeout=5) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        schema_str = payload["schema"]
        schema_id = int(payload["id"])
        return schema_str, schema_id, f"registry:{subject}"
    except Exception as exc:
        logger.warning("Falling back to local schema file (registry lookup failed): %s", exc)

    configured_path = os.getenv("CLICK_AVRO_SCHEMA_PATH")
    if configured_path:
        schema_path = Path(configured_path)
    else:
        schema_path = Path(__file__).resolve().parents[1] / "schemas" / "click-event.avsc"
    return schema_path.read_text(encoding="utf-8"), None, f"file:{schema_path}"


class AnomalyDetector:
    """Real-time anomaly detection for clickstream data"""
    
    def __init__(self, kafka_servers="kafka-1:29092,kafka-2:29093,kafka-3:29094",
                 checkpoint_location="/opt/spark-checkpoint/anomaly",
                 click_topic="km.clicks.raw.v1",
                 max_offsets_per_trigger=250000,
                 progress_output="/opt/test-results/streaming_progress.jsonl"):
        self.kafka_servers = kafka_servers
        self.checkpoint_location = checkpoint_location
        self.click_topic = click_topic
        self.max_offsets_per_trigger = max_offsets_per_trigger
        self.progress_output = progress_output
        self.shuffle_partitions = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "2"))
        self.late_topic = os.getenv("LATE_EVENTS_TOPIC", "km.events.late.v1")
        self.event_time_policy_minutes = int(
            os.getenv("EVENT_TIME_POLICY_MINUTES", os.getenv("LATE_EVENT_THRESHOLD_MINUTES", "10"))
        )
        self.late_threshold_minutes = self.event_time_policy_minutes
        self.watermark_duration = f"{self.event_time_policy_minutes} minutes"
        (
            self.click_avro_schema,
            self.click_schema_id,
            self.click_schema_source,
        ) = load_click_avro_schema()
        
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("ClickstreamAnomalyDetector") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.streaming.kafka.consumer.cache.capacity", "1000") \
            .config("spark.sql.shuffle.partitions", str(self.shuffle_partitions)) \
            .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
            .config(
                "spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
            ) \
            .config("spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage", "true") \
            .config("spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB", "512") \
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
            .config("spark.sql.streaming.statefulOperator.allowMultiple", "false") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.spark.streams.addListener(JsonProgressListener(self.progress_output))
        logger.info("Spark session initialized")
        logger.info(
            "Click schema loaded from %s (schema_id=%s)",
            self.click_schema_source,
            self.click_schema_id if self.click_schema_id is not None else "n/a",
        )
        logger.info("Event time policy: %s", self.watermark_duration)
        logger.info("Max offsets per trigger: %s", self.max_offsets_per_trigger)
        logger.info("Shuffle partitions: %s", self.shuffle_partitions)
        logger.info("Streaming progress output: %s", self.progress_output)
        
        # Anomaly detection thresholds
        self.HIGH_FREQUENCY_THRESHOLD = 50  # clicks in 10 seconds
        self.BOT_LIKE_THRESHOLD = 50  # clicks in 1 minute
        self.LOW_DIVERSITY_RATIO = 0.1  # unique items / total clicks
    
    def read_kafka_stream(self):
        """Read click events from Kafka"""
        logger.info(f"Reading from Kafka: {self.kafka_servers}")
        
        kafka_options = get_kafka_options(
            bootstrap_servers=self.kafka_servers,
            topic=self.click_topic,
            starting_offsets="latest",
            max_offsets_per_trigger=self.max_offsets_per_trigger,
        )
        
        return self.spark \
            .readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()
    
    def parse_events(self, kafka_df):
        """Parse Avro messages from Kafka using from_avro"""
        
        # Import from_avro here to avoid initialization issues
        from pyspark.sql.avro.functions import from_avro as avro_from_avro
        
        # NOTE: Avro timestamp-millis logical type is read as long in Spark, requires manual conversion
        framed_df = kafka_df.select(
            col("value"),
            col("timestamp").alias("kafka_ingest_ts"),
        )

        # Confluent wire format: [magic byte][schema id(4 bytes)][avro payload]
        clicks_df = framed_df.select(
            avro_from_avro(
                expr("substring(value, 6, length(value))"),
                self.click_avro_schema
            ).alias("data"),
            col("kafka_ingest_ts")
        ).select("data.*", "kafka_ingest_ts")
        
        # Spark may decode Avro timestamp-millis as either timestamp or long,
        # depending on the schema source and Spark Avro behavior.
        if not isinstance(clicks_df.schema["event_ts"].dataType, TimestampType):
            clicks_df = clicks_df.withColumn(
                "event_ts",
                (col("event_ts") / 1000).cast("timestamp")
            )

        if isinstance(clicks_df.schema["ingest_ts"].dataType, TimestampType):
            clicks_df = clicks_df.withColumn(
                "ingest_ts",
                when(col("ingest_ts").isNotNull(), col("ingest_ts"))
                .otherwise(col("kafka_ingest_ts"))
            )
        else:
            clicks_df = clicks_df.withColumn(
                "ingest_ts",
                when(col("ingest_ts") > 0, (col("ingest_ts") / 1000).cast("timestamp"))
                .otherwise(col("kafka_ingest_ts"))
            )

        clicks_df = clicks_df.withColumn(
            "event_id",
            when(
                col("event_id").isNull() | (length(trim(col("event_id"))) == 0),
                sha2(
                    concat_ws(
                        "|",
                        col("session_id").cast("string"),
                        col("item_id").cast("string"),
                        col("event_ts").cast("string"),
                        col("event_type")
                    ),
                    256
                )
            ).otherwise(col("event_id"))
        )
        
        # Filter out null rows (deserialization failures)
        clicks_df = clicks_df.filter(col("session_id").isNotNull())
        
        # Sample output for debugging
        logger.info("Sample parsed events:")
        
        return clicks_df

    def split_late_events(self, clicks_df):
        """Split stream into on-time and too-late events."""
        threshold_expr = expr(f"current_timestamp() - interval {self.late_threshold_minutes} minutes")

        late_events = clicks_df.filter(col("event_ts") < threshold_expr)
        on_time_events = clicks_df.filter(col("event_ts") >= threshold_expr)
        return on_time_events, late_events

    def write_late_events_to_kafka(self, late_df):
        """Route late events to a dedicated Kafka late-events topic."""
        payload = late_df.select(
            col("event_id").cast("string").alias("key"),
            to_json(
                struct(
                    col("event_id"),
                    col("session_id"),
                    col("item_id"),
                    col("event_type"),
                    col("event_ts"),
                    col("kafka_ingest_ts"),
                    lit("too_late").alias("reason")
                )
            ).alias("value")
        )

        return payload.writeStream \
            .queryName("late_events") \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("topic", self.late_topic) \
            .option("checkpointLocation", f"{self.checkpoint_location}/late_events") \
            .outputMode("append") \
            .trigger(processingTime='10 seconds') \
            .start()
    
    def detect_high_frequency(self, clicks_df):
        """
        Detect high frequency clicks (10-second tumbling window)
        Identifies sessions with unusually high click rates
        """
        logger.info("Setting up high frequency detection...")
        
        # 10-second tumbling window aggregation
        high_frequency = clicks_df \
            .groupBy(
                window("event_ts", "10 seconds"),
                "session_id"
            ) \
            .agg(
                count("*").alias("click_count"),
                approx_count_distinct("item_id").alias("unique_items"),
                min("event_ts").alias("window_start"),
                max("event_ts").alias("window_end"),
                max("kafka_ingest_ts").alias("kafka_ingest_ts")
            ) \
            .filter(col("click_count") > self.HIGH_FREQUENCY_THRESHOLD) \
            .withColumn("anomaly_type", lit("HIGH_FREQUENCY")) \
            .withColumn(
                "anomaly_score",
                col("click_count") / lit(self.HIGH_FREQUENCY_THRESHOLD)
            ) \
            .withColumn("detected_at", col("window_end")) \
            .withColumn("spark_process_ts", current_timestamp()) \
            .withColumn(
                "event_id",
                sha2(
                    concat_ws(
                        "|",
                        col("session_id").cast("string"),
                        col("window_start").cast("string"),
                        lit("HIGH_FREQUENCY")
                    ),
                    256
                )
            ) \
            .withColumn("source", lit("spark_streaming")) \
            .withColumn("event_ts", (unix_timestamp(col("window_start")) * 1000).cast("long")) \
            .select(
                "event_id",
                "event_ts",
                "session_id",
                "detected_at",
                "window_start",
                "window_end",
                "click_count",
                "unique_items",
                "anomaly_score",
                "anomaly_type",
                "kafka_ingest_ts",
                "spark_process_ts",
                "source"
            )
        
        return high_frequency
    
    def detect_bot_like(self, clicks_df):
        """
        Detect bot-like behavior (1-minute sliding window)
        Identifies sessions with high click count and low item diversity
        """
        logger.info("Setting up bot-like detection...")
        
        # 1-minute sliding window (30 second slide)
        bot_like = clicks_df \
            .groupBy(
                window("event_ts", "1 minute", "30 seconds"),
                "session_id"
            ) \
            .agg(
                count("*").alias("click_count"),
                approx_count_distinct("item_id").alias("unique_items"),
                min("event_ts").alias("window_start"),
                max("event_ts").alias("window_end"),
                max("kafka_ingest_ts").alias("kafka_ingest_ts")
            ) \
            .withColumn(
                "diversity_ratio",
                col("unique_items") / col("click_count")
            ) \
            .filter(
                (col("click_count") > self.BOT_LIKE_THRESHOLD) &
                (col("diversity_ratio") < self.LOW_DIVERSITY_RATIO)
            ) \
            .withColumn("anomaly_type", lit("BOT_LIKE")) \
            .withColumn(
                "anomaly_score",
                (col("click_count") / lit(self.BOT_LIKE_THRESHOLD)) * 
                (1 - col("diversity_ratio"))
            ) \
            .withColumn("detected_at", current_timestamp()) \
            .withColumn("spark_process_ts", current_timestamp()) \
            .withColumn(
                "event_id",
                sha2(
                    concat_ws(
                        "|",
                        col("session_id").cast("string"),
                        col("window_start").cast("string"),
                        lit("BOT_LIKE")
                    ),
                    256
                )
            ) \
            .withColumn("source", lit("spark_streaming")) \
            .withColumn("event_ts", (unix_timestamp(col("window_start")) * 1000).cast("long")) \
            .select(
                "event_id",
                "event_ts",
                "session_id",
                "detected_at",
                "window_start",
                "window_end",
                "click_count",
                "unique_items",
                "anomaly_score",
                "anomaly_type",
                "kafka_ingest_ts",
                "spark_process_ts",
                "source"
            )
        
        return bot_like
    
    def write_to_postgres(self, df, query_name, output_mode="update"):
        """Write anomalies to PostgreSQL"""
        postgres_props = get_postgres_properties()
        jdbc_url = postgres_props["url"].replace("jdbc:", "")
        db_user = postgres_props["user"]
        db_password = postgres_props["password"]
        connect_timeout = int(os.getenv("POSTGRES_CONNECT_TIMEOUT_SECONDS", "3"))
        max_attempts = int(os.getenv("POSTGRES_WRITE_MAX_ATTEMPTS", "6"))
        initial_backoff = float(
            os.getenv("POSTGRES_WRITE_INITIAL_BACKOFF_SECONDS", "1")
        )
        max_backoff = float(
            os.getenv("POSTGRES_WRITE_MAX_BACKOFF_SECONDS", "8")
        )

        if connect_timeout < 1:
            raise ValueError("POSTGRES_CONNECT_TIMEOUT_SECONDS must be at least 1")
        if max_attempts < 1:
            raise ValueError("POSTGRES_WRITE_MAX_ATTEMPTS must be at least 1")
        if initial_backoff < 0 or max_backoff < 0:
            raise ValueError("PostgreSQL retry backoff values cannot be negative")

        retryable_db_errors = (
            psycopg2.OperationalError,
            psycopg2.InterfaceError,
        )
        
        def write_batch(batch_df, batch_id):
            """Batch write function"""
            try:
                rows = batch_df.select(
                    "event_id",
                    "event_ts",
                    "session_id",
                    "detected_at",
                    "window_start",
                    "window_end",
                    "click_count",
                    "unique_items",
                    "anomaly_score",
                    "anomaly_type",
                    "kafka_ingest_ts",
                    "spark_process_ts",
                    "source"
                ).dropDuplicates(["event_id"]).collect()

                if not rows:
                    logger.info(f"Batch {batch_id}: No anomalies detected")
                    return

                logger.info(f"Batch {batch_id}: Detected {len(rows)} anomalies")
                logger.debug("Batch %s sample: %s", batch_id, rows[:5])

                values = [
                    (
                        row["event_id"],
                        row["event_ts"],
                        row["session_id"],
                        row["detected_at"],
                        row["window_start"],
                        row["window_end"],
                        row["click_count"],
                        row["unique_items"],
                        float(row["anomaly_score"]),
                        row["anomaly_type"],
                        row["kafka_ingest_ts"],
                        row["spark_process_ts"],
                        row["source"]
                    )
                    for row in rows
                ]

                for attempt in range(1, max_attempts + 1):
                    try:
                        with psycopg2.connect(
                            jdbc_url,
                            user=db_user,
                            password=db_password,
                            connect_timeout=connect_timeout,
                        ) as conn:
                            with conn.cursor() as cur:
                                event_ids = [value[0] for value in values]
                                cur.execute(
                                    "DELETE FROM anomaly_sessions WHERE event_id = ANY(%s)",
                                    (event_ids,)
                                )
                                execute_values(
                                    cur,
                                    """
                                    INSERT INTO anomaly_sessions (
                                        event_id, event_ts, session_id, detected_at, window_start, window_end,
                                        click_count, unique_items, anomaly_score, anomaly_type,
                                        kafka_ingest_ts, spark_process_ts, source, db_write_ts
                                    )
                                    VALUES %s
                                    ON CONFLICT (session_id, window_start, anomaly_type, detected_at)
                                    DO UPDATE SET
                                        click_count = EXCLUDED.click_count,
                                        unique_items = EXCLUDED.unique_items,
                                        anomaly_score = EXCLUDED.anomaly_score,
                                        kafka_ingest_ts = EXCLUDED.kafka_ingest_ts,
                                        spark_process_ts = EXCLUDED.spark_process_ts,
                                        source = EXCLUDED.source,
                                        db_write_ts = CURRENT_TIMESTAMP,
                                        updated_at = CURRENT_TIMESTAMP
                                    """,
                                    values,
                                    template=(
                                        "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                        "%s, %s, %s, CURRENT_TIMESTAMP)"
                                    )
                                )
                            conn.commit()
                        break
                    except retryable_db_errors as error:
                        if attempt == max_attempts:
                            logger.error(
                                "Batch %s: PostgreSQL write failed after %s attempts",
                                batch_id,
                                max_attempts,
                            )
                            raise

                        backoff = builtins.min(
                            initial_backoff * (2 ** (attempt - 1)),
                            max_backoff,
                        )
                        logger.warning(
                            "Batch %s: PostgreSQL unavailable on attempt %s/%s: %s. "
                            "Retrying in %.1f seconds",
                            batch_id,
                            attempt,
                            max_attempts,
                            error,
                            backoff,
                        )
                        time.sleep(backoff)

                logger.info(f"Batch {batch_id}: Upserted {len(values)} rows to database")
                
            except Exception as e:
                logger.error(f"Batch {batch_id}: Failed to write: {e}")
                raise
        
        # Write stream using foreachBatch
        # Use update mode (append not supported without watermark)
        query = df \
            .writeStream \
            .queryName(query_name) \
            .outputMode(output_mode) \
            .foreachBatch(write_batch) \
            .option("checkpointLocation", f"{self.checkpoint_location}/{query_name}") \
            .trigger(processingTime='5 seconds') \
            .start()
        
        return query
    
    def write_to_console(self, df, query_name="console"):
        """Write to console for debugging"""
        # Use complete mode to see all window results
        query = df \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "50") \
            .queryName(query_name) \
            .trigger(processingTime='5 seconds') \
            .start()
        
        return query
    
    def run(self, enable_console=True):
        """Run the anomaly detection job"""
        logger.info("="*60)
        logger.info("Starting Anomaly Detection Job")
        logger.info("="*60)
        
        # Read from Kafka
        kafka_stream = self.read_kafka_stream()
        
        # Parse events
        clicks_df = self.parse_events(kafka_stream)

        on_time_clicks, late_clicks = self.split_late_events(clicks_df)
        deduped_clicks = on_time_clicks \
            .withWatermark("event_ts", self.watermark_duration) \
            .dropDuplicates(["event_id"])
        
        raw_query = None
        if enable_console:
            # DEBUG: Verify raw Kafka -> Spark parsing only when explicitly enabled.
            logger.info("Setting up raw data console output for debugging...")
            raw_query = deduped_clicks \
                .writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .option("numRows", "10") \
                .queryName("raw_data_debug") \
                .trigger(processingTime='5 seconds') \
                .start()
        
        # High frequency detection
        high_frequency_anomalies = self.detect_high_frequency(deduped_clicks)

        late_query = self.write_late_events_to_kafka(late_clicks)
        
        # Write high frequency anomalies to PostgreSQL
        # Use update mode (append not possible without watermark)
        query1 = self.write_to_postgres(
            high_frequency_anomalies,
            query_name="high_frequency",
            output_mode="update"
        )
        
        # Optional: Console output for debugging
        if enable_console:
            query2 = self.write_to_console(
                high_frequency_anomalies,
                query_name="high_frequency_console"
            )
        
        # Bot-like detection: 1분 윈도우에서 고빈도 + 저다양성 세션 탐지
        bot_like_anomalies = self.detect_bot_like(deduped_clicks)
        query3 = self.write_to_postgres(
            bot_like_anomalies,
            query_name="bot_like",
            output_mode="update",
        )
        
        logger.info("Anomaly detection job started successfully")
        logger.info(f"Checkpoint location: {self.checkpoint_location}")
        logger.info("Monitoring for anomalies (HIGH_FREQUENCY + BOT_LIKE)...")
        
        # Wait for termination
        try:
            self.spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            logger.info("Stopping anomaly detection job...")
            if raw_query:
                raw_query.stop()
            late_query.stop()
            query1.stop()
            query3.stop()
            if enable_console:
                query2.stop()
            logger.info("Job stopped successfully")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Anomaly Detection Streaming Job')
    parser.add_argument(
        '--kafka-servers',
        default='kafka-1:29092,kafka-2:29093,kafka-3:29094',
        help='Kafka bootstrap servers'
    )
    parser.add_argument(
        '--checkpoint',
        default='/opt/spark-checkpoint/anomaly',
        help='Checkpoint location'
    )
    parser.add_argument(
        '--click-topic',
        default='km.clicks.raw.v1',
        help='Kafka click topic to consume'
    )
    parser.add_argument(
        '--max-offsets-per-trigger',
        type=int,
        default=250000,
        help='Maximum Kafka records consumed by each query per trigger'
    )
    parser.add_argument(
        '--progress-output',
        default='/opt/test-results/streaming_progress.jsonl',
        help='JSONL path for StreamingQueryProgress events'
    )
    parser.add_argument(
        '--no-console',
        action='store_true',
        help='Disable console output'
    )
    
    args = parser.parse_args()
    
    detector = AnomalyDetector(
        kafka_servers=args.kafka_servers,
        checkpoint_location=args.checkpoint,
        click_topic=args.click_topic,
        max_offsets_per_trigger=args.max_offsets_per_trigger,
        progress_output=args.progress_output,
    )
    
    detector.run(enable_console=not args.no_console)


if __name__ == '__main__':
    main()
