"""
Real-time Anomaly Detection using Spark Structured Streaming

This job monitors click events from Kafka and detects anomalous sessions
based on patterns like:
- High frequency clicks (potential bot)
- Repetitive behavior
- Unusual session duration
"""
import sys
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, lit, when, length, trim, sha2,
    concat_ws, window, min, max, current_timestamp,
    unix_timestamp, expr, to_json, struct,
    approx_count_distinct,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType
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


class AnomalyDetector:
    """Real-time anomaly detection for clickstream data"""
    
    def __init__(self, kafka_servers="kafka-1:29092,kafka-2:29093,kafka-3:29094",
                 checkpoint_location="/tmp/spark-checkpoint/anomaly"):
        self.kafka_servers = kafka_servers
        self.checkpoint_location = checkpoint_location
        self.late_topic = os.getenv("LATE_EVENTS_TOPIC", "km.events.late.v1")
        self.late_threshold_minutes = int(os.getenv("LATE_EVENT_THRESHOLD_MINUTES", "10"))
        
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("ClickstreamAnomalyDetector") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.streaming.kafka.consumer.cache.capacity", "1000") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized")
        
        # Anomaly detection thresholds
        # Adjusted threshold for easier testing (originally 50 -> 20)
        self.HIGH_FREQUENCY_THRESHOLD = 50  # clicks in 10 seconds
        self.BOT_LIKE_THRESHOLD = 50  # clicks in 1 minute (100 -> 50)
        self.LOW_DIVERSITY_RATIO = 0.1  # unique items / total clicks
    
    def read_kafka_stream(self):
        """Read click events from Kafka"""
        logger.info(f"Reading from Kafka: {self.kafka_servers}")
        
        kafka_options = get_kafka_options(
            bootstrap_servers=self.kafka_servers,
            topic="km.clicks.raw.v1",
            group_id="anomaly-detector"
        )
        
        # Use "latest" to start from current data (not historical)
        # "earliest" would read all past data, complicating watermark handling
        kafka_options["startingOffsets"] = "latest"
        
        return self.spark \
            .readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()
    
    def parse_events(self, kafka_df):
        """Parse Avro messages from Kafka using from_avro"""
        
        # Import from_avro here to avoid initialization issues
        from pyspark.sql.avro.functions import from_avro as avro_from_avro
        
        # Avro schema string for clicks (MUST match producer schema exactly)
        # NOTE: Avro timestamp-millis logical type is read as long in Spark, requires manual conversion
        avro_schema = """{
            "type": "record",
            "name": "ClickEvent",
            "namespace": "com.clickstream.guardian",
            "fields": [
                {"name": "session_id", "type": "long"},
                {"name": "event_ts", "type": "long"},
                {"name": "item_id", "type": "long"},
                {"name": "category", "type": ["null", "string"], "default": null},
                {"name": "event_type", "type": "string"},
                {"name": "event_id", "type": "string", "default": ""},
                {"name": "ingest_ts", "type": "long", "default": 0},
                {"name": "source", "type": "string", "default": "producer"}
            ]
        }"""
        
        # Schema Registry uses first 5 bytes for metadata (magic byte + schema ID)
        # We need to skip these bytes before deserializing
        clicks_df = kafka_df.select(
            avro_from_avro(
                expr("substring(value, 6, length(value))"),  # Skip first 5 bytes
                avro_schema
            ).alias("data"),
            col("timestamp").alias("kafka_ingest_ts")
        ).select("data.*", "kafka_ingest_ts")
        
        # Explicitly convert event_ts from long (milliseconds) to timestamp
        # Avro logical type may not be auto-converted in all cases
        clicks_df = clicks_df.withColumn(
            "event_ts",
            (col("event_ts") / 1000).cast("timestamp")
        )

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
        
        # Watermark를 설정하여 state 무한 증가를 방지
        # late_threshold_minutes 이전의 이벤트는 state에서 제거됨
        clicks_watermarked = clicks_df \
            .withWatermark("event_ts", f"{self.late_threshold_minutes} minutes")
        
        # 10-second tumbling window aggregation
        high_frequency = clicks_watermarked \
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
        
        clicks_watermarked = clicks_df \
            .withWatermark("event_ts", "5 minutes")
        
        # 1-minute sliding window (30 second slide)
        bot_like = clicks_watermarked \
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
    
    def write_to_postgres(self, df, output_mode="update"):
        """Write anomalies to PostgreSQL"""
        postgres_props = get_postgres_properties()
        jdbc_url = postgres_props["url"].replace("jdbc:", "")
        db_user = postgres_props["user"]
        db_password = postgres_props["password"]
        
        def write_batch(batch_df, batch_id):
            """Batch write function"""
            cnt = batch_df.count()
            if cnt == 0:
                logger.info(f"Batch {batch_id}: No anomalies detected")
                return
            
            try:
                # Show sample for debugging
                logger.info(f"Batch {batch_id}: Detected {cnt} anomalies")
                batch_df.show(5, truncate=False)

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

                with psycopg2.connect(jdbc_url, user=db_user, password=db_password) as conn:
                    with conn.cursor() as cur:
                        execute_values(
                            cur,
                            """
                            INSERT INTO anomaly_sessions (
                                event_id, event_ts, session_id, detected_at, window_start, window_end,
                                click_count, unique_items, anomaly_score, anomaly_type,
                                kafka_ingest_ts, spark_process_ts, source
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
                            values
                        )
                    conn.commit()

                logger.info(f"Batch {batch_id}: Upserted {len(values)} rows to database")
                
            except Exception as e:
                logger.error(f"Batch {batch_id}: Failed to write: {e}")
                raise
        
        # Write stream using foreachBatch
        # Use update mode (append not supported without watermark)
        query = df \
            .writeStream \
            .outputMode(output_mode) \
            .foreachBatch(write_batch) \
            .option("checkpointLocation", f"{self.checkpoint_location}/{output_mode}") \
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
            .withWatermark("event_ts", f"{self.late_threshold_minutes} minutes") \
            .dropDuplicates(["event_id"])
        
        # DEBUG: Verify raw data is arriving correctly
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
        query3 = self.write_to_postgres(bot_like_anomalies, output_mode="append")
        
        logger.info("Anomaly detection job started successfully")
        logger.info(f"Checkpoint location: {self.checkpoint_location}")
        logger.info("Monitoring for anomalies (HIGH_FREQUENCY + BOT_LIKE)...")
        
        # Wait for termination
        try:
            self.spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            logger.info("Stopping anomaly detection job...")
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
        default='/tmp/spark-checkpoint/anomaly',
        help='Checkpoint location'
    )
    parser.add_argument(
        '--no-console',
        action='store_true',
        help='Disable console output'
    )
    
    args = parser.parse_args()
    
    detector = AnomalyDetector(
        kafka_servers=args.kafka_servers,
        checkpoint_location=args.checkpoint
    )
    
    detector.run(enable_console=not args.no_console)


if __name__ == '__main__':
    main()
