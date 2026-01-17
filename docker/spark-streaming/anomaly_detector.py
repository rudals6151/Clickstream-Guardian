"""
Real-time Anomaly Detection using Spark Structured Streaming

This job monitors click events from Kafka and detects anomalous sessions
based on patterns like:
- High frequency clicks (potential bot)
- Repetitive behavior
- Unusual session duration
"""

# docker logs -f spark-streaming-anomaly
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

from common.kafka_utils import get_click_schema, get_kafka_options
from common.postgres_utils import get_postgres_properties, upsert_to_postgres


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
        
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("ClickstreamAnomalyDetector") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.streaming.kafka.consumer.cache.capacity", "1000") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized")
        
        # Anomaly detection thresholds
        self.HIGH_FREQUENCY_THRESHOLD = 50  # clicks in 10 seconds
        self.BOT_LIKE_THRESHOLD = 100  # clicks in 1 minute
        self.LOW_DIVERSITY_RATIO = 0.1  # unique items / total clicks
    
    def read_kafka_stream(self):
        """Read click events from Kafka"""
        logger.info(f"Reading from Kafka: {self.kafka_servers}")
        
        kafka_options = get_kafka_options(
            bootstrap_servers=self.kafka_servers,
            topic="km.clicks.raw.v1",
            group_id="anomaly-detector"
        )
        
        return self.spark \
            .readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()
    
    def parse_events(self, kafka_df):
        """Parse Avro messages from Kafka"""
        # For simplicity, we'll use JSON parsing here
        # In production, use proper Avro deserialization
        
        schema = get_click_schema()
        
        # Parse JSON value
        clicks_df = kafka_df.select(
            from_json(
                col("value").cast("string"),
                schema
            ).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Convert event_ts to timestamp if needed
        clicks_df = clicks_df.withColumn(
            "event_ts",
            from_unixtime(col("event_ts") / 1000).cast("timestamp")
        )
        
        return clicks_df
    
    def detect_high_frequency(self, clicks_df):
        """
        Detect high frequency clicks (10-second tumbling window)
        Identifies sessions with unusually high click rates
        """
        logger.info("Setting up high frequency detection...")
        
        # Add watermark to handle late data (5 minutes)
        clicks_watermarked = clicks_df \
            .withWatermark("event_ts", "5 minutes")
        
        # 10-second tumbling window aggregation
        high_frequency = clicks_watermarked \
            .groupBy(
                window("event_ts", "10 seconds"),
                "session_id"
            ) \
            .agg(
                count("*").alias("click_count"),
                countDistinct("item_id").alias("unique_items"),
                min("event_ts").alias("window_start"),
                max("event_ts").alias("window_end"),
                collect_set("category").alias("categories")
            ) \
            .filter(col("click_count") > self.HIGH_FREQUENCY_THRESHOLD) \
            .withColumn("anomaly_type", lit("HIGH_FREQUENCY")) \
            .withColumn(
                "anomaly_score",
                col("click_count") / lit(self.HIGH_FREQUENCY_THRESHOLD)
            ) \
            .withColumn("detected_at", current_timestamp()) \
            .select(
                "session_id",
                "detected_at",
                "window_start",
                "window_end",
                "click_count",
                "unique_items",
                "anomaly_score",
                "anomaly_type"
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
                countDistinct("item_id").alias("unique_items"),
                min("event_ts").alias("window_start"),
                max("event_ts").alias("window_end")
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
            .select(
                "session_id",
                "detected_at",
                "window_start",
                "window_end",
                "click_count",
                "unique_items",
                "anomaly_score",
                "anomaly_type"
            )
        
        return bot_like
    
    def write_to_postgres(self, df, output_mode="append"):
        """Write anomalies to PostgreSQL"""
        postgres_props = get_postgres_properties()
        
        def write_batch(batch_df, batch_id):
            """Batch write function"""
            if batch_df.count() == 0:
                logger.info(f"Batch {batch_id}: No anomalies detected")
                return
            
            try:
                # Show sample for debugging
                logger.info(f"Batch {batch_id}: Detected {batch_df.count()} anomalies")
                batch_df.show(5, truncate=False)
                
                # Write to PostgreSQL
                batch_df.write \
                    .format("jdbc") \
                    .options(**postgres_props) \
                    .option("dbtable", "anomaly_sessions") \
                    .mode("append") \
                    .save()
                
                logger.info(f"Batch {batch_id}: Successfully written to database")
                
            except Exception as e:
                logger.error(f"Batch {batch_id}: Failed to write: {e}")
                raise
        
        # Write stream using foreachBatch
        query = df \
            .writeStream \
            .outputMode(output_mode) \
            .foreachBatch(write_batch) \
            .option("checkpointLocation", f"{self.checkpoint_location}/{output_mode}") \
            .start()
        
        return query
    
    def write_to_console(self, df, query_name="console"):
        """Write to console for debugging"""
        query = df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("numRows", "20") \
            .queryName(query_name) \
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
        
        # High frequency detection
        high_frequency_anomalies = self.detect_high_frequency(clicks_df)
        
        # Write high frequency anomalies to PostgreSQL
        query1 = self.write_to_postgres(
            high_frequency_anomalies,
            output_mode="append"
        )
        
        # Optional: Console output for debugging
        if enable_console:
            query2 = self.write_to_console(
                high_frequency_anomalies,
                query_name="high_frequency_console"
            )
        
        # Bot-like detection (optional, can enable later)
        # bot_like_anomalies = self.detect_bot_like(clicks_df)
        # query3 = self.write_to_postgres(bot_like_anomalies, output_mode="append")
        
        logger.info("Anomaly detection job started successfully")
        logger.info(f"Checkpoint location: {self.checkpoint_location}")
        logger.info("Monitoring for anomalies...")
        
        # Wait for termination
        try:
            query1.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Stopping anomaly detection job...")
            query1.stop()
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
