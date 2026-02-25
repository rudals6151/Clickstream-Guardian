"""
Daily Metrics Calculation
Computes aggregated metrics for a specific date
"""
import sys
import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from common.s3_utils import configure_s3_spark, get_s3_path


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DailyMetricsCalculator:
    """Calculate daily clickstream metrics"""
    
    def __init__(self, target_date):
        self.target_date = target_date
        self.bucket = "km-data-lake"
        
        # Initialize Spark
        self.spark = SparkSession.builder \
            .appName(f"DailyMetrics-{target_date}") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Configure S3
        configure_s3_spark(self.spark)
        
        logger.info(f"DailyMetricsCalculator initialized for {target_date}")
    
    def read_clicks(self):
        """Read click data for target date from S3"""
        path = get_s3_path(self.bucket, "raw_clicks", self.target_date)
        logger.info(f"Reading clicks from: {path}")
        
        try:
            df = self.spark.read.parquet(path)
            count = df.count()
            logger.info(f"Loaded {count:,} click events")
            return df
        except Exception as e:
            logger.error(f"Failed to read clicks: {e}")
            raise
    
    def read_purchases(self):
        """Read purchase data for target date from S3"""
        path = get_s3_path(self.bucket, "raw_purchases", self.target_date)
        logger.info(f"Reading purchases from: {path}")
        
        try:
            df = self.spark.read.parquet(path)
            count = df.count()
            logger.info(f"Loaded {count:,} purchase events")
            return df
        except Exception as e:
            logger.warning(f"No purchase data found: {e}")
            # Return empty DataFrame with expected schema
            schema = StructType([
                StructField("session_id", LongType()),
                StructField("event_ts", LongType()),
                StructField("item_id", LongType()),
                StructField("price", LongType()),
                StructField("quantity", LongType())
            ])
            return self.spark.createDataFrame([], schema)
    
    def calculate_metrics(self):
        """Calculate all daily metrics"""
        logger.info("Calculating daily metrics...")
        
        clicks_df = self.read_clicks()
        purchases_df = self.read_purchases()
        
        # Click metrics
        click_metrics = clicks_df.agg(
            count("*").alias("total_clicks"),
            countDistinct("session_id").alias("unique_sessions_clicks"),
            countDistinct("item_id").alias("unique_items_clicks")
        )
        
        # Purchase metrics with proper revenue calculation
        purchase_metrics = purchases_df.agg(
            count("*").alias("total_purchases"),
            countDistinct("session_id").alias("unique_sessions_purchases"),
            sum(col("price") * col("quantity")).alias("total_revenue")
        )
        
        # Session-level statistics
        # event_ts is in milliseconds, convert to seconds
        session_clicks = clicks_df.groupBy("session_id").agg(
            count("*").alias("clicks_per_session"),
            ((max(col("event_ts")) - min(col("event_ts"))) / 1000).alias("duration_sec")
        )
        
        session_stats = session_clicks.agg(
            avg("clicks_per_session").alias("avg_clicks_per_session"),
            avg("duration_sec").alias("avg_session_duration_sec")
        )
        
        # Conversion rate
        conversion_df = click_metrics \
            .crossJoin(purchase_metrics) \
            .withColumn(
                "conversion_rate",
                when(
                    col("unique_sessions_clicks") > 0,
                    col("unique_sessions_purchases") / col("unique_sessions_clicks")
                ).otherwise(0.0)
            ) \
            .withColumn(
                "avg_order_value",
                when(
                    col("total_purchases") > 0,
                    col("total_revenue") / col("total_purchases")
                ).otherwise(0.0)
            )
        
        # Combine all metrics (excluding unique_users as user data is not available)
        result = conversion_df \
            .crossJoin(session_stats) \
            .withColumn("metric_date", lit(self.target_date).cast("date")) \
            .withColumn("unique_sessions", col("unique_sessions_clicks")) \
            .withColumn("unique_items", col("unique_items_clicks")) \
            .select(
                "metric_date",
                "total_clicks",
                "total_purchases",
                "unique_sessions",
                "unique_items",
                "conversion_rate",
                "avg_session_duration_sec",
                "avg_clicks_per_session",
                "total_revenue",
                "avg_order_value"
            )
        
        logger.info("Metrics calculation completed")
        return result
    
    def write_to_postgres(self, df):
        """Write metrics to PostgreSQL"""
        from pyspark.sql.functions import current_timestamp
        import psycopg2
        
        postgres_props = {
            "url": f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'clickstream')}",
            "user": os.getenv("POSTGRES_USER", "admin"),
            "password": os.getenv("POSTGRES_PASSWORD", "password"),
            "driver": "org.postgresql.Driver"
        }
        
        logger.info("Writing metrics to PostgreSQL...")
        
        # Add created_at and updated_at columns
        df_with_ts = df.withColumn("created_at", current_timestamp()) \
                       .withColumn("updated_at", current_timestamp())
        
        # Show metrics before writing
        logger.info("=== METRICS DATA (before writing) ===")
        df_with_ts.show(truncate=False)
        
        # Delete existing data for this date using psycopg2
        logger.info(f"Deleting existing data for {self.target_date}...")
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                database=os.getenv("POSTGRES_DB", "clickstream"),
                user=os.getenv("POSTGRES_USER", "admin"),
                password=os.getenv("POSTGRES_PASSWORD", "password")
            )
            cursor = conn.cursor()
            cursor.execute("DELETE FROM daily_metrics WHERE metric_date = %s", (self.target_date,))
            deleted_count = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Deleted {deleted_count} rows for {self.target_date}")
        except Exception as e:
            logger.warning(f"Error deleting existing data: {e}")
        
        # Append new data
        logger.info("Appending new data to PostgreSQL...")
        df_with_ts.write \
            .format("jdbc") \
            .options(**postgres_props) \
            .option("dbtable", "daily_metrics") \
            .mode("append") \
            .save()
        
        logger.info("✅ Metrics written successfully")

    
    def run(self):
        """Execute the metrics calculation"""
        logger.info("="*60)
        logger.info(f"Starting Daily Metrics Calculation for {self.target_date}")
        logger.info("="*60)
        
        try:
            metrics_df = self.calculate_metrics()
            self.write_to_postgres(metrics_df)
            
            logger.info("="*60)
            logger.info("Daily Metrics Calculation Completed Successfully")
            logger.info("="*60)
            
            return True
            
        except Exception as e:
            logger.error(f"Daily metrics calculation failed: {e}")
            raise
        finally:
            self.spark.stop()


def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python daily_metrics.py <date>")
        print("Example: python daily_metrics.py 2014-04-07")
        sys.exit(1)
    
    target_date = sys.argv[1]
    
    # Validate date format
    try:
        datetime.strptime(target_date, '%Y-%m-%d')
    except ValueError:
        print(f"Invalid date format: {target_date}. Expected YYYY-MM-DD")
        sys.exit(1)
    
    calculator = DailyMetricsCalculator(target_date)
    calculator.run()


if __name__ == '__main__':
    main()
