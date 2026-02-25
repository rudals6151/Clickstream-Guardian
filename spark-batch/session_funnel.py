"""
Session Funnel Analysis
Analyzes conversion funnel from view to purchase
"""
import sys
import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum, when, lit, greatest,
    current_timestamp,
)
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from pyspark.sql.window import Window

from common.s3_utils import configure_s3_spark, get_s3_path


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SessionFunnelAnalyzer:
    """Analyze session conversion funnel"""
    
    def __init__(self, target_date):
        self.target_date = target_date
        self.bucket = "km-data-lake"
        
        self.spark = SparkSession.builder \
            .appName(f"SessionFunnel-{target_date}") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        configure_s3_spark(self.spark)
        logger.info(f"SessionFunnelAnalyzer initialized for {target_date}")
    
    def read_data(self):
        """Read clicks and purchases"""
        clicks_path = get_s3_path(self.bucket, "raw_clicks", self.target_date)
        purchases_path = get_s3_path(self.bucket, "raw_purchases", self.target_date)
        
        clicks_df = self.spark.read.parquet(clicks_path)
        
        try:
            purchases_df = self.spark.read.parquet(purchases_path)
        except Exception:
            schema = StructType([
                StructField("session_id", LongType()),
                StructField("item_id", LongType())
            ])
            purchases_df = self.spark.createDataFrame([], schema)
        
        return clicks_df, purchases_df
    
    def analyze_funnel(self):
        """Analyze conversion funnel stages"""
        logger.info("Analyzing conversion funnel...")
        
        clicks_df, purchases_df = self.read_data()
        
        # Session-level aggregation
        session_clicks = clicks_df.groupBy("session_id").agg(
            count("*").alias("click_count"),
            countDistinct("item_id").alias("unique_items_viewed")
        )
        
        # Sessions with purchases
        session_purchases = purchases_df.groupBy("session_id").agg(
            count("*").alias("purchase_count")
        )
        
        # Join to identify converted sessions
        sessions = session_clicks.join(
            session_purchases,
            "session_id",
            "left"
        ).withColumn(
            "has_purchase",
            col("purchase_count").isNotNull()
        )
        
        # Funnel stages
        total_sessions = sessions.count()
        
        # Stage 1: VIEW - all sessions
        view_sessions = total_sessions
        
        # Stage 2: MULTI_VIEW - sessions with 2+ clicks
        multi_view_sessions = sessions.filter(col("click_count") >= 2).count()
        
        # Stage 3: PURCHASE - sessions with purchase
        purchase_sessions = sessions.filter(col("has_purchase") == True).count()
        
        # Create funnel DataFrame
        funnel_data = [
            ("VIEW", view_sessions, 100.0, 0.0),
            ("MULTI_VIEW", multi_view_sessions, 
             (multi_view_sessions / view_sessions * 100) if view_sessions > 0 else 0,
             ((view_sessions - multi_view_sessions) / view_sessions) if view_sessions > 0 else 0),
            ("PURCHASE", purchase_sessions,
             (purchase_sessions / view_sessions * 100) if view_sessions > 0 else 0,
             ((multi_view_sessions - purchase_sessions) / multi_view_sessions) if multi_view_sessions > 0 else 0)
        ]
        
        funnel_schema = StructType([
            StructField("funnel_stage", StringType()),
            StructField("session_count", LongType()),
            StructField("percentage", DoubleType()),
            StructField("drop_rate", DoubleType())
        ])
        
        funnel_df = self.spark.createDataFrame(funnel_data, funnel_schema) \
            .withColumn("metric_date", lit(self.target_date).cast("date"))
        
        logger.info("Funnel analysis completed")
        return funnel_df
    
    def write_to_postgres(self, df):
        """Write funnel data to PostgreSQL (atomic delete + insert)"""
        from pyspark.sql.functions import current_timestamp
        from common.postgres_utils import atomic_write
        
        logger.info("Writing funnel data to PostgreSQL...")
        
        # Add created_at column
        df_with_ts = df.withColumn("created_at", current_timestamp())
        df_with_ts.show(truncate=False)
        
        atomic_write(df_with_ts, "session_funnel", self.target_date)
        logger.info("Funnel data written successfully")
    
    def run(self):
        """Execute funnel analysis"""
        logger.info("="*60)
        logger.info(f"Starting Session Funnel Analysis for {self.target_date}")
        logger.info("="*60)
        
        try:
            funnel_df = self.analyze_funnel()
            self.write_to_postgres(funnel_df)
            
            logger.info("Session Funnel Analysis Completed Successfully")
            return True
            
        except Exception as e:
            logger.error(f"Funnel analysis failed: {e}")
            raise
        finally:
            self.spark.stop()


def main():
    if len(sys.argv) < 2:
        print("Usage: python session_funnel.py <date>")
        sys.exit(1)
    
    target_date = sys.argv[1]
    analyzer = SessionFunnelAnalyzer(target_date)
    analyzer.run()


if __name__ == '__main__':
    main()
