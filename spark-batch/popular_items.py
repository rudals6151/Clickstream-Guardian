"""
Popular Items Analysis
Identifies top items by clicks, purchases, and revenue
"""
import sys
import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum, when, lit, row_number,
    current_timestamp,
)
from pyspark.sql.window import Window

from common.s3_utils import configure_s3_spark, get_s3_path


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PopularItemsAnalyzer:
    """Analyze popular items and categories"""
    
    def __init__(self, target_date, top_n=100):
        self.target_date = target_date
        self.top_n = top_n
        self.bucket = "km-data-lake"
        
        self.spark = SparkSession.builder \
            .appName(f"PopularItems-{target_date}") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        configure_s3_spark(self.spark)
        logger.info(f"PopularItemsAnalyzer initialized for {target_date}, top_n={top_n}")
    
    def read_data(self):
        """Read clicks and purchases"""
        clicks_path = get_s3_path(self.bucket, "raw_clicks", self.target_date)
        purchases_path = get_s3_path(self.bucket, "raw_purchases", self.target_date)
        
        clicks_df = self.spark.read.parquet(clicks_path)
        
        try:
            purchases_df = self.spark.read.parquet(purchases_path)
        except Exception:
            from pyspark.sql.types import StructType, StructField, LongType, IntegerType
            schema = StructType([
                StructField("item_id", LongType()),
                StructField("price", IntegerType()),
                StructField("quantity", IntegerType())
            ])
            purchases_df = self.spark.createDataFrame([], schema)
        
        return clicks_df, purchases_df
    
    def analyze_popular_items(self):
        """Analyze top items by various metrics"""
        logger.info("Analyzing popular items...")
        
        clicks_df, purchases_df = self.read_data()
        
        # First, get the most frequent category for each item_id
        # to avoid duplicate keys when same item appears in multiple categories
        item_category_window = Window.partitionBy("item_id").orderBy(col("click_count_per_cat").desc())
        
        item_categories = clicks_df.groupBy("item_id", "category").agg(
            count("*").alias("click_count_per_cat")
        ).withColumn(
            "rank", row_number().over(item_category_window)
        ).filter(
            col("rank") == 1
        ).select("item_id", "category")
        
        # Aggregate clicks by item (total across all categories)
        click_stats = clicks_df.groupBy("item_id").agg(
            count("*").alias("click_count")
        )
        
        # Join with the primary category
        click_stats = click_stats.join(item_categories, "item_id", "left")
        
        # Aggregate purchases by item
        purchase_stats = purchases_df.groupBy("item_id").agg(
            count("*").alias("purchase_count"),
            sum(col("price") * col("quantity")).alias("revenue")
        )
        
        # Join click and purchase stats
        item_stats = click_stats.join(
            purchase_stats,
            "item_id",
            "left"
        ).na.fill(0, ["purchase_count", "revenue"])
        
        # Add click-to-purchase ratio
        item_stats = item_stats.withColumn(
            "click_to_purchase_ratio",
            when(
                col("purchase_count") > 0,
                col("click_count") / col("purchase_count")
            ).otherwise(None)
        )
        
        # Rank by revenue
        window_spec = Window.orderBy(col("revenue").desc())
        
        top_items = item_stats \
            .withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") <= self.top_n) \
            .withColumn("metric_date", lit(self.target_date).cast("date")) \
            .select(
                "metric_date",
                "item_id",
                "category",
                "click_count",
                "purchase_count",
                "revenue",
                "rank",
                "click_to_purchase_ratio"
            )
        
        logger.info(f"Identified top {self.top_n} items")
        return top_items
    
    def analyze_popular_categories(self):
        """Analyze top categories"""
        logger.info("Analyzing popular categories...")
        
        clicks_df, purchases_df = self.read_data()
        
        # Category stats from clicks
        category_clicks = clicks_df \
            .filter(col("category").isNotNull()) \
            .groupBy("category").agg(
                count("*").alias("click_count"),
                countDistinct("item_id").alias("unique_items")
            )
        
        # Join with purchases (need to get category from clicks first)
        clicks_with_cat = clicks_df.select("item_id", "category").distinct()
        
        purchases_with_cat = purchases_df.join(
            clicks_with_cat,
            "item_id",
            "left"
        )
        
        category_purchases = purchases_with_cat \
            .filter(col("category").isNotNull()) \
            .groupBy("category").agg(
                count("*").alias("purchase_count"),
                sum(col("price") * col("quantity")).alias("revenue")
            )
        
        # Join and rank
        category_stats = category_clicks.join(
            category_purchases,
            "category",
            "left"
        ).na.fill(0, ["purchase_count", "revenue"])
        
        window_spec = Window.orderBy(col("revenue").desc())
        
        top_categories = category_stats \
            .withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") <= 20) \
            .withColumn("metric_date", lit(self.target_date).cast("date")) \
            .select(
                "metric_date",
                "category",
                "click_count",
                "purchase_count",
                "unique_items",
                "revenue",
                "rank"
            )
        
        logger.info("Category analysis completed")
        return top_categories
    
    def write_to_postgres(self, items_df, categories_df):
        """Write results to PostgreSQL (atomic delete + insert)"""
        from pyspark.sql.functions import current_timestamp
        from common.postgres_utils import atomic_write
        
        logger.info("Writing popular items to PostgreSQL...")
        
        # Add created_at column to items
        items_with_ts = items_df.withColumn("created_at", current_timestamp())
        items_with_ts.show(20, truncate=False)
        
        atomic_write(items_with_ts, "popular_items", self.target_date, coalesce=1)
        
        logger.info("Writing popular categories to PostgreSQL...")
        
        # Add created_at column to categories
        categories_with_ts = categories_df.withColumn("created_at", current_timestamp())
        categories_with_ts.show(20, truncate=False)
        
        atomic_write(categories_with_ts, "popular_categories", self.target_date, coalesce=1)
        
        logger.info("Popular items/categories written successfully")
    
    def run(self):
        """Execute popular items analysis"""
        logger.info("="*60)
        logger.info(f"Starting Popular Items Analysis for {self.target_date}")
        logger.info("="*60)
        
        try:
            items_df = self.analyze_popular_items()
            categories_df = self.analyze_popular_categories()
            self.write_to_postgres(items_df, categories_df)
            
            logger.info("Popular Items Analysis Completed Successfully")
            return True
            
        except Exception as e:
            logger.error(f"Popular items analysis failed: {e}")
            raise
        finally:
            self.spark.stop()


def main():
    if len(sys.argv) < 2:
        print("Usage: python popular_items.py <date> [top_n]")
        sys.exit(1)
    
    target_date = sys.argv[1]
    top_n = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    
    analyzer = PopularItemsAnalyzer(target_date, top_n)
    analyzer.run()


if __name__ == '__main__':
    main()
