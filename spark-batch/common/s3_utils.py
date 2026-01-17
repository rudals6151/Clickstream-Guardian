"""
S3 utilities for Spark Batch processing
"""


def configure_s3_spark(spark, endpoint=None, 
                       access_key=None, secret_key=None):
    """Configure Spark to use S3 (AWS or MinIO)"""
    import os
    
    # Use AWS credentials from environment if available
    access_key = access_key or os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = secret_key or os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    if endpoint:
        # MinIO configuration
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", endpoint)
        spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    else:
        # AWS S3 configuration (default)
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint.region", "ap-northeast-2")
        spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "false")


def get_s3_path(bucket, prefix, date):
    """
    Get S3 path for a specific date
    
    Args:
        bucket: S3 bucket name
        prefix: Path prefix (e.g., 'raw_clicks')
        date: Date string in YYYY-MM-DD format
    
    Returns:
        S3 path string
    """
    # Kafka connector stores data in topics/{topic_name}/{prefix}/dt={date}/
    if prefix == 'raw_clicks':
        return f"s3a://{bucket}/topics/km.clicks.raw.v1/{prefix}/dt={date}/*"
    elif prefix == 'raw_purchases':
        return f"s3a://{bucket}/topics/km.purchases.raw.v1/{prefix}/dt={date}/*"
    else:
        return f"s3a://{bucket}/{prefix}/dt={date}/*"


def check_s3_path_exists(spark, path):
    """Check if S3 path exists and has data"""
    try:
        df = spark.read.parquet(path)
        count = df.count()
        return count > 0
    except Exception:
        return False
