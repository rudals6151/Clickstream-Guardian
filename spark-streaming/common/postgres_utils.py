"""
PostgreSQL utilities for Spark Streaming
"""
import logging
import os

logger = logging.getLogger(__name__)


def get_postgres_properties(host=None, port=None, database=None, user=None, password=None):
    """Get PostgreSQL JDBC properties"""
    host = host or os.getenv("POSTGRES_HOST", "postgres")
    port = int(port or os.getenv("POSTGRES_PORT", "5432"))
    database = database or os.getenv("POSTGRES_DB", "clickstream")
    user = user or os.getenv("POSTGRES_USER", "admin")
    password = password or os.getenv("POSTGRES_PASSWORD", "password")

    return {
        "url": f"jdbc:postgresql://{host}:{port}/{database}",
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "batchsize": "1000",
        "isolationLevel": "READ_COMMITTED"
    }


def upsert_to_postgres(batch_df, batch_id, table_name, key_columns, postgres_props):
    """
    Upsert data to PostgreSQL
    
    Args:
        batch_df: DataFrame to write
        batch_id: Batch ID from foreachBatch
        table_name: Target table name
        key_columns: List of key columns for upsert
        postgres_props: PostgreSQL connection properties
    """
    if batch_df.count() == 0:
        logger.info(f"Batch {batch_id}: No data to write")
        return
    
    try:
        # For simplicity, using append mode
        # In production, implement proper UPSERT logic
        batch_df.write \
            .format("jdbc") \
            .options(**postgres_props) \
            .option("dbtable", table_name) \
            .mode("append") \
            .save()
        
        logger.info(f"Batch {batch_id}: Written {batch_df.count()} rows to {table_name}")
        
    except Exception as e:
        logger.error(f"Batch {batch_id}: Failed to write to PostgreSQL: {e}")
        raise


def write_to_postgres_batch(df, table_name, mode="append"):
    """
    Write DataFrame to PostgreSQL in batch mode
    
    Args:
        df: DataFrame to write
        table_name: Target table name
        mode: Write mode (append, overwrite, etc.)
    """
    postgres_props = get_postgres_properties()
    
    df.write \
        .format("jdbc") \
        .options(**postgres_props) \
        .option("dbtable", table_name) \
        .mode(mode) \
        .save()
