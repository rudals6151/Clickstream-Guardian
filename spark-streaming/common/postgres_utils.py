"""
PostgreSQL utilities for Spark Streaming
"""
import logging
import os

logger = logging.getLogger(__name__)


def _require_env(name: str) -> str:
    """환경변수를 가져오되, 비밀번호 등 필수 값이 없으면 에러를 발생시킨다."""
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"필수 환경변수 '{name}'이(가) 설정되지 않았습니다.")
    return value


def get_postgres_properties(host=None, port=None, database=None, user=None, password=None):
    """Get PostgreSQL JDBC properties"""
    host = host or os.getenv("POSTGRES_HOST", "postgres")
    port = int(port or os.getenv("POSTGRES_PORT", "5432"))
    database = database or os.getenv("POSTGRES_DB", "clickstream")
    user = user or os.getenv("POSTGRES_USER", "admin")
    password = password or _require_env("POSTGRES_PASSWORD")

    return {
        "url": f"jdbc:postgresql://{host}:{port}/{database}",
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "batchsize": "1000",
        "isolationLevel": "READ_COMMITTED"
    }


def append_to_postgres(batch_df, batch_id, table_name, key_columns, postgres_props):
    """
    Append data to PostgreSQL (foreachBatch sink).

    Note: This uses simple append mode. For true upsert with
    ON CONFLICT handling, use a custom psycopg2 write function
    (see AnomalyDetector.write_to_postgres for an example).
    
    Args:
        batch_df: DataFrame to write
        batch_id: Batch ID from foreachBatch
        table_name: Target table name
        key_columns: List of key columns (reserved for future upsert logic)
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
