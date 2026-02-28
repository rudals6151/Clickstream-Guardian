"""
PostgreSQL utilities for Spark Batch jobs

Provides atomic delete-then-append write pattern inside a single
psycopg2 transaction so partial writes can never leave the table
in an inconsistent state.
"""
import logging
import os

import psycopg2

logger = logging.getLogger(__name__)


def _require_env(name: str) -> str:
    """환경변수를 가져오되, 비밀번호 등 필수 값이 없으면 에러를 발생시킨다."""
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"필수 환경변수 '{name}'이(가) 설정되지 않았습니다.")
    return value


def get_postgres_properties():
    """Return JDBC properties dict used by Spark DataFrameWriter."""
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "clickstream")
    user = os.getenv("POSTGRES_USER", "admin")
    password = _require_env("POSTGRES_PASSWORD")

    return {
        "url": f"jdbc:postgresql://{host}:{port}/{database}",
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def _psycopg2_conn():
    """Create a plain psycopg2 connection from env vars."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "clickstream"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=_require_env("POSTGRES_PASSWORD"),
    )


def atomic_write(df, table_name, target_date, coalesce=None):
    """
    Atomically replace rows for *target_date* in *table_name*.

    Strategy:
        1. Write the Spark DataFrame to a temporary staging table.
        2. Inside a single transaction, DELETE existing rows for the
           date and INSERT from the staging table.
        3. Drop the staging table.

    This guarantees readers never see a partial result.

    Args:
        df: Spark DataFrame to write (must already include created_at / updated_at).
        table_name: Target PostgreSQL table.
        target_date: Date string (YYYY-MM-DD) used in the WHERE clause.
        coalesce: Optional number of partitions before writing (reduces JDBC parallelism).
    """
    staging_table = f"_staging_{table_name}"
    props = get_postgres_properties()

    writer = df
    if coalesce:
        writer = writer.coalesce(coalesce)

    # Step 1 — write to staging table (overwrite so it's clean every run)
    logger.info(f"Writing to staging table '{staging_table}'...")
    writer.write \
        .format("jdbc") \
        .options(**props) \
        .option("dbtable", staging_table) \
        .mode("overwrite") \
        .save()

    # Step 2 — atomic swap inside a transaction
    logger.info(f"Atomic swap: {staging_table} -> {table_name} for {target_date}")
    conn = _psycopg2_conn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table_name} WHERE metric_date = %s",
                (target_date,),
            )
            deleted = cur.rowcount
            cur.execute(
                f"INSERT INTO {table_name} SELECT * FROM {staging_table}"
            )
            inserted = cur.rowcount
        conn.commit()
        logger.info(
            f"Atomic write complete: deleted {deleted}, inserted {inserted} rows"
        )
    except Exception:
        conn.rollback()
        logger.error("Atomic write failed — rolled back")
        raise
    finally:
        # Clean up staging table
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
        except Exception:
            pass
        conn.close()
