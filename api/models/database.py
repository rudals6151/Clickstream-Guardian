"""
Database models and connection utilities

Uses a threaded connection pool to avoid per-request connection overhead.
"""
import atexit
import threading
import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from config import settings
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# --------------- connection pool ---------------
_parsed = urlparse(settings.DATABASE_URL)
_pool = psycopg2.pool.ThreadedConnectionPool(
    minconn=settings.DB_POOL_MIN,
    maxconn=settings.DB_POOL_MAX,
    host=_parsed.hostname or "postgres",
    port=int(_parsed.port or 5432),
    database=(_parsed.path or "/clickstream").lstrip("/"),
    user=_parsed.username,
    password=_parsed.password,
    cursor_factory=RealDictCursor,
)
_pool_slots = threading.BoundedSemaphore(settings.DB_POOL_MAX)


def _close_pool():
    """Close pool on application shutdown."""
    if _pool and not _pool.closed:
        _pool.closeall()
        logger.info("Database connection pool closed")


atexit.register(_close_pool)


@contextmanager
def get_db_connection():
    """Get a connection from the pool (context manager)."""
    acquired = _pool_slots.acquire(timeout=settings.DB_POOL_ACQUIRE_TIMEOUT)
    if not acquired:
        raise psycopg2.pool.PoolError(
            f"database connection pool acquire timed out after "
            f"{settings.DB_POOL_ACQUIRE_TIMEOUT}s"
        )

    conn = None
    try:
        conn = _pool.getconn()
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if conn is not None:
            _pool.putconn(conn)
        _pool_slots.release()


def get_db():
    """FastAPI dependency – yields a pooled connection."""
    with get_db_connection() as conn:
        yield conn
