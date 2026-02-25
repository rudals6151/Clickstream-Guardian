"""
Database models and connection utilities

Uses a threaded connection pool to avoid per-request connection overhead.
"""
import atexit
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
    minconn=2,
    maxconn=10,
    host=_parsed.hostname or "postgres",
    port=int(_parsed.port or 5432),
    database=(_parsed.path or "/clickstream").lstrip("/"),
    user=_parsed.username,
    password=_parsed.password,
    cursor_factory=RealDictCursor,
)


def _close_pool():
    """Close pool on application shutdown."""
    if _pool and not _pool.closed:
        _pool.closeall()
        logger.info("Database connection pool closed")


atexit.register(_close_pool)


@contextmanager
def get_db_connection():
    """Get a connection from the pool (context manager)."""
    conn = _pool.getconn()
    try:
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        _pool.putconn(conn)


def get_db():
    """FastAPI dependency – yields a pooled connection."""
    with get_db_connection() as conn:
        yield conn
