"""
Database models and connection utilities
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from config import settings
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


@contextmanager
def get_db_connection():
    """Get database connection with context manager"""
    conn = None
    try:
        parsed = urlparse(settings.DATABASE_URL)
        user = parsed.username
        password = parsed.password
        host = parsed.hostname or "postgres"
        port = parsed.port or 5432
        database = (parsed.path or "/clickstream").lstrip("/")
        
        conn = psycopg2.connect(
            host=host,
            port=int(port),
            database=database,
            user=user,
            password=password,
            cursor_factory=RealDictCursor
        )
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()


def get_db():
    """Dependency for getting database connection"""
    with get_db_connection() as conn:
        yield conn
