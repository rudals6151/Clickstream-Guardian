"""
Database models and connection utilities
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from config import settings
import logging

logger = logging.getLogger(__name__)


@contextmanager
def get_db_connection():
    """Get database connection with context manager"""
    conn = None
    try:
        # Parse DATABASE_URL
        db_url = settings.DATABASE_URL.replace("postgresql://", "")
        user_pass, host_db = db_url.split("@")
        user, password = user_pass.split(":")
        host_port, database = host_db.split("/")
        host, port = host_port.split(":") if ":" in host_port else (host_port, "5432")
        
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
