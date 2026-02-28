"""
Unit tests for FastAPI endpoints.

Uses TestClient with mocked database connections so no running
PostgreSQL instance is required.
"""
import sys
import os
from datetime import datetime, date
from unittest.mock import patch, MagicMock

import pytest

try:
    from fastapi.testclient import TestClient
except ImportError:
    pytest.skip("FastAPI가 설치되지 않아 API 테스트를 건너뜁니다", allow_module_level=True)

# Ensure the api/ directory is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

# Patch the database pool *before* importing main, because database.py
# creates the pool at import time.
_mock_pool = MagicMock()
with patch.dict(os.environ, {
    "DATABASE_URL": "postgresql://admin:changeme@localhost:5432/clickstream",
}):
    with patch("psycopg2.pool.ThreadedConnectionPool", return_value=_mock_pool):
        from main import app  # noqa: E402

client = TestClient(app)


# --------------- helpers ---------------

def _fake_db():
    """Yield a mock connection with a mock cursor."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    yield conn


# --------------- root & health ---------------

class TestRootEndpoints:
    def test_root(self):
        resp = client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["message"] == "Clickstream Guardian API"
        assert "version" in data

    def test_health_ok(self):
        with patch("main.get_db_connection") as mock_ctx:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.fetchone.return_value = {"1": 1}
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            resp = client.get("/health")
            assert resp.status_code == 200
            assert resp.json()["status"] == "healthy"


# --------------- anomalies ---------------

class TestAnomalyEndpoints:
    def _override_db(self, rows):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_get_anomalies(self):
        fake_rows = [
            {
                "id": 1,
                "session_id": 100,
                "detected_at": datetime(2026, 1, 11, 12, 0, 0),
                "window_start": datetime(2026, 1, 11, 11, 59, 50),
                "window_end": datetime(2026, 1, 11, 12, 0, 0),
                "click_count": 60,
                "unique_items": 3,
                "anomaly_score": 1.2,
                "anomaly_type": "HIGH_FREQUENCY",
            }
        ]
        conn = self._override_db(fake_rows)
        from models.database import get_db
        app.dependency_overrides[get_db] = lambda: conn
        resp = client.get("/anomalies?limit=10")
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["anomaly_type"] == "HIGH_FREQUENCY"

    def test_get_anomaly_types(self):
        fake_rows = [
            {"anomaly_type": "HIGH_FREQUENCY", "count": 42, "avg_score": 1.5, "last_detected": "2026-01-11"}
        ]
        conn = self._override_db(fake_rows)
        from models.database import get_db
        app.dependency_overrides[get_db] = lambda: conn
        resp = client.get("/anomalies/types")
        app.dependency_overrides.clear()
        assert resp.status_code == 200


# --------------- metrics ---------------

class TestMetricsEndpoints:
    def _override_db(self, rows):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        cursor.fetchone.return_value = rows[0] if rows else None
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_get_daily_metrics(self):
        row = {
            "metric_date": date(2026, 1, 11),
            "total_clicks": 10000,
            "total_purchases": 500,
            "unique_sessions": 3000,
            "unique_items": 200,
            "conversion_rate": 0.05,
            "avg_session_duration_sec": 120.5,
            "avg_clicks_per_session": 3.33,
            "total_revenue": 25000,
            "avg_order_value": 50.0,
        }
        conn = self._override_db([row])
        from models.database import get_db
        app.dependency_overrides[get_db] = lambda: conn
        resp = client.get("/metrics/daily?start_date=2026-01-11&end_date=2026-01-11")
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["total_clicks"] == 10000

    def test_invalid_date_range(self):
        from models.database import get_db
        app.dependency_overrides[get_db] = lambda: MagicMock()
        resp = client.get("/metrics/daily?start_date=2026-01-12&end_date=2026-01-11")
        app.dependency_overrides.clear()
        assert resp.status_code == 400


# --------------- items ---------------

class TestItemsEndpoints:
    def _override_db(self, rows):
        conn = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_get_popular_items(self):
        fake_rows = [
            {
                "item_id": 42,
                "category": "electronics",
                "click_count": 500,
                "purchase_count": 50,
                "revenue": 10000,
                "rank": 1,
                "click_to_purchase_ratio": 10.0,
            }
        ]
        conn = self._override_db(fake_rows)
        from models.database import get_db
        app.dependency_overrides[get_db] = lambda: conn
        resp = client.get("/items/popular/2026-01-11?limit=5")
        app.dependency_overrides.clear()
        assert resp.status_code == 200
        data = resp.json()
        assert data[0]["item_id"] == 42

    def test_popular_items_404_when_empty(self):
        conn = self._override_db([])
        from models.database import get_db
        app.dependency_overrides[get_db] = lambda: conn
        resp = client.get("/items/popular/2026-01-11")
        app.dependency_overrides.clear()
        assert resp.status_code == 404
