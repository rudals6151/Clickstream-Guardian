"""
Unit tests for producer utility functions.

Tests pure logic (parsing, event ID generation) without requiring
a running Kafka cluster.
"""
import sys
import os

import pytest

# Ensure the producers/ directory is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producers"))

from producer_clicks import make_event_id, _get_first


# --------------- make_event_id ---------------

class TestMakeEventId:
    def test_deterministic(self):
        """Same input always produces the same hash."""
        id1 = make_event_id(100, 42, 1700000000000, "click")
        id2 = make_event_id(100, 42, 1700000000000, "click")
        assert id1 == id2

    def test_different_inputs(self):
        """Different inputs produce different hashes."""
        id1 = make_event_id(100, 42, 1700000000000, "click")
        id2 = make_event_id(101, 42, 1700000000000, "click")
        assert id1 != id2

    def test_returns_hex_string(self):
        """Output is a 64-character hex SHA-256 digest."""
        eid = make_event_id(1, 2, 3, "click")
        assert len(eid) == 64
        assert all(c in "0123456789abcdef" for c in eid)


# --------------- _get_first ---------------

class TestGetFirst:
    def test_returns_first_matching_key(self):
        row = {"Timestamp": "2026-01-11T00:00:00", "timestamp": "ignored"}
        assert _get_first(row, ["Timestamp", "timestamp"]) == "2026-01-11T00:00:00"

    def test_skips_empty_values(self):
        row = {"Timestamp": "", "timestamp": "2026-01-11"}
        assert _get_first(row, ["Timestamp", "timestamp"]) == "2026-01-11"

    def test_returns_none_when_missing(self):
        row = {"other_key": "value"}
        assert _get_first(row, ["Timestamp", "timestamp"]) is None

    def test_skips_none_values(self):
        row = {"ts": None, "event_ts": 12345}
        assert _get_first(row, ["ts", "event_ts"]) == 12345


# --------------- parse_row (integration-ish) ---------------

class TestParseRow:
    """
    Tests parse_row via the ImprovedClickProducer class.
    Requires mocking Kafka/SchemaRegistry dependencies so we don't
    need a running broker.
    """

    @pytest.fixture()
    def producer(self):
        """Create an ImprovedClickProducer with mocked Kafka."""
        from unittest.mock import patch, MagicMock

        with patch("producer_clicks.SchemaRegistry") as MockSR, \
             patch("producer_clicks.Producer") as MockProducer:
            MockSR.return_value = MagicMock()
            MockProducer.return_value = MagicMock()
            from producer_clicks import ImprovedClickProducer
            p = ImprovedClickProducer()
            return p

    def test_valid_row(self, producer):
        row = {
            "Timestamp": "2026-01-11T10:00:00+00:00",
            "Session ID": "12345",
            "Item ID": "42",
            "Category": "electronics",
        }
        event = producer.parse_row(row)
        assert event is not None
        assert event["session_id"] == 12345
        assert event["item_id"] == 42
        assert event["category"] == "electronics"
        assert event["event_type"] == "click"
        assert event["source"] == "producer_clicks"

    def test_missing_required_field(self, producer):
        row = {"Timestamp": "2026-01-11T10:00:00+00:00"}
        event = producer.parse_row(row)
        assert event is None

    def test_category_zero_treated_as_none(self, producer):
        row = {
            "Timestamp": "2026-01-11T10:00:00+00:00",
            "Session ID": "100",
            "Item ID": "1",
            "Category": "0",
        }
        event = producer.parse_row(row)
        assert event is not None
        assert event["category"] is None
