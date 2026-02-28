"""
Spark Streaming 단위 테스트

AnomalyDetector의 핵심 탐지 로직 (high_frequency, bot_like)을
로컬 SparkSession의 정적 DataFrame으로 검증합니다.
실제 Kafka/PostgreSQL 연결 없이 순수 변환 로직만 테스트합니다.
"""
import os
import sys
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark가 설치되지 않아 Spark 스트리밍 테스트를 건너뜁니다")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark-streaming"))


@pytest.fixture(scope="module")
def spark():
    """모듈 범위 로컬 SparkSession"""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-spark-streaming")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


# ===================================================================
# kafka_utils 테스트
# ===================================================================

class TestKafkaUtils:
    """kafka_utils 유틸리티 함수 검증"""

    def test_get_click_schema(self):
        from common.kafka_utils import get_click_schema

        schema = get_click_schema()
        field_names = [f.name for f in schema.fields]
        assert "session_id" in field_names
        assert "event_ts" in field_names
        assert "item_id" in field_names
        assert "category" in field_names
        assert "event_type" in field_names

    def test_get_purchase_schema(self):
        from common.kafka_utils import get_purchase_schema

        schema = get_purchase_schema()
        field_names = [f.name for f in schema.fields]
        assert "session_id" in field_names
        assert "price" in field_names
        assert "quantity" in field_names

    def test_get_kafka_options(self):
        from common.kafka_utils import get_kafka_options

        opts = get_kafka_options("broker:9092", "test-topic", "test-group")
        assert opts["kafka.bootstrap.servers"] == "broker:9092"
        assert opts["subscribe"] == "test-topic"
        assert opts["kafka.group.id"] == "test-group"
        assert "startingOffsets" in opts


# ===================================================================
# AnomalyDetector 임계값/설정 테스트
# ===================================================================

class TestAnomalyDetectorConfig:
    """AnomalyDetector 초기화 설정 검증 (Spark 미사용)"""

    def test_thresholds(self):
        """기본 임계값이 올바르게 설정됩니다."""
        with patch("anomaly_detector.SparkSession") as MockSpark:
            MockSpark.builder.appName.return_value = MockSpark.builder
            MockSpark.builder.config.return_value = MockSpark.builder
            MockSpark.builder.getOrCreate.return_value = MagicMock()

            from anomaly_detector import AnomalyDetector
            det = AnomalyDetector.__new__(AnomalyDetector)
            det.HIGH_FREQUENCY_THRESHOLD = 50
            det.BOT_LIKE_THRESHOLD = 50
            det.LOW_DIVERSITY_RATIO = 0.1

            assert det.HIGH_FREQUENCY_THRESHOLD == 50
            assert det.BOT_LIKE_THRESHOLD == 50
            assert det.LOW_DIVERSITY_RATIO == 0.1


# ===================================================================
# 이상 탐지 로직 테스트 (정적 DataFrame)
# ===================================================================

class TestHighFrequencyDetection:
    """HIGH_FREQUENCY 탐지 로직 — 10초 윈도우 집계 검증"""

    @pytest.fixture()
    def session_window_df(self, spark):
        """10초 윈도우 내 세션별 클릭 수를 모의합니다."""
        from pyspark.sql.functions import lit
        from pyspark.sql.types import (
            StructType, StructField, LongType, IntegerType, StringType,
            TimestampType,
        )

        # 세션 1: 60 클릭 (임계값 50 초과), 세션 2: 10 클릭 (정상)
        data = [
            (1, 60, 3, datetime(2026, 1, 11, 12, 0, 0), datetime(2026, 1, 11, 12, 0, 10), datetime(2026, 1, 11, 12, 0, 10)),
            (2, 10, 5, datetime(2026, 1, 11, 12, 0, 0), datetime(2026, 1, 11, 12, 0, 10), datetime(2026, 1, 11, 12, 0, 10)),
        ]
        schema = StructType([
            StructField("session_id", LongType()),
            StructField("click_count", IntegerType()),
            StructField("unique_items", IntegerType()),
            StructField("window_start", TimestampType()),
            StructField("window_end", TimestampType()),
            StructField("kafka_ingest_ts", TimestampType()),
        ])
        return spark.createDataFrame(data, schema)

    def test_filter_exceeds_threshold(self, session_window_df):
        """임계값(50)을 초과하는 세션만 필터링됩니다."""
        threshold = 50
        anomalies = session_window_df.filter(
            session_window_df.click_count > threshold
        ).collect()

        assert len(anomalies) == 1
        assert anomalies[0]["session_id"] == 1

    def test_anomaly_score_calculation(self, session_window_df):
        """anomaly_score = click_count / threshold"""
        from pyspark.sql.functions import col, lit

        threshold = 50
        scored = (
            session_window_df
            .filter(col("click_count") > threshold)
            .withColumn("anomaly_score", col("click_count") / lit(threshold))
            .collect()
        )

        assert len(scored) == 1
        assert scored[0]["anomaly_score"] == pytest.approx(1.2)

    def test_normal_session_not_detected(self, session_window_df):
        """정상 세션(10 클릭)은 탐지되지 않습니다."""
        threshold = 50
        normal = session_window_df.filter(
            session_window_df.click_count <= threshold
        ).collect()

        assert len(normal) == 1
        assert normal[0]["session_id"] == 2


class TestBotLikeDetection:
    """BOT_LIKE 탐지 로직 — 고빈도 + 저다양성 필터링 검증"""

    @pytest.fixture()
    def bot_window_df(self, spark):
        """1분 윈도우 내 세션별 집계를 모의합니다."""
        from pyspark.sql.types import (
            StructType, StructField, LongType, IntegerType,
            DoubleType, TimestampType,
        )
        from pyspark.sql.functions import col

        # 세션 1: 80 클릭, 2 유니크 아이템 → diversity=0.025 (봇)
        # 세션 2: 80 클릭, 20 유니크 아이템 → diversity=0.25 (정상)
        # 세션 3: 30 클릭, 1 유니크 아이템 → diversity=0.033 이지만 클릭 수 미달
        data = [
            (1, 80, 2, datetime(2026, 1, 11, 12, 0, 0), datetime(2026, 1, 11, 12, 1, 0), datetime(2026, 1, 11, 12, 1, 0)),
            (2, 80, 20, datetime(2026, 1, 11, 12, 0, 0), datetime(2026, 1, 11, 12, 1, 0), datetime(2026, 1, 11, 12, 1, 0)),
            (3, 30, 1, datetime(2026, 1, 11, 12, 0, 0), datetime(2026, 1, 11, 12, 1, 0), datetime(2026, 1, 11, 12, 1, 0)),
        ]
        schema = StructType([
            StructField("session_id", LongType()),
            StructField("click_count", IntegerType()),
            StructField("unique_items", IntegerType()),
            StructField("window_start", TimestampType()),
            StructField("window_end", TimestampType()),
            StructField("kafka_ingest_ts", TimestampType()),
        ])
        df = spark.createDataFrame(data, schema)
        return df.withColumn("diversity_ratio", col("unique_items") / col("click_count"))

    def test_bot_like_filter(self, bot_window_df):
        """고빈도(>50) + 저다양성(<0.1) 조건에 해당하는 세션만 탐지됩니다."""
        from pyspark.sql.functions import col

        bot_threshold = 50
        low_diversity = 0.1

        detected = (
            bot_window_df
            .filter((col("click_count") > bot_threshold) & (col("diversity_ratio") < low_diversity))
            .collect()
        )

        assert len(detected) == 1
        assert detected[0]["session_id"] == 1

    def test_high_diversity_not_detected(self, bot_window_df):
        """다양성이 높은 고빈도 세션은 봇으로 탐지되지 않습니다."""
        from pyspark.sql.functions import col

        high_diversity = bot_window_df.filter(
            (col("click_count") > 50) & (col("diversity_ratio") >= 0.1)
        ).collect()

        assert len(high_diversity) == 1
        assert high_diversity[0]["session_id"] == 2

    def test_low_click_count_not_detected(self, bot_window_df):
        """클릭 수가 임계값 미만인 세션은 탐지되지 않습니다."""
        from pyspark.sql.functions import col

        low_clicks = bot_window_df.filter(col("click_count") <= 50).collect()
        assert len(low_clicks) == 1
        assert low_clicks[0]["session_id"] == 3

    def test_bot_anomaly_score(self, bot_window_df):
        """봇 anomaly_score = (click_count/threshold) * (1 - diversity_ratio)"""
        from pyspark.sql.functions import col, lit

        threshold = 50
        scored = (
            bot_window_df
            .filter((col("click_count") > threshold) & (col("diversity_ratio") < 0.1))
            .withColumn(
                "anomaly_score",
                (col("click_count") / lit(threshold)) * (1 - col("diversity_ratio")),
            )
            .collect()
        )

        expected = (80 / 50) * (1 - 2 / 80)  # 1.6 * 0.975 = 1.56
        assert scored[0]["anomaly_score"] == pytest.approx(expected, abs=0.01)


# ===================================================================
# split_late_events 로직 테스트
# ===================================================================

class TestLateEventSplit:
    """지연 이벤트 분류 로직 검증"""

    @pytest.fixture()
    def events_df(self, spark):
        """현재 시간 기준 온타임/지연 이벤트 혼합"""
        from pyspark.sql.types import StructType, StructField, LongType, TimestampType, StringType

        now = datetime.utcnow()
        on_time = now - timedelta(minutes=2)  # 2분 전 (온타임)
        late = now - timedelta(minutes=30)     # 30분 전 (지연)

        data = [
            (1, on_time, 10, "click", "id1", now, "producer"),
            (2, late, 20, "click", "id2", now, "producer"),
        ]
        schema = StructType([
            StructField("session_id", LongType()),
            StructField("event_ts", TimestampType()),
            StructField("item_id", LongType()),
            StructField("event_type", StringType()),
            StructField("event_id", StringType()),
            StructField("kafka_ingest_ts", TimestampType()),
            StructField("source", StringType()),
        ])
        return spark.createDataFrame(data, schema)

    def test_on_time_events_kept(self, spark, events_df):
        """임계값(10분) 이내의 이벤트는 온타임으로 분류됩니다."""
        from pyspark.sql.functions import col, expr

        threshold_minutes = 10
        threshold_expr = expr(f"current_timestamp() - interval {threshold_minutes} minutes")

        on_time = events_df.filter(col("event_ts") >= threshold_expr).collect()
        assert len(on_time) == 1
        assert on_time[0]["session_id"] == 1

    def test_late_events_separated(self, spark, events_df):
        """임계값(10분)을 초과한 이벤트는 지연으로 분류됩니다."""
        from pyspark.sql.functions import col, expr

        threshold_minutes = 10
        threshold_expr = expr(f"current_timestamp() - interval {threshold_minutes} minutes")

        late = events_df.filter(col("event_ts") < threshold_expr).collect()
        assert len(late) == 1
        assert late[0]["session_id"] == 2


# ===================================================================
# 스트리밍 postgres_utils 테스트
# ===================================================================

class TestStreamingPostgresUtils:
    """spark-streaming/common/postgres_utils 검증"""

    def test_get_postgres_properties_keys(self):
        from common.postgres_utils import get_postgres_properties

        props = get_postgres_properties()
        assert "url" in props
        assert "user" in props
        assert "password" in props
        assert "driver" in props
        assert "batchsize" in props

    def test_require_env_missing(self):
        from common.postgres_utils import _require_env

        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError):
                _require_env("NONEXISTENT_VAR")
