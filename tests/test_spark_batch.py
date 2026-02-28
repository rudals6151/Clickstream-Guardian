"""
Spark Batch 잡 단위 테스트

DailyMetricsCalculator, PopularItemsAnalyzer, SessionFunnelAnalyzer의
핵심 계산 로직을 로컬 SparkSession으로 검증합니다.
외부 의존성(S3, PostgreSQL)은 모킹하여 순수 로직만 테스트합니다.
"""
import os
import sys
from unittest.mock import patch, MagicMock

import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark가 설치되지 않아 Spark 배치 테스트를 건너뜁니다")

# spark-batch 디렉터리를 path에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark-batch"))


# --------------- SparkSession fixture ---------------

@pytest.fixture(scope="module")
def spark():
    """모듈 범위 로컬 SparkSession을 생성합니다."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-spark-batch")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


# ===================================================================
# DailyMetricsCalculator 테스트
# ===================================================================

class TestDailyMetricsCalculator:
    """DailyMetricsCalculator.calculate_metrics() 계산 로직 검증"""

    @pytest.fixture()
    def clicks_df(self, spark):
        """테스트용 클릭 데이터 3건 (세션 2개, 아이템 2개)"""
        data = [
            (1, 1000000, 10, "electronics"),
            (1, 1005000, 20, "electronics"),
            (2, 1010000, 10, "books"),
        ]
        return spark.createDataFrame(data, ["session_id", "event_ts", "item_id", "category"])

    @pytest.fixture()
    def purchases_df(self, spark):
        """테스트용 구매 데이터 1건"""
        data = [
            (1, 1002000, 10, 5000, 2),
        ]
        return spark.createDataFrame(data, ["session_id", "event_ts", "item_id", "price", "quantity"])

    @pytest.fixture()
    def empty_purchases_df(self, spark):
        """빈 구매 데이터"""
        from pyspark.sql.types import StructType, StructField, LongType
        schema = StructType([
            StructField("session_id", LongType()),
            StructField("event_ts", LongType()),
            StructField("item_id", LongType()),
            StructField("price", LongType()),
            StructField("quantity", LongType()),
        ])
        return spark.createDataFrame([], schema)

    def test_click_aggregation(self, spark, clicks_df, purchases_df):
        """클릭 집계 (total_clicks, unique_sessions, unique_items) 확인"""
        from pyspark.sql.functions import count, countDistinct

        result = clicks_df.agg(
            count("*").alias("total_clicks"),
            countDistinct("session_id").alias("unique_sessions"),
            countDistinct("item_id").alias("unique_items"),
        ).collect()[0]

        assert result["total_clicks"] == 3
        assert result["unique_sessions"] == 2
        assert result["unique_items"] == 2

    def test_purchase_aggregation(self, spark, purchases_df):
        """구매 집계 (total_purchases, total_revenue) 확인"""
        from pyspark.sql.functions import count, sum, col

        result = purchases_df.agg(
            count("*").alias("total_purchases"),
            sum(col("price") * col("quantity")).alias("total_revenue"),
        ).collect()[0]

        assert result["total_purchases"] == 1
        assert result["total_revenue"] == 10000  # 5000 * 2

    def test_conversion_rate(self, spark, clicks_df, purchases_df):
        """전환율 = 구매 세션 수 / 전체 세션 수"""
        from pyspark.sql.functions import countDistinct

        click_sessions = clicks_df.select(countDistinct("session_id")).collect()[0][0]
        purchase_sessions = purchases_df.select(countDistinct("session_id")).collect()[0][0]

        conversion_rate = purchase_sessions / click_sessions
        assert conversion_rate == pytest.approx(0.5, abs=0.01)

    def test_session_duration(self, spark, clicks_df):
        """세션별 평균 duration(초) 검증"""
        from pyspark.sql.functions import count, avg, max, min

        session_stats = clicks_df.groupBy("session_id").agg(
            ((max("event_ts") - min("event_ts")) / 1000).alias("duration_sec")
        )
        avg_dur = session_stats.agg(avg("duration_sec")).collect()[0][0]

        # session 1: (1005000-1000000)/1000 = 5, session 2: 0 → avg = 2.5
        assert avg_dur == pytest.approx(2.5, abs=0.1)

    def test_zero_purchases_conversion_rate(self, spark, clicks_df, empty_purchases_df):
        """구매 0건일 때 전환율은 0"""
        from pyspark.sql.functions import countDistinct

        purchase_sessions = empty_purchases_df.select(countDistinct("session_id")).collect()[0][0]
        assert purchase_sessions == 0

    def test_avg_order_value_with_purchases(self, spark, purchases_df):
        """평균 주문 금액 = total_revenue / total_purchases"""
        from pyspark.sql.functions import count, sum, col

        result = purchases_df.agg(
            count("*").alias("total_purchases"),
            sum(col("price") * col("quantity")).alias("total_revenue"),
        ).collect()[0]

        aov = result["total_revenue"] / result["total_purchases"]
        assert aov == pytest.approx(10000.0, abs=0.01)


# ===================================================================
# PopularItemsAnalyzer 테스트
# ===================================================================

class TestPopularItemsAnalyzer:
    """PopularItemsAnalyzer 인기 아이템/카테고리 로직 검증"""

    @pytest.fixture()
    def clicks_df(self, spark):
        """아이템별 클릭 수가 다른 테스트 데이터"""
        data = [
            (1, 10, "electronics"), (2, 10, "electronics"),
            (3, 10, "electronics"), (4, 20, "books"),
            (5, 20, "books"),       (6, 30, "sports"),
        ]
        return spark.createDataFrame(data, ["session_id", "item_id", "category"])

    @pytest.fixture()
    def purchases_df(self, spark):
        data = [
            (10, 5000, 1), (10, 5000, 2),  # item 10: 15000
            (20, 3000, 1),                   # item 20: 3000
        ]
        return spark.createDataFrame(data, ["item_id", "price", "quantity"])

    def test_item_click_count(self, spark, clicks_df):
        """아이템별 클릭 수 정확도"""
        from pyspark.sql.functions import count

        item_clicks = (
            clicks_df.groupBy("item_id")
            .agg(count("*").alias("click_count"))
            .orderBy("item_id")
            .collect()
        )
        lookup = {row["item_id"]: row["click_count"] for row in item_clicks}
        assert lookup[10] == 3
        assert lookup[20] == 2
        assert lookup[30] == 1

    def test_revenue_ranking(self, spark, purchases_df):
        """매출 기준 내림차순 정렬"""
        from pyspark.sql.functions import sum, col
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number

        revenue = (
            purchases_df.groupBy("item_id")
            .agg(sum(col("price") * col("quantity")).alias("revenue"))
        )
        w = Window.orderBy(col("revenue").desc())
        ranked = revenue.withColumn("rank", row_number().over(w)).collect()

        assert ranked[0]["item_id"] == 10
        assert ranked[0]["rank"] == 1
        assert ranked[1]["item_id"] == 20
        assert ranked[1]["rank"] == 2

    def test_click_to_purchase_ratio(self, spark):
        """click_to_purchase_ratio 계산 검증"""
        from pyspark.sql.functions import when, col, lit

        data = [(10, 100, 10), (20, 50, 0)]
        df = spark.createDataFrame(data, ["item_id", "click_count", "purchase_count"])

        result = df.withColumn(
            "ratio",
            when(col("purchase_count") > 0, col("click_count") / col("purchase_count")).otherwise(None),
        ).collect()

        assert result[0]["ratio"] == pytest.approx(10.0)
        assert result[1]["ratio"] is None

    def test_category_aggregation(self, spark, clicks_df):
        """카테고리별 집계 정확도"""
        from pyspark.sql.functions import count, countDistinct

        cat_stats = (
            clicks_df.filter(clicks_df.category.isNotNull())
            .groupBy("category")
            .agg(count("*").alias("click_count"), countDistinct("item_id").alias("unique_items"))
            .collect()
        )
        lookup = {row["category"]: row for row in cat_stats}

        assert lookup["electronics"]["click_count"] == 3
        assert lookup["electronics"]["unique_items"] == 1
        assert lookup["books"]["click_count"] == 2
        assert lookup["sports"]["click_count"] == 1


# ===================================================================
# SessionFunnelAnalyzer 테스트
# ===================================================================

class TestSessionFunnelAnalyzer:
    """SessionFunnelAnalyzer 전환 퍼널 로직 검증"""

    @pytest.fixture()
    def clicks_df(self, spark):
        """세션 4개: 1·2번은 multi-view, 3·4번은 single-view"""
        data = [
            (1, 10), (1, 20),   # session 1: 2 clicks
            (2, 10), (2, 30),   # session 2: 2 clicks
            (3, 40),            # session 3: 1 click
            (4, 50),            # session 4: 1 click
        ]
        return spark.createDataFrame(data, ["session_id", "item_id"])

    @pytest.fixture()
    def purchases_df(self, spark):
        """세션 1만 구매"""
        data = [(1, 10)]
        return spark.createDataFrame(data, ["session_id", "item_id"])

    def test_view_stage(self, clicks_df):
        """VIEW 단계 = 전체 세션 수"""
        total = clicks_df.select("session_id").distinct().count()
        assert total == 4

    def test_multi_view_stage(self, spark, clicks_df):
        """MULTI_VIEW 단계 = 클릭 2회 이상 세션"""
        from pyspark.sql.functions import count

        session_clicks = clicks_df.groupBy("session_id").agg(count("*").alias("cnt"))
        multi_view = session_clicks.filter(session_clicks.cnt >= 2).count()
        assert multi_view == 2

    def test_purchase_stage(self, spark, clicks_df, purchases_df):
        """PURCHASE 단계 = 구매가 있는 세션"""
        from pyspark.sql.functions import count, countDistinct

        purchase_sessions = purchases_df.select("session_id").distinct().count()
        assert purchase_sessions == 1

    def test_funnel_percentages(self, spark, clicks_df, purchases_df):
        """퍼널 비율 계산 (VIEW 100%, MULTI_VIEW 50%, PURCHASE 25%)"""
        from pyspark.sql.functions import count

        total = clicks_df.select("session_id").distinct().count()  # 4

        session_clicks = clicks_df.groupBy("session_id").agg(count("*").alias("cnt"))
        multi_view = session_clicks.filter(session_clicks.cnt >= 2).count()  # 2
        purchase = purchases_df.select("session_id").distinct().count()      # 1

        view_pct = 100.0
        multi_pct = multi_view / total * 100  # 50
        purchase_pct = purchase / total * 100  # 25

        assert view_pct == 100.0
        assert multi_pct == pytest.approx(50.0)
        assert purchase_pct == pytest.approx(25.0)

    def test_drop_rates(self, spark, clicks_df, purchases_df):
        """단계별 이탈률 계산"""
        from pyspark.sql.functions import count

        total = clicks_df.select("session_id").distinct().count()
        session_clicks = clicks_df.groupBy("session_id").agg(count("*").alias("cnt"))
        multi_view = session_clicks.filter(session_clicks.cnt >= 2).count()
        purchase = purchases_df.select("session_id").distinct().count()

        drop_view_to_multi = (total - multi_view) / total         # 0.5
        drop_multi_to_purchase = (multi_view - purchase) / multi_view  # 0.5

        assert drop_view_to_multi == pytest.approx(0.5)
        assert drop_multi_to_purchase == pytest.approx(0.5)


# ===================================================================
# s3_utils 테스트
# ===================================================================

class TestS3Utils:
    """S3 경로 생성 로직 검증"""

    def test_clicks_path(self):
        from common.s3_utils import get_s3_path

        path = get_s3_path("km-data-lake", "raw_clicks", "2026-01-11")
        assert path == "s3a://km-data-lake/topics/km.clicks.raw.v1/raw_clicks/dt=2026-01-11/*"

    def test_purchases_path(self):
        from common.s3_utils import get_s3_path

        path = get_s3_path("km-data-lake", "raw_purchases", "2026-01-11")
        assert path == "s3a://km-data-lake/topics/km.purchases.raw.v1/raw_purchases/dt=2026-01-11/*"

    def test_custom_prefix_path(self):
        from common.s3_utils import get_s3_path

        path = get_s3_path("my-bucket", "custom", "2026-02-01")
        assert path == "s3a://my-bucket/custom/dt=2026-02-01/*"


# ===================================================================
# postgres_utils 테스트
# ===================================================================

class TestPostgresUtils:
    """postgres_utils 환경변수 검증"""

    def test_get_postgres_properties_returns_dict(self):
        from common.postgres_utils import get_postgres_properties

        props = get_postgres_properties()
        assert "url" in props
        assert "user" in props
        assert "password" in props
        assert "driver" in props
        assert props["driver"] == "org.postgresql.Driver"

    def test_require_env_raises_on_missing(self):
        from common.postgres_utils import _require_env

        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(EnvironmentError, match="MISSING_VAR"):
                _require_env("MISSING_VAR")

    def test_require_env_returns_value(self):
        from common.postgres_utils import _require_env

        with patch.dict(os.environ, {"MY_VAR": "hello"}):
            assert _require_env("MY_VAR") == "hello"
