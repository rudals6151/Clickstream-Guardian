"""
Airflow DAG 구조 및 임포트 검증 테스트

DAG 파일이 에러 없이 import 되는지, 태스크 의존성이 올바른지,
스케줄 설정이 기대값과 일치하는지를 검증합니다.

NOTE: 이 테스트는 실제 Airflow 환경(DB, Variable 등)을 요구하지 않으며
      필요한 부분만 모킹합니다.
"""
import os
import sys
from datetime import timedelta
from unittest.mock import patch, MagicMock

import pytest

try:
    from airflow import DAG  # noqa: F401
except (ImportError, AttributeError):
    pytest.skip("Airflow가 설치되지 않아 DAG 테스트를 건너뜁니다", allow_module_level=True)

# 경로 추가
DAG_DIR = os.path.join(os.path.dirname(__file__), "..", "airflow", "dags")
sys.path.insert(0, DAG_DIR)


# ===================================================================
# spark_batch_dag 테스트
# ===================================================================

class TestSparkBatchDAG:
    """spark_batch_processing DAG 구조 검증"""

    @pytest.fixture(scope="class")
    def dag(self):
        """spark_batch_dag를 import하여 DAG 객체를 반환합니다."""
        import importlib
        mod = importlib.import_module("spark_batch_dag")
        return mod.dag

    def test_dag_loaded(self, dag):
        """DAG가 정상적으로 로드됩니다."""
        assert dag is not None
        assert dag.dag_id == "spark_batch_processing"

    def test_schedule_interval(self, dag):
        """스케줄이 매일 UTC 18시(KST 03시)로 설정되어 있습니다."""
        assert dag.schedule_interval == "0 18 * * *"

    def test_tags(self, dag):
        """tags에 spark, batch, analytics가 포함되어 있습니다."""
        assert set(dag.tags) == {"spark", "batch", "analytics"}

    def test_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_task_count(self, dag):
        """6개 태스크(start, record_start, daily_metrics, popular_items, session_funnel, record_end, end)"""
        # start + record_start + 3 spark jobs + record_end + end = 7
        assert len(dag.tasks) == 7

    def test_task_ids_present(self, dag):
        """필수 태스크 ID가 모두 존재합니다."""
        task_ids = {t.task_id for t in dag.tasks}
        expected = {"start", "record_start", "daily_metrics", "popular_items",
                    "session_funnel", "record_end", "end"}
        assert expected == task_ids

    def test_start_upstream_of_record_start(self, dag):
        """start → record_start 의존성"""
        record_start = dag.get_task("record_start")
        assert "start" in record_start.upstream_task_ids

    def test_record_start_upstream_of_daily_metrics(self, dag):
        """record_start → daily_metrics 의존성"""
        daily = dag.get_task("daily_metrics")
        assert "record_start" in daily.upstream_task_ids

    def test_daily_metrics_upstream_of_parallel_tasks(self, dag):
        """daily_metrics → [popular_items, session_funnel] 의존성"""
        popular = dag.get_task("popular_items")
        funnel = dag.get_task("session_funnel")
        assert "daily_metrics" in popular.upstream_task_ids
        assert "daily_metrics" in funnel.upstream_task_ids

    def test_parallel_tasks_upstream_of_record_end(self, dag):
        """[popular_items, session_funnel] → record_end 의존성"""
        record_end = dag.get_task("record_end")
        assert "popular_items" in record_end.upstream_task_ids
        assert "session_funnel" in record_end.upstream_task_ids

    def test_record_end_upstream_of_end(self, dag):
        """record_end → end 의존성"""
        end = dag.get_task("end")
        assert "record_end" in end.upstream_task_ids

    def test_default_args_retries(self, dag):
        """기본 retry 횟수가 1입니다."""
        assert dag.default_args.get("retries") == 1

    def test_default_args_retry_delay(self, dag):
        """retry 대기 시간이 5분입니다."""
        assert dag.default_args.get("retry_delay") == timedelta(minutes=5)


# ===================================================================
# dlq_alert_dag 테스트
# ===================================================================

class TestDLQAlertDAG:
    """dlq_alert_monitoring DAG 구조 검증"""

    @pytest.fixture(scope="class")
    def dag(self):
        # dlq_alert_dag는 Airflow Variable을 사용하므로 모킹 필요
        with patch("airflow.models.Variable.get", side_effect=lambda k, default_var=None: default_var):
            import importlib
            mod = importlib.import_module("dlq_alert_dag")
            return mod.dag

    def test_dag_loaded(self, dag):
        assert dag is not None
        assert dag.dag_id == "dlq_alert_dag"

    def test_schedule_interval(self, dag):
        """DLQ 모니터링은 매분 실행됩니다."""
        assert dag.schedule_interval == "*/1 * * * *"

    def test_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_has_sensor_task(self, dag):
        """DLQ offset 센서 태스크가 존재합니다."""
        task_ids = {t.task_id for t in dag.tasks}
        assert "detect_dlq_growth" in task_ids

    def test_tags(self, dag):
        assert "dlq" in dag.tags


# ===================================================================
# partition_manager_dag 테스트
# ===================================================================

class TestPartitionManagerDAG:
    """partition_manager DAG 구조 검증"""

    @pytest.fixture(scope="class")
    def dag(self):
        import importlib
        mod = importlib.import_module("partition_manager_dag")
        return mod.dag

    def test_dag_loaded(self, dag):
        assert dag is not None
        assert dag.dag_id == "partition_manager"

    def test_schedule_daily(self, dag):
        """파티션 관리는 매일 자정(UTC)에 실행됩니다."""
        assert dag.schedule_interval == "0 0 * * *"

    def test_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_task_ids(self, dag):
        task_ids = {t.task_id for t in dag.tasks}
        assert "create_partitions" in task_ids
        assert "cleanup_partitions" in task_ids

    def test_create_before_cleanup(self, dag):
        """파티션 생성이 정리보다 먼저 실행됩니다."""
        cleanup = dag.get_task("cleanup_partitions")
        assert "create_partitions" in cleanup.upstream_task_ids

    def test_tags(self, dag):
        assert "maintenance" in dag.tags
