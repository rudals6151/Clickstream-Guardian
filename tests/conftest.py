"""
공유 pytest fixtures

Spark 세션, 모의 DB 연결 등 테스트 전반에 걸쳐 사용되는 공통 fixture를 제공합니다.
"""
import os
import sys
import pytest

# --------------- path helpers ---------------
# 프로젝트 하위 디렉터리를 sys.path에 추가하여 직접 import 가능하게 합니다.

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

for subdir in ("api", "producers", "spark-batch", "spark-streaming", "airflow/dags", "scripts"):
    path = os.path.join(ROOT, subdir)
    if path not in sys.path:
        sys.path.insert(0, path)


# --------------- 환경변수 기본값 ---------------
# 테스트 실행 시 필수 환경변수가 없으면 기본값을 설정합니다.

_TEST_ENV_DEFAULTS = {
    "POSTGRES_PASSWORD": "test_password",
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "clickstream_test",
    "POSTGRES_USER": "admin",
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "S3_ENDPOINT": "http://localhost:9000",
}

for key, value in _TEST_ENV_DEFAULTS.items():
    os.environ.setdefault(key, value)
