# Clickstream Guardian
실시간 클릭스트림 이상 탐지와 일일 KPI 집계를 통합한 E2E 데이터 파이프라인 프로젝트

## Why
- 해결하려던 문제: 증가하는 트래픽 환경에서 이상 이벤트를 실시간으로 탐지하고, 장애 상황에서도 데이터 유실 없이 운영 품질을 보장
- 타겟 사용자: 데이터 엔지니어, 백엔드 엔지니어, 데이터 플랫폼 운영자
- 핵심 제약: 로컬 환경 재현 가능성, 장애/지연/중복 이벤트 대응, 운영 지표(SLO)로 품질 증명

## What
- 주요 기능(5~7개):
  - Kafka 기반 클릭스트림 이벤트 수집 (3 Broker 구성)
  - Spark Structured Streaming 실시간 이상 세션 탐지
  - Airflow + Spark Batch 일일 KPI 집계 자동화
  - FastAPI 조회 API 및 Streamlit 대시보드 제공
  - DLQ 모니터링 및 Slack/Email 알림 체계
  - 장애 주입/부하 테스트/스모크 테스트 스크립트 제공
  - SLO 리포트 생성으로 지연 구간별 성능 측정
- 스크린샷/데모 GIF:
  - Architecture: ![Architecture](diagram/Diagram.png)

## How
- Tech stack:
  - Streaming: Apache Kafka, Spark Structured Streaming
  - Batch/Orchestration: Apache Airflow, Spark Batch
  - Serving: FastAPI, PostgreSQL, Streamlit
  - Data Contract: Avro, Schema Registry
  - Infra: Docker Compose
  - Validation: Locust, smoke/failure/SLO scripts
- Architecture (짧게):
  - Producer -> Kafka -> Spark Streaming -> PostgreSQL -> FastAPI/Streamlit
  - Kafka Connect -> MinIO(S3) raw parquet 저장
  - Airflow DAG -> Spark Batch -> KPI 테이블 적재
  - DLQ Topic -> Airflow Sensor -> Slack/Email Alert
- Key decisions (3개):
  - 내구성 우선 Kafka 설정: `acks=all`, `enable.idempotence=true`, replication factor 2
  - 이벤트 정합성 보장: `event_id`, `event_ts`, `ingest_ts` 기반 지연/중복 대응 및 upsert 전략
  - 운영 자동화: DLQ 임계치 기반 경보와 `alert_history`로 중복 알림 방지

## Getting Started
- Requirements:
  - Docker Desktop
  - Python 3.10+
  - Bash 실행 환경 (Git Bash/WSL)
- Install:
  - `cp .env.example .env`
  - `.env`의 `POSTGRES_*`, `AIRFLOW__*`, `AWS_*` 값 확인
- Run:
  - `docker compose -f docker/docker-compose.yml build airflow-webserver airflow-scheduler`
  - `docker compose -f docker/docker-compose.yml up -d`
  - (선택) `bash scripts/apply_migration.sh`
- Test:
  - 스모크 테스트: `bash scripts/run_local_smoke.sh`
  - API 부하 테스트: `python -m locust -f scripts/load_test.py --host http://localhost:8000 --headless -u 15 -r 5 -t 20s --only-summary`
  - 장애 주입 테스트: `python scripts/failure_simulation.py --mode stop-start --down-seconds 20`

## Quality
- Tests:
  - API 스모크 테스트 및 엔드포인트 상태 검증 (`scripts/run_local_smoke.sh`, `scripts/smoke_test.py`)
  - 부하 테스트(Locust), 장애 주입 테스트, 처리량 테스트 스크립트 제공
- CI:
  - 현재 GitHub Actions 파이프라인은 미구성
- Lint/Format:
  - 전용 lint/format 자동화는 미구성 (추후 `ruff`/`black` 도입 권장)

## Links
- Demo:
  - API: `http://localhost:8000`
  - API Docs: `http://localhost:8000/docs`
  - Dashboard: `http://localhost:8501`
- Portfolio Case Study:
  - [프로젝트 요약](docs/PORTFOLIO_PROJECT_SUMMARY.md)
  - [테스트 실행 보고서 (2026-02-22)](docs/TEST_EXECUTION_REPORT_2026-02-22.md)
- PR highlights:
  - 핵심 변경사항 정리 예정 (업데이트 예정)
