# 테스트 실행 리포트

작성일: 2026-02-22 KST  
실행 환경: Windows, Docker Compose, 로컬 서비스  
Compose 파일: `docker/docker-compose.yml`

## 실행 범위

이 문서는 Clickstream Guardian 파이프라인의 로컬 E2E 검증 결과를 정리한 리포트입니다.

검증 항목:

- Docker 서비스 빌드 및 기동
- PostgreSQL schema migration
- FastAPI smoke test
- Locust 기반 API 부하 테스트
- 서비스 stop/start 장애 시뮬레이션
- SLO latency report 생성

## 요약

| 검증 항목 | 결과 | 비고 |
|---|---|---|
| Airflow image build | PASS | 최초 Docker engine 연결 오류는 재실행으로 해결 |
| 서비스 기동 | PASS | 핵심 container `Up` 상태 확인 |
| DB migration | PASS | migration SQL 적용 완료 |
| Smoke test | PASS | 핵심 API endpoint 4개 HTTP 200 |
| API load test | PASS | 396 requests, 실패 0건, 평균 응답 14 ms |
| Failure simulation | PASS | 대상 서비스 4개 stop/start 복구 확인 |
| SLO report | PASS | telemetry sample 131건 집계 |

## 실행 명령

```bash
docker compose -f docker/docker-compose.yml build airflow-webserver airflow-scheduler
docker compose -f docker/docker-compose.yml up -d
bash scripts/apply_migration.sh
python scripts/smoke_test.py --base-url http://localhost:8000
python -m locust -f scripts/load_test.py --host http://localhost:8000 --headless -u 15 -r 5 -t 20s --only-summary
python scripts/failure_simulation.py --mode stop-start --down-seconds 20
python scripts/generate_slo_report.py --hours 24
```

## Smoke Test

| Endpoint | Status |
|---|---:|
| `/health` | 200 |
| `/stats` | 200 |
| `/anomalies?limit=5` | 200 |
| `/metrics/summary?days=7` | 200 |

## API Load Test

도구: Locust  
대상: `http://localhost:8000`  
사용자 수: 15  
증가율: 5 users/sec  
실행 시간: 20초

| 지표 | 값 |
|---|---:|
| 총 요청 수 | 396 |
| 실패 수 | 0 |
| 실패율 | 0.00% |
| 평균 응답 시간 | 14 ms |
| p95 | 22 ms |
| p99 | 26 ms |

## Failure Simulation

명령: `python scripts/failure_simulation.py --mode stop-start --down-seconds 20`

대상 서비스:

- `kafka-2`
- `schema-registry`
- `postgres`
- `spark-streaming`

결과:

- 모든 대상 서비스의 stop/start가 완료되었습니다.
- 복구 후 smoke test가 통과했습니다.

## SLO Report

명령: `python scripts/generate_slo_report.py --hours 24`

| 지표 | 값 |
|---|---:|
| Samples | 131 |
| p50 produce to Kafka | 707.0 ms |
| p50 Kafka to Spark | 3323.0 ms |
| p50 Spark to DB | 4425.328 ms |
| p50 end to end | 8431.328 ms |
| p95 end to end | 20656.408 ms |
| p99 end to end | 20656.408 ms |

## 검증 중 발견한 이슈

| 이슈 | 조치 | 상태 |
|---|---|---|
| Airflow DAG import 시 `kafka` module 누락 | Airflow image에 `kafka-python==2.0.2` 추가 | 해결 |
| Spark streaming checkpoint/state schema 충돌 | 로컬 checkpoint 경로 정리 후 streaming 재시작 | 로컬 실행 기준 해결 |
| Streaming upsert column/value 개수 불일치 | insert statement와 `db_write_ts` 기본값 정리 | 해결 |

## 참고 사항

- 이 결과는 로컬 검증 결과이며 production benchmark가 아닙니다.
- SLO sample은 telemetry timestamp가 모두 기록된 anomaly event만 집계합니다.
- 운영 기준 SLO를 정하려면 더 긴 시간의 트래픽과 다양한 장애 조건에서 재측정해야 합니다.
