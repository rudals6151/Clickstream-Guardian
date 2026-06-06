# 01. API 동시 사용자 부하 테스트

## 목적

FastAPI 기반 조회 API가 동시 사용자 증가에 따라 어느 수준까지 안정적으로 응답하는지 확인한다. 단순 평균 응답시간만 보지 않고 실패율, p95/p99, endpoint별 병목, 실패 원인, 최적화 전후 차이를 함께 기록한다.

## 테스트 대상

- API 서버: `http://localhost:8000`
- 주요 endpoint: `/health`, `/anomalies`, `/anomalies/types`, `/anomalies/timeline`, `/metrics/daily`, `/metrics/summary`, `/items/trending`
- 부하 도구: `Locust`
- 시나리오 파일: `scripts/load_test.py`
- 결과 저장 위치: `docs/testing/results/`

## 테스트 전 환경 점검

| 항목 | 결과 |
|---|---|
| Docker Compose 설정 검증 | 통과 |
| API smoke test | 통과 |
| PostgreSQL healthcheck | healthy |
| Kafka broker/topic 초기화 | 정상 |
| Kafka Connect connector 등록 | `s3-sink-clicks`, `s3-sink-purchases` 등록 |
| Spark Streaming | micro-batch 실행, checkpoint 오류 해결 |

## 실행 명령

```bash
python -m locust -f scripts/load_test.py --headless -H http://localhost:8000 -u 25 -r 5 -t 60s --csv docs/testing/results/01_api_baseline_25u --html docs/testing/results/01_api_baseline_25u.html --only-summary
python -m locust -f scripts/load_test.py --headless -H http://localhost:8000 -u 100 -r 10 -t 90s --csv docs/testing/results/01_api_load_100u --html docs/testing/results/01_api_load_100u.html --only-summary
python -m locust -f scripts/load_test.py --headless -H http://localhost:8000 -u 100 -r 10 -t 90s --csv docs/testing/results/01_api_load_100u_after_pool --html docs/testing/results/01_api_load_100u_after_pool.html --only-summary
python -m locust -f scripts/load_test.py --headless -H http://localhost:8000 -u 200 -r 20 -t 90s --csv docs/testing/results/01_api_stress_200u_after_pool --html docs/testing/results/01_api_stress_200u_after_pool.html --only-summary
python -m locust -f scripts/load_test.py --headless -H http://localhost:8000 -u 200 -r 20 -t 90s --csv docs/testing/results/01_api_stress_200u_after_workers --html docs/testing/results/01_api_stress_200u_after_workers.html --only-summary
```

## 결과 요약

| 단계 | 동시 사용자 | 처리량 | 실패율 | 평균 | p95 | p99 | 판정 |
|---|---:|---:|---:|---:|---:|---:|---|
| 기준선 | 25 | 34.13 req/s | 0.00% | 5 ms | 12 ms | 24 ms | 통과 |
| 최적화 전 | 100 | 131.91 req/s | 0.45% | 15 ms | 51 ms | 99 ms | 실패 |
| pool 최적화 후 | 100 | 134.38 req/s | 0.00% | 6 ms | 14 ms | 28 ms | 통과 |
| worker 최적화 전 | 200 | 29.35 req/s | 20.33% | 4,854 ms | 23,000 ms | 45,000 ms | 실패 |
| worker 최적화 후 | 200 | 237.66 req/s | 0.00% | 96 ms | 420 ms | 580 ms | 통과 |

## 발견한 병목과 조치

첫 100명 테스트에서 일부 endpoint가 500, `/health`가 503을 반환했다. API 로그를 확인한 결과 `psycopg2.pool.PoolError: connection pool exhausted`가 원인이었다. 기존 DB connection pool은 `maxconn=10`으로 고정되어 있어 동시 요청이 몰릴 때 connection을 즉시 확보하지 못했다.

1차 조치로 DB pool 설정을 환경변수화하고 기본값을 `DB_POOL_MIN=5`, `DB_POOL_MAX=30`, `DB_POOL_ACQUIRE_TIMEOUT=5`로 조정했다. 또한 pool이 가득 찼을 때 즉시 실패하지 않고 semaphore로 짧게 대기하도록 변경했다. 이 조치 후 100명 테스트는 실패율 0%로 통과했다.

200명 테스트에서는 여전히 pool acquire timeout이 발생했다. Postgres `max_connections=100`을 확인한 뒤 API worker를 2개로 늘리고 worker당 pool 최대값을 40으로 조정했다. 총 최대 DB 연결 수를 약 80개로 제한해 Postgres 상한을 넘지 않으면서 API 처리 병렬성을 확보했다. 재테스트 결과 200명에서도 실패율 0%를 달성했다.

## 최종 기준

현재 Docker 로컬 환경 기준으로 API는 `동시 사용자 200명, 약 237 req/s, 실패율 0%`까지 확인했다. 단, 이는 조회 API 중심의 포트폴리오 테스트 결과이며 실제 운영 한계로 일반화하려면 더 긴 soak test, 실데이터 규모 확대, CPU 제한 조건 고정, DB slow query 분석이 추가로 필요하다.

## 테스트 후 리소스 스냅샷

| 컨테이너 | CPU | 메모리 |
|---|---:|---:|
| api | 0.36% | 143.5 MiB / 512 MiB |
| postgres | 0.01% | 43.59 MiB |
| spark-streaming-anomaly | 0.70% | 740.8 MiB / 4 GiB |
| kafka-1 | 1.85% | 410 MiB |

## 결과 파일

| 파일 | 설명 |
|---|---|
| `docs/testing/results/01_api_baseline_25u.html` | 25명 기준선 HTML 리포트 |
| `docs/testing/results/01_api_load_100u.html` | 100명 최적화 전 HTML 리포트 |
| `docs/testing/results/01_api_load_100u_after_pool.html` | pool 최적화 후 100명 HTML 리포트 |
| `docs/testing/results/01_api_stress_200u_after_pool.html` | worker 최적화 전 200명 HTML 리포트 |
| `docs/testing/results/01_api_stress_200u_after_workers.html` | worker 최적화 후 200명 HTML 리포트 |
