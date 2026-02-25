# Metrics Summary (Single Source)
## Clickstream Guardian - 2026-02-22 Baseline

이 문서는 포트폴리오 제출용으로 핵심 수치를 한 곳에 모은 단일 요약본입니다.

## 1) 한눈에 보는 결과

| Category | Metric | Value | Status |
|---|---|---:|---|
| Smoke Test | Core endpoints success | 4/4 (HTTP 200) | PASS |
| API Load Test | Total requests | 396 | PASS |
| API Load Test | Failure rate | 0.00% | PASS |
| API Load Test | Avg response time | 14 ms | PASS |
| API Load Test | p95 / p99 | 22 ms / 26 ms | PASS |
| Failure Simulation | Target services recovered | 4/4 | PASS |
| Kafka Broker-down Throughput | Sent / Failed (kafka-2 down) | 5000 / 0 | PASS |
| Kafka Broker-down Throughput | Avg rate (kafka-2 down) | ~4,690 msg/sec | PASS |
| Kafka Normal Throughput | Avg rate (normal) | ~4,819 msg/sec | PASS |
| SLO Report | Samples | 4 | INFO |
| SLO Report | p50 produce->kafka | 828.0 ms | INFO |
| SLO Report | p50 kafka->spark | 2648.0 ms | INFO |
| SLO Report | p50 spark->db | 6586.2185 ms | INFO |
| SLO Report | p50 e2e | 10094.2185 ms | INFO |
| SLO Report | p95 e2e | 13169.472 ms | INFO |
| SLO Report | p99 e2e | 13409.472 ms | INFO |

## 2) 테스트별 상세

### 2.1 Smoke Test
- Command:
```bash
python scripts/smoke_test.py --base-url http://localhost:8000
```
- Verified endpoints:
1. `/health` -> 200
2. `/stats` -> 200
3. `/anomalies?limit=5` -> 200
4. `/metrics/summary?days=7` -> 200

### 2.2 API Load Test (Locust)
- Command:
```bash
python -m locust -f scripts/load_test.py --host http://localhost:8000 --headless -u 15 -r 5 -t 20s --only-summary
```
- Result:
1. Total requests: 396
2. Failures: 0 (0.00%)
3. Avg response: 14 ms
4. p95: 22 ms
5. p99: 26 ms

### 2.3 Failure Simulation (Stop/Start)
- Command:
```bash
python scripts/failure_simulation.py --mode stop-start --down-seconds 20
```
- Services:
1. `kafka-2`
2. `schema-registry`
3. `postgres`
4. `spark-streaming`
- Result:
1. Stop/start completed for all targets
2. Post-recovery smoke test passed

### 2.4 Kafka Broker-down Throughput Check
- Scenario A (normal brokers):
1. Sent: 5000
2. Failed: 0
3. Avg: ~4,819 msg/sec

- Scenario B (`kafka-2` down):
1. Sent: 5000
2. Failed: 0
3. Avg: ~4,690 msg/sec

- Interpretation:
1. Single broker failure tolerance confirmed for this workload
2. Throughput drop is minor under this test condition

### 2.5 SLO Report
- Command:
```bash
python scripts/generate_slo_report.py --hours 24
```
- Report fields:
1. `samples`: 4
2. `p50_produce_to_kafka_ms`: 828.0
3. `p50_kafka_to_spark_ms`: 2648.0
4. `p50_spark_to_db_ms`: 6586.2185
5. `p50_e2e_ms`: 10094.2185
6. `p95_e2e_ms`: 13169.472
7. `p99_e2e_ms`: 13409.472

## 3) 수치 해석 메모
1. 현재 SLO 샘플 수(`samples=4`)가 작아서 통계 신뢰도가 낮습니다.
2. 운영 수준 판단을 위해 샘플을 최소 수십~수백 건 이상 확보하는 것이 좋습니다.
3. 장애 테스트는 "복구 가능성" 확인 중심이며, MTTR 자동 집계는 후속 과제입니다.

## 4) 원본 근거 문서
1. `docs/TEST_EXECUTION_REPORT_2026-02-22.md`
2. `docs/PORTFOLIO_PROJECT_SUMMARY.md`
3. `README.md`

## 5) 업데이트 규칙
- 새 테스트 실행 후 이 문서의 표부터 갱신합니다.
- 날짜가 바뀌면 제목의 기준일(`2026-02-22`)도 함께 변경합니다.
