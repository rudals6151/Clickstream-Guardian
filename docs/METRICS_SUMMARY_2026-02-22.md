# 지표 요약

작성일: 2026-02-22 KST  
목적: 포트폴리오 검토용 핵심 검증 수치 요약

## 기준 검증 결과

| 분류 | 지표 | 값 | 상태 |
|---|---|---:|---|
| Smoke test | HTTP 200을 반환한 핵심 endpoint | 4 / 4 | PASS |
| API load test | 총 요청 수 | 396 | PASS |
| API load test | 실패율 | 0.00% | PASS |
| API load test | 평균 응답 시간 | 14 ms | PASS |
| API load test | p95 / p99 | 22 ms / 26 ms | PASS |
| Failure simulation | 복구 확인 대상 서비스 | 4 / 4 | PASS |
| Kafka broker-down test | `kafka-2` 중단 중 전송 / 실패 | 5000 / 0 | PASS |
| Kafka broker-down test | 평균 전송률 | 약 4,690 msg/sec | PASS |
| Kafka normal test | 평균 전송률 | 약 4,819 msg/sec | PASS |
| SLO report | Telemetry sample | 131 | INFO |
| SLO report | p50 produce to Kafka | 707.0 ms | INFO |
| SLO report | p50 Kafka to Spark | 3323.0 ms | INFO |
| SLO report | p50 Spark to DB | 4425.328 ms | INFO |
| SLO report | p50 end to end | 8431.328 ms | INFO |
| SLO report | p95 end to end | 20656.408 ms | INFO |
| SLO report | p99 end to end | 20656.408 ms | INFO |

## 지표 해석

- API는 smoke test와 짧은 부하 테스트에서 요청 실패 없이 응답했습니다.
- 로컬 테스트 기준으로 단일 Kafka broker 중단 상황에서도 producer 전송이 지속되었습니다.
- 장애 시뮬레이션 후 대상 서비스가 재기동되었고, 복구 이후 smoke test가 통과했습니다.
- SLO report는 telemetry timestamp가 모두 기록된 anomaly event를 기준으로 단계별 latency를 계산했습니다.

## 실행 명령

```bash
python scripts/smoke_test.py --base-url http://localhost:8000
python -m locust -f scripts/load_test.py --host http://localhost:8000 --headless -u 15 -r 5 -t 20s --only-summary
python scripts/failure_simulation.py --mode stop-start --down-seconds 20
python scripts/producer_throughput_test.py
python scripts/generate_slo_report.py --hours 24
```

## 한계

- 모든 수치는 로컬 Docker 환경에서 측정한 결과이며 production capacity 수치가 아닙니다.
- SLO sample 131건은 smoke-level 검증에는 의미가 있지만 안정적인 운영 SLO를 정하기에는 부족합니다.
- p95, p99는 더 긴 트래픽, 더 많은 이벤트, 통제된 장애 조건에서 재측정해야 합니다.

## 관련 문서

- [테스트 실행 리포트](TEST_EXECUTION_REPORT_2026-02-22.md)
