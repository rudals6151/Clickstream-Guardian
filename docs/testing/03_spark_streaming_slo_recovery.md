# 03 Spark Streaming SLO 및 복구 테스트

## 목적

Kafka에 들어간 이벤트가 Spark Streaming을 거쳐 PostgreSQL에 적재되기까지의 end-to-end 지연을 측정하고, streaming job 재시작 후 checkpoint 기반 복구와 중복 방지 여부를 확인한다.

## 현재 상태

- 기존 스크립트: `scripts/generate_slo_report.py`
- 기존 기록: sample 131건, p95 e2e 약 20.6초
- 보완 필요: sample 수 확대, restart 전후 중복 확인, checkpoint 안정성 확인

## 테스트 단계

| 단계 | 이벤트 수 | 조건 | 목적 |
|---|---:|---|---|
| Baseline | 10,000 | 정상 | 기본 SLO |
| Spike | 50,000 | anomaly spike 활성화 | anomaly 탐지량 확보 |
| Large | 100,000 | 정상 | 지연 증가 확인 |
| Restart | 50,000 | streaming stop/start | checkpoint 복구 확인 |
| Duplicate check | - | 재시작 후 | 중복 적재 확인 |

## 실행 명령

```bash
python producers/producer_clicks.py --anomaly-interval 20 --max-events 50000
python scripts/generate_slo_report.py --hours 24
python scripts/failure_simulation.py --mode stop-start --services spark-streaming --down-seconds 60
python producers/producer_clicks.py --anomaly-interval 20 --max-events 50000
python scripts/generate_slo_report.py --hours 24
```

## 측정 지표

- anomaly row count
- duplicate event_id count
- late event count
- p50/p95/p99 produce_to_kafka
- p50/p95/p99 kafka_to_spark
- p50/p95/p99 spark_to_db
- p50/p95/p99 e2e
- restart 후 적재 재개 여부

## 성공 기준

- streaming 재시작 후 anomaly 적재 재개
- duplicate event_id 0건
- p95 e2e가 기록되고 병목 구간을 설명할 수 있음

## 실패 시 원인 분석

- checkpoint 경로가 유실되었는가?
- state schema 충돌이 발생했는가?
- DB upsert가 병목인가?
- console debug query가 성능에 영향을 주는가?

## 결과 기록

| 단계 | Events | Samples | p50 e2e | p95 e2e | p99 e2e | Duplicate | 판단 |
|---|---:|---:|---:|---:|---:|---:|---|
| Baseline | | | | | | | |
| Spike | | | | | | | |
| Large | | | | | | | |
| Restart | | | | | | | |

