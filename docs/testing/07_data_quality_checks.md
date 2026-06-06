# 07 데이터 품질 테스트

## 목적

파이프라인이 데이터를 처리한 뒤 결과 테이블이 기본 품질 기준을 만족하는지 검증한다.

## 현재 상태

- DB table: `data_quality_checks`
- 보완 필요: 실제 품질 체크 스크립트와 Airflow task 추가

## 품질 체크 항목

| 체크 | 대상 | 기준 |
|---|---|---|
| row count | 주요 결과 테이블 | 0보다 커야 함 |
| null event_id | `anomaly_sessions` | 0건 |
| duplicate event_id | `anomaly_sessions` | 0건 |
| invalid anomaly_score | `anomaly_sessions` | 0 이상 |
| conversion_rate range | `daily_metrics` | 0~1 |
| negative revenue | `daily_metrics`, `popular_items` | 0 이상 |
| late event ratio | late topic / raw topic | 기준치 이하 |
| batch idempotency | metric_date 기준 | 재실행 후 중복 없음 |

## 실행 명령

```bash
docker exec postgres psql -U admin -d clickstream -c "select count(*) from anomaly_sessions;"
docker exec postgres psql -U admin -d clickstream -c "select event_id, count(*) from anomaly_sessions group by event_id having count(*) > 1 limit 10;"
docker exec postgres psql -U admin -d clickstream -c "select count(*) from daily_metrics where conversion_rate < 0 or conversion_rate > 1;"
```

## 측정 지표

- table별 row count
- null count
- duplicate count
- invalid range count
- data_quality_checks 기록 수

## 성공 기준

- duplicate event_id 0건
- metric 범위 위반 0건
- 주요 결과 table row count 정상
- 실패한 check는 `data_quality_checks`에 남김

## 실패 시 원인 분석

- producer event_id 생성 문제인가?
- Spark deduplication이 동작하지 않았는가?
- batch 재실행 멱등성이 깨졌는가?
- schema mismatch나 null field가 유입되었는가?

## 결과 기록

| 체크 | 대상 | 결과 | 실제 값 | 판단 |
|---|---|---|---:|---|
| row count | | | | |
| null event_id | | | | |
| duplicate event_id | | | | |
| invalid range | | | | |

