# 05 Airflow Batch E2E 테스트

## 목적

Airflow가 Spark batch job을 실행하고 PostgreSQL 분석 테이블을 정확히 갱신하는지 검증한다. 동일 날짜 재실행 시 중복 없이 멱등적으로 동작하는지도 확인한다.

## 현재 상태

- DAG: `spark_batch_processing`
- Batch job: `daily_metrics.py`, `popular_items.py`, `session_funnel.py`
- 실행 이력 table: `pipeline_runs`
- 보완 필요: DAG 수동 실행과 결과 검증 자동화

## 테스트 단계

| 단계 | 목적 |
|---|---|
| Raw data 준비 | MinIO에 대상 날짜 Parquet 존재 확인 |
| DAG trigger | `spark_batch_processing` 수동 실행 |
| Result check | `daily_metrics`, `popular_items`, `session_funnel` row 확인 |
| Pipeline history | `pipeline_runs` SUCCESS 확인 |
| Idempotency | 같은 날짜 재실행 후 row count 동일 확인 |

## 실행 명령

```bash
docker exec airflow-scheduler airflow dags trigger spark_batch_processing
docker exec postgres psql -U admin -d clickstream -c "select * from pipeline_runs order by id desc limit 5;"
docker exec postgres psql -U admin -d clickstream -c "select count(*) from daily_metrics;"
docker exec postgres psql -U admin -d clickstream -c "select count(*) from popular_items;"
docker exec postgres psql -U admin -d clickstream -c "select count(*) from session_funnel;"
```

## 측정 지표

- DAG run status
- task별 실행 시간
- 결과 table row count
- 재실행 전후 row count 차이
- `pipeline_runs` status
- Spark job 실패 로그

## 성공 기준

- DAG run SUCCESS
- 결과 table row count 정상
- 같은 날짜 재실행 후 중복 없음
- 실패 시 pipeline_runs에 실패 이력 기록

## 실패 시 원인 분석

- MinIO raw path가 없는가?
- Spark package 다운로드/로드 문제인가?
- PostgreSQL JDBC 연결 실패인가?
- atomic_write 중 staging/final table schema가 불일치하는가?

## 결과 기록

| 단계 | DAG 상태 | daily_metrics | popular_items | session_funnel | pipeline_runs | 판단 |
|---|---|---:|---:|---:|---|---|
| 1차 실행 | | | | | | |
| 재실행 | | | | | | |

