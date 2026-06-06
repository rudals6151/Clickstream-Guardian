# 06 장애 복구 매트릭스 테스트

## 목적

핵심 서비스 장애가 발생했을 때 파이프라인이 어떤 영향을 받는지, 복구 후 데이터 유실/중복/지연이 발생하는지 확인한다.

## 현재 상태

- 기존 스크립트: `scripts/failure_simulation.py`
- 기존 기록: `kafka-2`, `schema-registry`, `postgres`, `spark-streaming` stop/start 후 smoke test 통과
- 보완 필요: 장애 전/중/후 지표 기록, 복구 시간, DLQ/중복/lag 확인

## 테스트 시나리오

| 장애 대상 | Down 시간 | 예상 영향 |
|---|---:|---|
| `kafka-2` | 60초 | producer 처리량 감소 가능 |
| `schema-registry` | 60초 | 신규 schema 조회/등록 실패 가능 |
| `spark-streaming` | 60초 | anomaly 적재 중단, 복구 후 재개 |
| `postgres` | 60초 | API health 실패, DB write 실패 |
| `kafka-connect` | 60초 | MinIO 적재 지연 |
| `minio` | 60초 | Connect S3 sink 실패/DLQ 가능 |
| `airflow-scheduler` | 60초 | DAG scheduling 중단 |

## 실행 명령

```bash
python scripts/failure_simulation.py --mode stop-start --services kafka-2 --down-seconds 60
python scripts/failure_simulation.py --mode stop-start --services schema-registry --down-seconds 60
python scripts/failure_simulation.py --mode stop-start --services spark-streaming --down-seconds 60
python scripts/failure_simulation.py --mode stop-start --services postgres --down-seconds 60
python scripts/failure_simulation.py --mode stop-start --services kafka-connect --down-seconds 60
python scripts/failure_simulation.py --mode stop-start --services minio --down-seconds 60
python scripts/failure_simulation.py --mode stop-start --services airflow-scheduler --down-seconds 60
```

## 측정 지표

- 장애 시작/종료/복구 완료 시각
- smoke test 상태
- producer 실패 수
- topic offset 증가량
- DLQ offset 증가량
- anomaly row count 변화
- duplicate event_id count
- API health 상태
- recovery time

## 성공 기준

- 장애 복구 후 smoke test 통과
- producer/streaming이 재개됨
- 중복 적재 없음
- 장애 영향과 한계를 설명할 수 있음

## 실패 시 원인 분석

- 재시작 순서 문제인가?
- healthcheck는 통과했지만 application readiness가 부족한가?
- checkpoint 유실인가?
- DB connection retry가 부족한가?
- DLQ로 이동했는가?

## 결과 기록

| 대상 | Down | 복구 시간 | Producer 실패 | DLQ 증가 | Duplicate | Smoke | 판단 |
|---|---:|---:|---:|---:|---:|---|---|
| kafka-2 | | | | | | | |
| schema-registry | | | | | | | |
| spark-streaming | | | | | | | |
| postgres | | | | | | | |
| kafka-connect | | | | | | | |
| minio | | | | | | | |
| airflow-scheduler | | | | | | | |

