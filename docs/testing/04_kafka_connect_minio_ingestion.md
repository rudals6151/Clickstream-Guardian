# 04 Kafka Connect / MinIO 적재 테스트

## 목적

Kafka topic의 raw event가 Kafka Connect S3 Sink를 통해 MinIO에 Parquet 파일로 저장되는지 검증한다.

## 현재 상태

- Kafka Connect S3 Sink 설정 존재
- MinIO bucket `km-data-lake` 자동 생성
- 보완 필요: connector status, object count, parquet row count 검증 자동화

## 테스트 단계

| 단계 | 데이터 | 목적 |
|---|---|---|
| Connector status | - | connector RUNNING 확인 |
| Click ingestion | click 10,000건 | raw_clicks parquet 생성 확인 |
| Purchase ingestion | purchase 1,000건 | raw_purchases parquet 생성 확인 |
| Parquet read | 생성 파일 | Spark batch read 가능 여부 |
| DLQ route | invalid event | DLQ 적재 확인 |

## 실행 명령

```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/s3-sink-clicks/status
curl http://localhost:8083/connectors/s3-sink-purchases/status
python producers/producer_clicks.py --max-events 10000
python producers/producer_purchases.py --max-events 1000
```

## 측정 지표

- connector status
- MinIO object count
- Parquet file count
- Parquet row count
- DLQ offset 증가량
- small file 개수

## 성공 기준

- connector status가 RUNNING
- MinIO에 `raw_clicks/dt=.../hour=...` 경로 생성
- Spark가 Parquet를 읽을 수 있음
- invalid event가 DLQ로 이동

## 실패 시 원인 분석

- connector plugin이 로드되지 않았는가?
- Schema Registry 연결 실패인가?
- MinIO credential 문제인가?
- flush.size/rotate.interval 때문에 파일 생성이 지연되는가?

## 결과 기록

| 단계 | 결과 | Object count | Row count | DLQ 증가 | 판단 |
|---|---|---:|---:|---:|---|
| Connector status | | | | | |
| Click ingestion | | | | | |
| Purchase ingestion | | | | | |
| DLQ route | | | | | |

