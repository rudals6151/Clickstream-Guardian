# 00. Environment Smoke Test

## 목적

부하/장애 테스트 전에 로컬 Docker 환경이 동일한 기준 설정으로 실행 가능한지 확인했다.
검증 대상은 Kafka -> Spark Structured Streaming -> PostgreSQL 이상 탐지 핵심 경로다.
Kafka Connect와 MinIO 적재는 이번 성능/장애 테스트 범위에서 제외했다.

## 최종 상태

| 항목 | 결과 |
| --- | --- |
| 실행 일시 | 2026-06-07 |
| 판정 | PASS |
| 테스트 가능 여부 | 용량 테스트와 장애 테스트 진행 가능 |
| 측정 경로 | Producer -> Kafka -> Spark Streaming/state -> PostgreSQL |

## 로컬 실행 환경

| 구성 요소 | 설정 |
| --- | --- |
| Kafka | 3 brokers |
| Kafka topic | `km.clicks.raw.v1`, 3 partitions, replication factor 2 |
| Spark Worker | 1 worker, 2 cores, 2 GiB executor memory |
| Spark Streaming driver | 2 GiB driver memory, container limit 4 GiB |
| Spark shuffle partitions | 2 |
| Spark state store | RocksDB, bounded memory 512 MiB |
| Max offsets per trigger | query당 250,000 events |
| PostgreSQL | `clickstream` DB, anomaly result upsert |
| Checkpoint | `/opt/spark-checkpoint/pipeline-capacity-v4` |
| Progress output | `docs/testing/results/pipeline_capacity_progress.jsonl` |

## 확인 항목

| 확인 항목 | 결과 |
| --- | --- |
| Kafka broker 3대 실행 | PASS |
| raw topic partition/ISR 정상 | PASS |
| Schema Registry 응답 | PASS |
| PostgreSQL 연결 | PASS |
| Spark master/worker 실행 | PASS |
| Spark executor 할당 | PASS |
| Streaming query 시작 | PASS |
| 시작 시 Kafka lag | 0 |
| Spark Streaming/Worker restart count | 0 |

## 사전 조정

Spark Worker가 executor 2 GiB를 실행할 수 있도록 worker container limit을 3 GiB, CPU를 2 cores로 맞췄다.
Spark executor 2 cores에 맞춰 shuffle partitions를 2로 조정했다.

Kafka Connect는 기존 S3 sink backlog가 핵심 경로 측정을 방해할 수 있어 이번 테스트 범위에서 제외했다.
따라서 이 문서 이후의 결과는 전체 데이터 레이크 적재 경로가 아니라 다음 핵심 경로에 대한 결과다.

```text
Producer -> Kafka -> Spark Structured Streaming -> PostgreSQL
```
