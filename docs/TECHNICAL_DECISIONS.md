# 기술 선택과 운영 설정

이 문서는 Clickstream Guardian에서 Kafka와 Spark를 사용한 이유, 더 가벼운 대안과의 차이, Spark Streaming 운영 설정의 의도를 정리합니다.

프로젝트 규모만 보면 Kafka와 Spark는 필수 선택이 아닙니다. 단일 서버에서 파일을 읽고 Python/FastAPI/PostgreSQL만으로 처리해도 데모는 만들 수 있습니다. 이 프로젝트에서는 대규모 시스템을 그대로 운영하기보다, 데이터 엔지니어링에서 자주 등장하는 이벤트 기반 파이프라인의 구성 요소와 운영 이슈를 직접 다뤄보기 위해 Kafka와 Spark를 사용했습니다.

## 기술 선택 기준

| 질문 | 선택 |
|---|---|
| 이벤트를 어떻게 안정적으로 받을 것인가 | Kafka |
| 이벤트 schema를 어떻게 관리할 것인가 | Avro + Schema Registry |
| event-time window, watermark, deduplication을 어떻게 다룰 것인가 | Spark Structured Streaming |
| 원천 이벤트를 어디에 저장할 것인가 | Kafka Connect + MinIO |
| 일별 집계를 어떻게 스케줄링할 것인가 | Airflow + Spark Batch |
| 결과를 어떻게 조회할 것인가 | PostgreSQL + FastAPI + Streamlit |

## 왜 Kafka를 사용했는가

Kafka는 단순 message queue보다 이벤트 로그에 가까운 시스템입니다. 이 프로젝트에서는 아래 내용을 체험하고 검증하기 위해 Kafka를 사용했습니다.

- producer와 consumer를 분리해 각 처리 단계가 독립적으로 동작하도록 구성
- topic, partition, replication factor를 통한 이벤트 스트림 구조 이해
- consumer group, offset, 재처리 가능성에 대한 실습
- Schema Registry와 Avro를 붙여 producer-consumer 간 데이터 계약 관리
- broker 장애 상황에서 producer 전송이 어떻게 동작하는지 확인
- Kafka Connect를 통해 raw event를 data lake로 내보내는 패턴 실습

이 프로젝트 규모에서는 Redis Stream, RabbitMQ, PostgreSQL queue, 또는 단순 파일 기반 처리도 가능합니다. 다만 그런 방식은 Kafka의 offset 관리, partitioning, schema registry 연동, connect sink 같은 데이터 플랫폼 패턴을 보여주기 어렵습니다.

## Kafka 설정 의도

Producer 설정은 [producers/common/config.py](../producers/common/config.py)에 정의되어 있습니다.

| 설정 | 값 | 의도 |
|---|---:|---|
| `acks` | `all` | leader뿐 아니라 ISR replica 확인 후 ack를 받아 메시지 손실 위험을 낮춥니다. |
| `enable.idempotence` | `true` | retry 과정에서 producer 중복 전송 위험을 줄입니다. |
| `compression.type` | `lz4` | 처리량과 압축 효율 사이의 균형을 잡습니다. |
| `linger.ms` | `10` | 아주 짧게 기다려 batch 전송 효율을 높입니다. |
| `batch.size` | `32768` | 작은 이벤트를 묶어 네트워크 비용을 줄입니다. |
| `max.in.flight.requests.per.connection` | `5` | idempotent producer에서 허용되는 범위 안에서 처리량을 확보합니다. |
| `retries` | `3` | 일시적인 broker/network 오류를 재시도합니다. |

운영 환경이라면 workload에 맞춰 `delivery.timeout.ms`, `request.timeout.ms`, `retry.backoff.ms`, topic별 retention, partition 수, min ISR 값을 별도로 조정해야 합니다.

## 왜 Spark를 사용했는가

Spark Structured Streaming은 단순히 빠른 처리를 위해서만 사용한 것이 아닙니다. 이 프로젝트에서는 streaming data engineering에서 중요한 개념을 직접 구현하기 위해 사용했습니다.

- event-time 기반 window 집계
- watermark를 통한 late event 처리
- event id 기반 중복 제거
- micro-batch 처리와 checkpoint 복구
- Kafka source와 PostgreSQL sink 연결
- batch job과 streaming job을 같은 Spark 생태계 안에서 비교

현재 데이터 규모만 보면 Spark는 과한 선택입니다. Python consumer, Faust, Flink mini cluster, Kafka Streams, PostgreSQL 집계, 또는 단순 pandas batch로도 기능 구현은 가능합니다. 하지만 Spark를 사용하면 대용량 처리 시스템의 window/state/checkpoint 개념을 직접 다룰 수 있고, batch와 streaming을 같은 프레임워크로 설명할 수 있습니다.

## Spark Streaming 설정 의도

주요 설정은 [spark-streaming/anomaly_detector.py](../spark-streaming/anomaly_detector.py)에 있습니다.

| 설정 | 현재 값 | 의도 | 운영 시 보완 |
|---|---|---|---|
| `startingOffsets` | `latest` | 데모 실행 시 과거 topic 데이터를 모두 재처리하지 않고 현재 유입 데이터부터 처리합니다. | 재처리 목적이면 `earliest` 또는 명시 offset을 사용하고, 실행 목적별로 설정을 분리합니다. |
| `checkpointLocation` | `/tmp/spark-checkpoint/anomaly` | local Docker 환경에서 빠르게 checkpoint를 만들고 재시작 테스트를 하기 위한 경로입니다. | 운영에서는 container 내부 `/tmp`가 아니라 durable storage에 저장해야 합니다. |
| `spark.sql.shuffle.partitions` | `4` | 로컬 환경에서 불필요한 partition overhead를 줄입니다. | 실제 cluster 규모와 처리량에 맞춰 조정합니다. |
| `spark.streaming.kafka.consumer.cache.capacity` | `1000` | Kafka consumer cache를 활용해 반복 생성 비용을 줄입니다. | executor 수와 topic/partition 수에 맞춰 조정합니다. |
| `spark.sql.streaming.stateStore.stateSchemaCheck` | `false` | 로컬 실험 중 state schema 변경으로 checkpoint 재사용이 막히는 문제를 완화했습니다. | 운영에서는 기본값을 유지하고 schema 변경 시 checkpoint migration 또는 신규 checkpoint를 사용합니다. |
| `spark.sql.streaming.statefulOperator.checkCorrectness.enabled` | `false` | 여러 stateful 연산 실험 중 Spark correctness warning/error를 완화하기 위한 데모 설정입니다. | 운영에서는 끄지 않는 것이 원칙입니다. query 구조를 단순화하거나 stateful query를 분리합니다. |
| `spark.sql.streaming.statefulOperator.allowMultiple` | `false` | 하나의 query 안에서 여러 stateful operator가 섞이는 위험을 제한합니다. | stateful query를 명확히 분리하고 checkpoint를 독립적으로 둡니다. |

## `startingOffsets=latest`를 둔 이유

이 프로젝트의 streaming job은 실시간 이상 탐지 데모에 초점이 있습니다. `earliest`를 사용하면 Kafka topic에 남아 있는 과거 이벤트를 모두 읽어 window와 watermark 기준이 복잡해지고, 데모 실행 때마다 오래된 데이터가 다시 처리될 수 있습니다.

따라서 기본값은 `latest`로 두었습니다. 이 선택은 "신규 유입 이벤트 감시"에는 적합하지만, "장애 후 전체 재처리"나 "과거 데이터 backfill"에는 맞지 않습니다.

운영적으로는 다음처럼 분리하는 것이 좋습니다.

- 실시간 감시 job: `latest` 또는 마지막 checkpoint 기반 재시작
- backfill job: `earliest` 또는 특정 timestamp/offset 기반 실행
- 장애 복구: checkpoint 보존 후 재시작
- 검증/재현 테스트: 별도 topic 또는 별도 consumer group 사용

## Checkpoint 위치

현재 기본 checkpoint 경로는 `/tmp/spark-checkpoint/anomaly`입니다. 이 경로는 Docker 로컬 데모에서는 간단하지만, container 재생성이나 수동 정리 시 state가 사라질 수 있습니다.

운영에 가까운 구성이라면 다음이 필요합니다.

- checkpoint를 volume, HDFS, S3 같은 durable storage에 저장
- query별 checkpoint 경로 분리
- schema 변경 시 checkpoint 재사용 여부를 명확히 판단
- checkpoint 삭제를 장애 해결 수단으로 남용하지 않기

현재 코드는 late event, PostgreSQL write query, console query가 각각 checkpoint를 사용합니다. 특히 실험 중 query 구조가 자주 바뀌면 checkpoint state와 코드의 state schema가 충돌할 수 있습니다.

## Debug console query

현재 streaming job에는 raw event와 anomaly 결과를 console에 출력하는 debug query가 있습니다.

이 설정은 다음 목적을 위해 남겼습니다.

- Kafka에서 Spark로 이벤트가 실제 유입되는지 빠르게 확인
- Avro parsing 결과와 timestamp 변환 확인
- anomaly window 결과를 DB 적재 전 눈으로 검증

하지만 운영 환경에서는 console sink가 불필요한 로그를 만들고 성능에도 영향을 줄 수 있습니다. 따라서 운영 기준으로는 `--no-console`을 기본값으로 두거나, 환경변수로 debug query를 명시적으로 켜는 방식이 더 적절합니다.

## State schema 설정

`stateStore.stateSchemaCheck=false`는 운영적으로 조심해야 하는 설정입니다. Spark Structured Streaming은 checkpoint에 state schema를 저장합니다. window 집계 컬럼이나 stateful 연산 구조가 바뀌면 기존 checkpoint와 새 query가 맞지 않아 실패할 수 있습니다.

이 프로젝트에서는 로컬 실험 중 query 구조를 여러 번 바꾸면서 checkpoint 충돌이 발생했고, 데모 재실행 편의를 위해 해당 설정을 완화했습니다.

운영 환경에서는 다음 방식이 더 적절합니다.

- state schema check를 끄지 않는다.
- stateful query 구조 변경 시 새 checkpoint 경로를 사용한다.
- 변경 전후 replay/backfill 계획을 세운다.
- stateful operator를 한 query에 과도하게 섞지 않는다.

## 이 프로젝트에서 가벼운 대안을 쓰지 않은 이유

솔직한 이유는 학습과 체험입니다. 이 규모의 데이터라면 Kafka와 Spark가 필수는 아닙니다. 다만 포트폴리오 관점에서는 단순 구현보다 아래 내용을 직접 다룬 경험이 중요하다고 판단했습니다.

- Kafka topic/partition/offset/replication 이해
- Avro schema와 Schema Registry 연동
- Spark event-time window와 watermark 처리
- checkpoint와 state schema 충돌 경험
- Airflow orchestration과 batch idempotency 설계
- 장애 시뮬레이션과 SLO 측정

면접에서는 "이 프로젝트 규모에 Kafka/Spark가 꼭 필요해서 썼다"라고 말하기보다, "작은 도메인에 대규모 데이터 플랫폼 패턴을 축소 적용해보고, 각 기술의 운영 trade-off를 직접 확인했다"라고 설명하는 것이 더 정확합니다.

## 실제 운영이라면 다르게 할 점

- Kafka/Spark가 필요한 처리량과 요구사항을 먼저 산정합니다.
- 단순한 요구라면 PostgreSQL, Redis Stream, RabbitMQ, Python worker 같은 가벼운 대안을 먼저 검토합니다.
- Spark checkpoint는 durable storage에 저장하고, state schema 변경 정책을 문서화합니다.
- debug console query는 기본 비활성화합니다.
- streaming/backfill/replay job을 분리합니다.
- CI에서 DAG import, producer unit test, API test, schema compatibility test를 자동화합니다.
- Prometheus/Grafana 또는 OpenTelemetry로 pipeline lag, consumer lag, processing latency를 관측합니다.
