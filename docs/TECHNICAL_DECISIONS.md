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

Kafka broker와 topic 설정은 [docker/docker-compose.yml](../docker/docker-compose.yml)에 정의되어 있습니다.

| 설정 | 현재 값 | 의도 | 운영 시 보완 |
|---|---:|---|---|
| broker 수 | `3` | 단일 broker가 아닌 cluster 구조, replication, 장애 테스트를 체험하기 위해 구성했습니다. | 운영에서는 처리량, 장애 도메인, 비용을 기준으로 broker 수를 산정합니다. |
| replication factor | `2` | 로컬 자원 사용을 줄이면서 broker 1대 장애를 일부 견딜 수 있게 했습니다. | 운영에서는 보통 `3` 이상을 검토합니다. |
| partition 수 | click/purchase `3` | 병렬 소비와 partition 개념을 실습하기 위한 최소 구성입니다. | key 분포, 처리량, consumer 수를 기준으로 산정합니다. |
| DLQ/late topic partition | `1` | 장애/지연 이벤트는 순서와 단순 모니터링을 우선해 작게 구성했습니다. | 이벤트량이 많으면 partition 확장이 필요합니다. |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `false` | topic을 실수로 자동 생성하지 않고, 명시적으로 관리하기 위해 껐습니다. | 운영에서도 일반적으로 topic 생성은 IaC/운영 절차로 관리합니다. |
| internal listener | `kafka-1:29092` 등 | container 내부 서비스 간 통신용입니다. | Kubernetes/Cloud 환경에서는 service discovery와 listener 전략을 별도 설계합니다. |
| host listener | `localhost:9092` 등 | 로컬 producer와 CLI 테스트를 쉽게 하기 위한 포트입니다. | 운영에서는 외부 listener, TLS, ACL, 인증을 적용합니다. |

Producer 설정:

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

## 왜 Schema Registry와 Avro를 사용했는가

JSON으로 이벤트를 보내면 구현은 더 쉽습니다. 하지만 producer와 consumer가 많아지는 순간 field 이름, 타입, nullable 여부, timestamp 단위가 조금만 달라도 downstream 장애로 이어질 수 있습니다.

이 프로젝트에서는 Avro와 Schema Registry를 사용해 다음을 경험했습니다.

- 이벤트 schema를 코드 밖에서 명시적으로 관리
- producer와 consumer 간 데이터 계약을 강제
- schema id가 붙은 Confluent wire format 처리
- Spark에서 Avro payload를 schema 기반으로 decoding
- schema 변경이 consumer에 미치는 영향 이해

현재 schema compatibility 정책을 깊게 자동화하지는 않았습니다. 운영에 가깝게 만들려면 compatibility mode 설정, schema 변경 테스트, CI에서 schema compatibility check를 추가해야 합니다.

## 왜 Kafka Connect를 사용했는가

Kafka Connect는 Kafka topic의 데이터를 외부 저장소로 내보내는 표준화된 방법을 제공합니다. 직접 Python consumer를 만들어 MinIO에 쓰는 것도 가능하지만, Connect를 사용하면 source/sink connector, offset 관리, DLQ, format, partitioner 같은 운영 패턴을 다룰 수 있습니다.

S3 Sink 설정은 [connectors/s3-sink-clicks.json](../connectors/s3-sink-clicks.json), [connectors/s3-sink-purchases.json](../connectors/s3-sink-purchases.json)에 있습니다.

| 설정 | 현재 값 | 의도 | 운영 시 보완 |
|---|---|---|---|
| `format.class` | `ParquetFormat` | batch 분석에 적합한 columnar format으로 저장합니다. | schema evolution과 compaction 전략을 함께 검토합니다. |
| `parquet.codec` | `snappy` | 압축률과 읽기 성능의 균형을 잡습니다. | 저장 비용과 query 성능 기준으로 codec을 선택합니다. |
| `partitioner.class` | `TimeBasedPartitioner` | `dt=YYYY-MM-dd/hour=HH` 경로로 시간 기반 batch 처리를 쉽게 합니다. | timezone, late event, partition repair 정책을 명확히 합니다. |
| `timestamp.extractor` | `RecordField` | Kafka timestamp가 아니라 event payload의 `event_ts`를 기준으로 partitioning합니다. | event timestamp 품질 검증이 필요합니다. |
| `flush.size` | clicks `100`, purchases `10` | 로컬 데모에서 빠르게 파일이 생성되도록 작게 설정했습니다. | 운영에서는 작은 파일 문제를 줄이기 위해 더 크게 조정합니다. |
| `rotate.interval.ms` | clicks `300000`, purchases `600000` | 일정 시간마다 파일을 닫아 batch job이 읽을 수 있게 합니다. | 파일 크기, 지연 허용치, downstream 주기를 기준으로 조정합니다. |
| `errors.tolerance` | `all` | 변환 실패 이벤트를 전체 connector 장애로 만들지 않고 DLQ로 보냅니다. | 장애 은폐 위험이 있어 DLQ 모니터링과 alert가 필수입니다. |
| `errors.deadletterqueue.topic.name` | `km.events.dlq.v1` | 실패 이벤트를 별도 topic에 보관합니다. | DLQ 원인 분석과 재처리 절차가 필요합니다. |

현재 설정은 로컬 검증을 위해 file flush가 빠르게 일어나도록 잡혀 있습니다. 운영에서는 너무 작은 Parquet 파일이 많이 생기면 Spark batch 성능이 떨어지므로 compaction 또는 flush 정책 조정이 필요합니다.

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

## 왜 Airflow를 사용했는가

Airflow는 단순 cron보다 무겁습니다. 이 프로젝트 규모에서는 cron, Python scheduler, Makefile, Docker command만으로도 배치 실행은 가능합니다. 그래도 Airflow를 사용한 이유는 데이터 파이프라인 운영에서 중요한 orchestration 개념을 보여주기 위해서입니다.

이 프로젝트의 Airflow 역할은 세 가지입니다.

- Spark batch job orchestration: daily metrics, popular items, session funnel
- 운영/품질 관리: DLQ offset 감지와 알림
- lifecycle 관리: anomaly table partition 생성/정리

주요 DAG:

| DAG | 역할 |
|---|---|
| `spark_batch_processing` | 전날 데이터를 기준으로 일별 KPI, 인기 상품, 전환 funnel을 계산합니다. |
| `dlq_alert_dag` | DLQ topic offset 증가를 감지하고 warning/critical 알림을 보냅니다. |
| `partition_manager` | `anomaly_sessions`의 향후 partition 생성과 오래된 partition 정리를 수행합니다. |

## Airflow 설정 의도

Airflow 설정은 [docker/docker-compose.yml](../docker/docker-compose.yml)과 [airflow/dags](../airflow/dags)에 있습니다.

| 설정 | 현재 값 | 의도 | 운영 시 보완 |
|---|---|---|---|
| executor | `LocalExecutor` | Docker Compose 단일 노드에서 병렬 task 실행을 실습하기 위한 선택입니다. | 운영에서는 CeleryExecutor, KubernetesExecutor 등을 검토합니다. |
| metadata DB | 별도 PostgreSQL | Airflow metadata와 서비스 DB를 분리합니다. | 운영에서는 백업, migration, 접근 권한을 별도로 관리합니다. |
| `DAGS_ARE_PAUSED_AT_CREATION` | `True` | 새 DAG가 자동 실행되는 것을 막아 데모 환경에서 의도치 않은 실행을 줄입니다. | 운영에서도 배포 후 enable 절차를 명확히 합니다. |
| `LOAD_EXAMPLES` | `False` | 예제 DAG를 제거해 UI와 테스트를 단순화합니다. | 운영에서도 일반적으로 끕니다. |
| batch schedule | `0 18 * * *` | UTC 18시는 KST 기준 다음 날 03시로, 전날 데이터를 집계하기 위한 시간입니다. | timezone 설정과 데이터 도착 지연을 함께 고려해야 합니다. |
| `catchup` | `False` | 로컬 데모에서 과거 logical date가 한꺼번에 실행되는 것을 방지합니다. | backfill 필요 시 별도 DAG run이나 catchup 전략을 사용합니다. |
| retries | `1` | 일시적 오류를 한 번 재시도합니다. | task별 오류 성격에 맞춰 retry/backoff를 세분화합니다. |
| DLQ schedule | `*/1 * * * *` | DLQ 증가를 빠르게 감지하기 위해 1분 주기로 확인합니다. | sensor mode, poke interval, scheduler 부하를 함께 고려합니다. |
| partition schedule | `0 0 * * *` | 매일 partition 생성/정리를 수행합니다. | 보존 기간, vacuum, archive 정책과 함께 운영합니다. |

## Airflow DAG 설계 의도

`spark_batch_processing` DAG는 `record_start -> daily_metrics -> [popular_items, session_funnel] -> record_end` 흐름입니다.

- `daily_metrics`를 먼저 실행해 기본 KPI를 계산합니다.
- `popular_items`와 `session_funnel`은 서로 독립적이므로 병렬 실행합니다.
- `pipeline_runs` table에 실행 시작/종료를 기록해 운영 이력을 남깁니다.
- `catchup=False`로 두어 로컬 환경에서 대량 backfill이 실수로 실행되지 않게 했습니다.

`dlq_alert_dag`는 offset 증가량을 기준으로 warning/critical을 나눕니다.

- DLQ message를 직접 소비해 없애지 않고 offset만 확인합니다.
- `alert_history` table로 같은 시간 bucket 안의 중복 알림을 줄입니다.
- Slack과 Email은 환경변수가 없으면 조용히 skip되도록 했습니다.

`partition_manager`는 anomaly table의 시간 기반 조회와 보존 관리를 위한 DAG입니다.

- 향후 7일 partition을 미리 생성합니다.
- 30일 이전 데이터를 정리합니다.
- 운영에서는 보존 기간과 archive 정책을 더 명확히 해야 합니다.

## 왜 PostgreSQL을 사용했는가

PostgreSQL은 분석 결과 서빙과 운영 이력 저장을 동시에 처리하기에 충분하고, 로컬 환경에서 실행하기 쉽습니다. 이 프로젝트에서는 다음 용도로 사용했습니다.

- anomaly result 저장
- daily metrics, funnel, popular items 저장
- pipeline_runs로 batch 실행 이력 저장
- alert_history로 DLQ 알림 중복 억제
- data_quality_checks table로 향후 품질 체크 확장 여지 확보

주요 설계:

- `anomaly_sessions`는 `detected_at` 기준 partition table입니다.
- `session_id`, `detected_at`, `anomaly_type`, `window` 관련 index를 둬 조회를 빠르게 합니다.
- batch table은 `metric_date` 중심으로 primary key를 둡니다.
- Spark batch write는 staging table + transaction swap 방식으로 partial write를 줄입니다.

운영 규모가 커지면 PostgreSQL 하나로 raw event와 분석 serving을 모두 처리하기는 어렵습니다. 이 경우 raw data는 data lake/warehouse에 두고, PostgreSQL은 dashboard serving과 운영 metadata 중심으로 제한하는 편이 적절합니다.

## 왜 MinIO를 사용했는가

MinIO는 로컬에서 S3와 유사한 object storage 패턴을 실습하기 위해 사용했습니다. AWS S3를 바로 쓰면 비용, 계정, 권한 관리가 필요하지만 MinIO는 Docker Compose 안에서 같은 구조를 재현할 수 있습니다.

이 프로젝트에서 MinIO의 역할:

- Kafka Connect S3 Sink의 target storage
- Spark batch job의 raw data source
- S3 path, bucket, partitioned parquet layout 실습

운영에서는 실제 S3/GCS/ADLS 같은 managed object storage를 쓰고, IAM 권한, lifecycle policy, encryption, compaction, table format을 함께 설계해야 합니다.

## Docker Compose 설정 의도

Docker Compose는 전체 파이프라인을 한 로컬 환경에서 재현하기 위한 선택입니다.

의도:

- Kafka, Spark, Airflow, Postgres, MinIO, API, Dashboard를 한 번에 실행
- healthcheck와 `depends_on`으로 기동 순서 문제를 줄임
- 각 서비스의 환경변수를 `.env`로 분리

한계:

- 운영 수준의 scheduler, service discovery, secret 관리가 아닙니다.
- `depends_on`은 application readiness를 완전히 보장하지 않습니다.
- container resource limit은 로컬 안정성을 위한 값이지 capacity planning 결과가 아닙니다.
- Kafka/Spark/Airflow를 모두 한 PC에서 띄우므로 실제 분산 환경과 다릅니다.

운영에 가깝게 만들려면 Kubernetes, Helm, Terraform, managed Kafka, managed Airflow, managed database, object storage, secret manager를 별도로 검토해야 합니다.

## 이 프로젝트에서 가벼운 대안을 쓰지 않은 이유

현재 데이터 규모에서는 Kafka와 Spark가 필수는 아니다. 이 구성은 이벤트 기반 처리와 분산 데이터 파이프라인의 동작 특성을 검증하기 위해 선택했다.

- Kafka topic/partition/offset/replication 이해
- Avro schema와 Schema Registry 연동
- Kafka Connect Sink와 DLQ 처리
- Spark event-time window와 watermark 처리
- checkpoint와 state schema 충돌 경험
- Airflow orchestration과 batch idempotency 설계
- PostgreSQL partition/index/운영 metadata 설계
- 장애 시뮬레이션과 SLO 측정

따라서 이 구성은 최소 비용 구현보다 각 구성 요소의 처리 특성, 장애 복구 방식, 운영상 제약을 확인하는 데 목적이 있다.

## 실제 운영이라면 다르게 할 점

- Kafka/Spark가 필요한 처리량과 요구사항을 먼저 산정합니다.
- 단순한 요구라면 PostgreSQL, Redis Stream, RabbitMQ, Python worker 같은 가벼운 대안을 먼저 검토합니다.
- Spark checkpoint는 durable storage에 저장하고, state schema 변경 정책을 문서화합니다.
- debug console query는 기본 비활성화합니다.
- streaming/backfill/replay job을 분리합니다.
- Airflow executor와 worker 구조를 실제 부하에 맞게 재설계합니다.
- Kafka topic retention, partition 수, replication factor, min ISR을 요구사항에 맞게 재산정합니다.
- Kafka Connect 작은 파일 문제를 줄이기 위해 flush/rotate/compaction 전략을 조정합니다.
- CI에서 DAG import, producer unit test, API test, schema compatibility test를 자동화합니다.
- Prometheus/Grafana 또는 OpenTelemetry로 pipeline lag, consumer lag, processing latency를 관측합니다.
