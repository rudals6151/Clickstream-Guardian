# 02. Failure Recovery Test

## 범위

Kafka -> Spark Structured Streaming -> PostgreSQL 이상 탐지 경로에 장애를 주입하고
입력 지속 여부, 복구 시간, 데이터 유실과 중복을 확인했다.

Kafka Connect와 MinIO 적재는 제외했다. 각 테스트는 12,000 EPS를 60초간 입력하고
20초 시점에 장애를 발생시켰다.

## 판정 기준

- Producer 전송 실패 0건
- Kafka offset 증가량과 Producer 전달량 일치
- 모든 Spark query가 최종 Kafka offset까지 처리
- 최종 Kafka lag 0
- PostgreSQL 결과 반영
- PostgreSQL business key 중복 0건

RTO는 장애 발생부터 모든 query가 최종 offset과 lag 0에 도달할 때까지의 시간이다.
PostgreSQL 테스트는 DB 쓰기 재개 시점까지 포함한다.

## 결과 요약

| 장애 시나리오 | 결과 | RTO | RPO | 판정 |
| --- | --- | ---: | ---: | --- |
| Spark 애플리케이션 종료 | 컨테이너 자동 재시작 후 checkpoint 복구 | 63.67초 | 0건 | PASS |
| Spark Worker 중단 | Worker 복구 후 executor 재할당 및 backlog 처리 | 62.20초 | 0건 | PASS |
| Kafka broker 1대 중단 | leader failover 및 full ISR 복구 | 62.67초 | 0건 | PASS, latency degraded |
| PostgreSQL 중단 | sink retry 후 query 재시작 없이 DB 쓰기 재개 | 61.578초 | 0건 | PASS |

## 1. Spark 애플리케이션 장애

### 방법

Spark Python 애플리케이션을 `SIGKILL`로 종료했다.

```powershell
docker exec spark-streaming-anomaly pkill -9 -f "^python3 /opt/spark-streaming/anomaly_detector.py"
```

Python 애플리케이션이 컨테이너의 주 프로세스이므로 컨테이너가 종료됐고,
`restart: unless-stopped` 정책에 따라 다시 시작됐다.

### 결과

| 항목 | 결과 |
| --- | ---: |
| Producer 전달 | 719,996건 |
| warm-up 포함 Kafka offset 증가 | 719,997건 |
| Producer 실패 | 0건 |
| 자동 재시작 관측 | 장애 후 1.47초 |
| 최종 RTO | 63.67초 |
| RPO | 0건 |
| PostgreSQL 결과 증가 | 40건 |
| 중복 business key | 0건 |

세 query는 새 run ID로 시작해 checkpoint의 마지막 성공 offset부터 처리했다.

## 2. Spark Worker 장애

### 방법

단일 Worker를 15초간 중단한 뒤 다시 시작했다.

```powershell
docker stop --timeout 0 spark-worker
Start-Sleep -Seconds 15
docker start spark-worker
```

### 결과

| 항목 | 결과 |
| --- | ---: |
| Producer 전달 | 719,996건 |
| warm-up 포함 Kafka offset 증가 | 719,997건 |
| Producer 실패 | 0건 |
| Worker 재등록 | 장애 후 15.91초 |
| executor 재할당 | 장애 후 19.97초 |
| 최대 lag (`bot_like`) | 74,726건 |
| 최종 RTO | 62.20초 |
| RPO | 0건 |
| 중복 business key | 0건 |

Worker 중단 동안 실행 가능한 executor가 없어 backlog가 증가했다.
Worker 복구 후 같은 Spark 애플리케이션에 executor가 다시 할당됐고,
checkpoint와 state를 유지한 채 backlog를 처리했다.

단일 Worker 구성은 장애 중 처리를 계속할 수 없다. 이 결과는 자동 failover가 아니라
Worker 재기동 후 복구 능력을 측정한 것이다.

## 3. Kafka broker 장애

### 방법

partition 2의 leader였던 `kafka-3`을 30초간 중단했다.

```powershell
docker stop --timeout 0 kafka-3
Start-Sleep -Seconds 30
docker start kafka-3
```

Producer와 Spark는 broker 3대를 모두 bootstrap server로 사용했다.

### 결과

| 항목 | 결과 |
| --- | ---: |
| Producer 전달 | 720,000건 |
| warm-up 포함 Kafka offset 증가 | 720,001건 |
| Producer 실패 | 0건 |
| Producer delivery p95 | 16.46초 |
| leader failover | partition 2: broker 3 -> broker 2 |
| full ISR 복구 | 성공 |
| 최종 RTO | 62.67초 |
| RPO | 0건 |
| 중복 business key | 0건 |

broker 중단 중 partition 2 leader가 `kafka-2`로 변경됐다. `kafka-3` 복귀 후
leader는 `kafka-2`에 유지됐고 replica가 ISR에 재합류했다.

복구와 데이터 정합성은 기준을 만족했지만 leader 전환 중 Producer 지연이 증가했다.

현재 topic은 RF=2, `min.insync.replicas=1`이다. `acks=all`을 사용해도 ISR이
1개인 상태에서 쓰기가 가능하므로 가용성은 유지되지만 추가 replica 장애에 취약하다.
운영 구성에서는 RF=3, `min.insync.replicas=2`, `acks=all`을 적용한다.

## 4. PostgreSQL 장애

### 적용한 변경

- PostgreSQL `restart: unless-stopped`
- DB 연결 timeout 3초
- 최대 6회 exponential backoff
- 재시도 실패 시 예외를 발생시켜 checkpoint commit 방지
- 기존 idempotent upsert 유지

### 방법

PostgreSQL을 15초간 중단한 뒤 다시 시작했다.

```powershell
docker stop --timeout 0 postgres
Start-Sleep -Seconds 15
docker start postgres
```

### 결과

| 항목 | 결과 |
| --- | ---: |
| Producer 전달 | 720,000건 |
| warm-up 포함 Kafka offset 증가 | 720,001건 |
| Producer 실패 | 0건 |
| Spark Streaming 재시작 | 0회 |
| query termination | 0건 |
| 최대 lag (`high_frequency`) | 68,309건 |
| 최종 RTO | 61.578초 |
| RPO | 0건 |
| PostgreSQL 결과 증가 | 40건 |
| 중복 business key | 0건 |

DB 연결 오류는 같은 micro-batch 안에서 재시도됐다. PostgreSQL 복구 후 쓰기가 완료됐고,
Spark 애플리케이션과 query run ID는 유지됐다.

단일 PostgreSQL 인스턴스이므로 호스트나 스토리지 장애에 대한 자동 failover는 제공하지 않는다.

## 결과 파일

- `docs/testing/results/02_01_spark_application_failure.json`
- `docs/testing/results/02_02_spark_worker_failure.json`
- `docs/testing/results/02_03_kafka_broker_failure.json`
- `docs/testing/results/02_04_postgres_failure.json`
