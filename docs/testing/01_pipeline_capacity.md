# 01. Pipeline Capacity Test

## 결론

Kafka -> Spark Structured Streaming -> PostgreSQL 이상 탐지 핵심 경로는 로컬 60초 단기 부하 기준으로
**15,000 EPS까지 안정 처리**를 확인했다.

20,000 EPS에서는 Kafka와 PostgreSQL은 버텼지만, Spark `BOT_LIKE` stateful query의 trigger p95가
8.78초까지 증가해 5초 실시간 처리 기준을 초과했다. 따라서 실패 원인은 Kafka나 PostgreSQL이 아니라
**2-core Spark Worker의 stateful window/dedup 연산 병목**으로 판단했다.

장기 운영 가능 처리량은 아직 확정하지 않았다. 운영 여유를 고려한 후속 soak test 시작값은
**12,000 EPS**로 잡는다.

## 테스트 범위

| 항목 | 내용 |
| --- | --- |
| 측정 경로 | Producer -> Kafka -> Spark Streaming -> PostgreSQL |
| 제외 범위 | Kafka Connect, MinIO/S3 sink |
| 테스트 단위 | EPS별 60초 단기 부하 |
| 안정 기준 | Producer 실패 0건, 최종 Kafka lag 0, Spark stateful trigger p95 5초 이하 |
| 데이터 처리 | Avro decode, late-event 분리, 10분 watermark, `event_id` dedup, HIGH_FREQUENCY/BOT_LIKE 탐지, PostgreSQL upsert |

## 로컬 리소스 기준

| 구성 요소 | 설정 |
| --- | --- |
| Kafka | 3 brokers, broker heap 최대 512 MiB |
| Kafka topic | 3 partitions, RF=2 |
| Spark Worker | 1 worker, 2 cores, executor memory 2 GiB |
| Spark Streaming driver | driver memory 2 GiB, container limit 4 GiB |
| Spark shuffle partitions | 2 |
| Spark state store | RocksDB, bounded memory |
| PostgreSQL | 별도 CPU/memory 제한 없음 |

## 단계별 결과

| 목표 EPS | 생산 결과 | Producer p95 | High p95 | Bot p95 | 최종 lag | 판정 |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1,000 | 60,000건, 실패 0 | 22.06 ms | 3.54초 | 3.89초 | 0 | PASS |
| 3,000 | 180,000건, 실패 0 | 20.76 ms | 3.59초 | 3.63초 | 0 | PASS |
| 5,000 | 299,998건, 실패 0 | 20.10 ms | 4.20초 | 4.54초 | 0 | PASS |
| 7,500 | 449,998건, 실패 0 | 17.33 ms | 2.28초 | 2.40초 | 0 | PASS |
| 10,000 | 599,998건, 실패 0 | 17.25 ms | 2.84초 | 3.44초 | 0 | PASS |
| 15,000 | 899,997건, 실패 0 | 15.56 ms | 3.74초 | 4.47초 | 0 | PASS |
| 20,000 | 1,200,001건, 실패 0 | 15.21 ms | 4.91초 | **8.78초** | 0 | FAIL |

## 병목 판단

20,000 EPS에서도 Producer 실패는 0건이고 최종 Kafka lag도 0으로 회복됐다.
PostgreSQL CPU는 최대 4.87% 수준이라 DB 병목으로 보기 어렵다.

반면 Spark Worker CPU는 최대 199.14%까지 사용됐고, `BOT_LIKE` state row는 약 371만 건까지 증가했다.
`BOT_LIKE`는 1분 sliding window와 10분 watermark 기반 deduplication state를 함께 사용하므로,
입력량 증가 시 state 관리 비용이 가장 먼저 커졌다.

`BOT_LIKE`가 먼저 병목이 된 이유는 다음과 같다.

- 현재 이벤트만 판단하지 않고 `session_id`별 최근 1분 window 상태를 유지한다.
- 1분 window를 30초 간격으로 sliding하므로 이벤트 하나가 둘 이상의 window에 반영될 수 있다.
- `session_id + window` 조합이 state key가 되며, 입력량이 늘수록 유지해야 하는 key와 aggregate state가 증가한다.
- 같은 key끼리 집계하기 위해 Spark shuffle이 발생한다.
- upstream에서 10분 watermark 기반 `event_id` deduplication state도 함께 관리한다.
- watermark 기준으로 늦은 이벤트를 기다려야 하므로 window가 끝나도 state를 즉시 삭제할 수 없다.
- RocksDB state store 조회/업데이트 비용이 batch마다 누적된다.

```text
검증된 안정 처리량: 15,000 EPS
최초 SLO 초과 지점: 20,000 EPS
주요 병목: Spark Worker CPU + stateful BOT_LIKE 연산
Kafka 병목: 확인되지 않음
PostgreSQL 병목: 확인되지 않음
권장 후속 soak test 입력값: 12,000 EPS
```

## 해석

15,000 EPS는 “60초 단기 부하에서 안정 처리 확인”이라는 의미다.
장기 운영 가능 처리량으로 표현하려면 12,000 EPS 30분~1시간 soak test 또는
15,000 EPS 10~30분 지속 테스트가 추가로 필요하다.

20,000 EPS는 데이터 유실 실패가 아니라 실시간 처리 SLO 실패다.
즉 최종 처리는 완료했지만, `BOT_LIKE` batch p95가 trigger 주기를 초과해 실시간성 기준을 만족하지 못했다.
