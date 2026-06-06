# 02 Kafka Producer 처리량 테스트

## 목적

Kafka producer가 정상 상태와 broker 장애 상태에서 어느 정도 이벤트를 안정적으로 전송할 수 있는지 측정한다.

## 현재 상태

- 기존 스크립트: `scripts/producer_throughput_test.py`
- 기존 기록: normal 약 4,819 msg/sec, `kafka-2` down 약 4,690 msg/sec, 실패 0건
- 보완 필요: 데이터 규모별 반복 측정, producer error count, broker down 전후 비교

## 테스트 단계

| 단계 | 이벤트 수 | 조건 | 목적 |
|---|---:|---|---|
| Baseline | 10,000 | 정상 broker | 기본 처리량 |
| Medium | 100,000 | 정상 broker | sustained throughput |
| Large | 500,000 | 정상 broker | 장시간 처리 안정성 |
| Broker-down | 100,000 | `kafka-2` 중단 | 단일 broker 장애 영향 |
| Recovery | 100,000 | broker 재기동 후 | 복구 후 처리량 |

## 실행 명령

```bash
python scripts/producer_throughput_test.py --duration 60 --producer-cmd "python producers/producer_clicks.py --anomaly-interval 20 --max-events 100000"
python scripts/failure_simulation.py --mode stop-start --services kafka-2 --down-seconds 60
python scripts/producer_throughput_test.py --duration 60 --producer-cmd "python producers/producer_clicks.py --anomaly-interval 20 --max-events 100000"
```

## 측정 지표

- 시작 offset
- 종료 offset
- 생산 이벤트 수
- 평균 events/sec
- producer delivery error count
- broker down 중 실패 수
- topic partition별 offset 증가량

## 성공 기준

- 정상 상태: producer 실패 0건
- broker 1대 중단: producer 전송 지속, 실패율 1% 이하
- broker 복구 후: 처리량 회복 확인

## 실패 시 원인 분석

- `acks=all`과 ISR 조건 때문에 produce가 막혔는가?
- 특정 partition leader가 down broker에 있었는가?
- retry timeout이 짧았는가?
- broker 복구 후 leader election이 정상적으로 되었는가?

## 결과 기록

| 단계 | 조건 | Produced | Failed | Avg msg/sec | 비고 |
|---|---|---:|---:|---:|---|
| Baseline | | | | | |
| Medium | | | | | |
| Large | | | | | |
| Broker-down | | | | | |
| Recovery | | | | | |

