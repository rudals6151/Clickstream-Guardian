# Clickstream Guardian 포트폴리오 정리 문서

## 1. 프로젝트 한 줄 요약
실시간 클릭스트림을 Kafka로 수집하고 Spark Structured Streaming으로 이상 징후를 탐지해 PostgreSQL/API로 제공하며, 배치 집계(Airflow + Spark)와 장애/지연/부하 검증 체계까지 갖춘 데이터 엔지니어링 프로젝트입니다.

## 2. 문제 정의와 목표
- 문제: 트래픽 급증, 비정상 이벤트, 시스템 장애 상황에서 데이터 파이프라인이 얼마나 안정적으로 동작하는지 증명하기 어렵다.
- 목표:
1. 실시간 이상 탐지 파이프라인 구축
2. DLQ 기반 운영 경보 체계 구축
3. 지연/중복 이벤트를 고려한 재처리 안전성(idempotency) 확보
4. 부하/장애/복구를 수치로 검증(SLO 리포트)

## 3. 아키텍처 개요
- Ingestion: Producer -> Kafka(3 broker)
- Stream Processing: Spark Structured Streaming
- Storage/Serving: PostgreSQL + FastAPI
- Batch Analytics: Airflow DAG -> Spark Batch -> PostgreSQL
- Observability/Validation: Locust, failure simulation, SLO report script
- Alerting: DLQ topic 감지(Airflow DAG) -> Slack/Email(환경변수/연결 설정)

## 4. 핵심 구현 포인트

### 4.1 운영 안정성 중심 Kafka 설계
- 3개 브로커 클러스터 구성
- 주요 토픽 replication factor 2 구성
- Producer 내구성 설정 사용
1. `acks=all`
2. `enable.idempotence=true`
3. `retries` + backoff
- 결과: broker 1대 중단 시에도 리더/ISR 기반으로 생산 지속 가능

### 4.2 DLQ -> Sensor -> Alert 체인
- DLQ 토픽(`km.events.dlq.v1`)으로 실패 이벤트 격리
- Airflow DAG(`dlq_alert_dag.py`)로 DLQ 증가량 폴링
- 임계치 기반 경고/심각도 분기
- 중복 알림 방지(히스토리 기반 suppress) 설계

### 4.3 지연/중복 이벤트 대응
- 이벤트 모델에 `event_id`, `event_ts`, `ingest_ts`, `source` 포함
- 스트리밍에서 dedup 및 지연 이벤트 분기(late topic/DLQ) 고려
- DB upsert 및 유니크 키 기반 멱등 저장
- 재처리 시 결과 일관성 확보를 목표로 설계

## 5. 테스트 및 검증 결과 (실행 기준: 2026-02-22)

### 5.1 스모크 테스트
- `/health`, `/stats`, `/anomalies`, `/metrics/summary` 정상(HTTP 200)

### 5.2 API 부하 테스트 (Locust)
- 총 요청: 396
- 실패율: 0.00%
- 평균 응답: 14ms
- p95: 22ms, p99: 26ms

### 5.3 장애 복구 테스트
- 대상: `kafka-2`, `schema-registry`, `postgres`, `spark-streaming`
- 방식: stop-start failure simulation
- 결과: 서비스 재기동 후 스모크 테스트 정상

### 5.4 Kafka broker 다운 내성 확인
- 실험: `kafka-2` 중단 상태에서 producer 5,000건 전송
- 결과: `sent=5000`, `failed=0`, 약 `4,690 msg/sec`
- 비교(정상 상태): 약 `4,819 msg/sec`, `failed=0`
- 해석: 단일 broker 장애 시에도 복제/리더 선출 구조로 처리 지속

### 5.5 SLO 리포트
- `samples`: 4
- `p50_produce_to_kafka_ms`: 828.0
- `p50_kafka_to_spark_ms`: 2648.0
- `p50_spark_to_db_ms`: 6586.2
- `p95_e2e_ms`: 13169.5

## 6. 내가 해결한 실제 이슈
1. Airflow DAG import 실패(`kafka` 모듈 누락) 해결
2. Spark checkpoint/state schema 충돌 이슈 정리 및 재기동 안정화
3. streaming upsert SQL 컬럼/값 불일치 수정
4. DB timestamp 기본값 보강으로 쓰기 일관성 개선

## 7. 기술적 트레이드오프
- 로컬 환경에서 빠른 재현성을 우선해 일부 설정은 운영 최적값 대신 단순화
- `failOnDataLoss=false` 등 개발 편의 설정은 운영 배포 시 강화 필요
- 현재 SLO 샘플 수가 적어, 장기 부하 데이터 누적으로 신뢰구간 확대 필요

## 8. 포트폴리오에서 강조할 메시지
1. 기능 구현을 넘어서 장애/복구/지연을 수치로 검증한 프로젝트
2. 데이터 신뢰성(중복/지연/재처리)을 구조적으로 다룬 설계
3. 운영 관점(DLQ, 알림, 리포팅, failure simulation)을 포함한 E2E 완결성

## 9. 공개 저장소 품질 체크리스트
- [ ] `.env` 비밀값 제거, `.env.example`만 공개
- [ ] README 실행 절차를 실제 코드/스크립트와 1:1 동기화
- [ ] 원클릭 로컬 재현 절차 정리(`compose up` + `smoke test`)
- [ ] 테스트 결과 문서 최신화(날짜/수치)
- [ ] CI 최소 검증(정적 점검 + 스모크)

## 10. 면접 1분 설명 템플릿
"Clickstream Guardian은 Kafka-Spark-Airflow 기반의 실시간+배치 하이브리드 파이프라인입니다. 단순 구현이 아니라, 브로커 장애/서비스 재시작 상황에서도 데이터 처리가 유지되는지 직접 failure simulation과 SLO 리포트로 검증했습니다. 또한 DLQ 감지와 알림 체인을 넣어 운영 대응 시간을 줄였고, 지연/중복 이벤트를 고려한 멱등 처리로 재처리 안정성까지 설계한 것이 핵심입니다."

---
문서 버전: 1.0  
최종 갱신: 2026-02-22
