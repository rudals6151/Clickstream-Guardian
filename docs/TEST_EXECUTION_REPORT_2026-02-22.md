# 테스트 실행 결과 보고서
## Clickstream Guardian End-to-End Validation

작성일시: 2026-02-22 (KST)  
실행 환경: Windows + Docker Compose (`docker/docker-compose.yml`)

---

## 1. 실행 범위
요청된 순서대로 아래 항목을 실제 실행하고 결과를 확인했다.

1. Airflow 이미지 빌드
2. 전체 서비스 기동
3. DB 마이그레이션
4. API 스모크 테스트
5. 부하 테스트(Locust, `localhost:8000`)
6. 장애 테스트(`failure_simulation.py --mode stop-start`)
7. SLO 리포트 생성

---

## 2. 요약 결과

| 항목 | 결과 | 비고 |
|---|---|---|
| Airflow 빌드 | 성공(재시도 후) | 1차는 Docker 엔진 연결 오류 |
| 서비스 기동 | 성공 | 핵심 컨테이너 Up 확인 |
| 마이그레이션 | 성공 | `scripts/migrate_schema.sql` 적용 완료 |
| 스모크 테스트 | 성공 | 4개 endpoint 모두 HTTP 200 |
| 부하 테스트 | 성공 | 실패율 0%, 평균 응답 14ms (최근 실행) |
| 장애 테스트 | 성공 | 4개 핵심 서비스 stop/start 복구 확인 |
| SLO 리포트 | 성공 | 샘플 4건, p95 E2E 13169ms |

---

## 3. 실행 상세

### 3.1 빌드 / 기동
- 명령:
  - `docker compose -f docker/docker-compose.yml build airflow-webserver airflow-scheduler`
  - `docker compose -f docker/docker-compose.yml up -d`
- 결과:
  - 최초 빌드 시 Docker 엔진 pipe 연결 실패(일시)
  - 재시도 후 빌드 성공
  - 전체 서비스 `Up` 상태 확인

### 3.2 마이그레이션
- 명령:
  - `Get-Content scripts/migrate_schema.sql | docker exec -i postgres psql -U admin -d clickstream`
- 결과:
  - `ALTER TABLE`, `CREATE INDEX`, `CREATE TABLE` 모두 성공

### 3.3 스모크 테스트
- 명령:
  - `python scripts/smoke_test.py --base-url http://localhost:8000`
- 결과:
  - `/health`: 200
  - `/stats`: 200
  - `/anomalies?limit=5`: 200
  - `/metrics/summary?days=7`: 200

### 3.4 부하 테스트
- 명령:
  - `python -m locust -f scripts/load_test.py --host http://localhost:8000 --headless -u 15 -r 5 -t 20s --only-summary`
- 결과(최근 실행):
  - 총 요청: 396
  - 실패: 0 (0.00%)
  - Aggregated 평균 응답: 14ms
  - Aggregated p95: 22ms, p99: 26ms

### 3.5 장애 테스트
- 명령:
  - `python scripts/failure_simulation.py --mode stop-start --down-seconds 20`
- 대상:
  - `kafka-2`, `schema-registry`, `postgres`, `spark-streaming`
- 결과:
  - 각 서비스 stop/start 완료
  - 테스트 후 스모크 재검증 200 OK

### 3.6 SLO 리포트
- 명령:
  - `python scripts/generate_slo_report.py --hours 24`
- 결과(JSON):
  - `samples`: 4
  - `p50_produce_to_kafka_ms`: 828.0
  - `p50_kafka_to_spark_ms`: 2648.0
  - `p50_spark_to_db_ms`: 6586.2185
  - `p50_e2e_ms`: 10094.2185
  - `p95_e2e_ms`: 13169.472
  - `p99_e2e_ms`: 13409.472

---

## 4. 실행 중 발견 이슈 및 조치

### 이슈 A: DLQ DAG import 실패 (`kafka` 모듈 없음)
- 증상:
  - Airflow `list-import-errors`에 `ModuleNotFoundError: No module named 'kafka'`
- 조치:
  - `docker/airflow/Dockerfile`에 `kafka-python==2.0.2` 추가
  - Airflow 이미지 재빌드 및 재기동
- 상태:
  - 해결 (`dlq_alert_dag` 목록 확인됨)

### 이슈 B: Spark checkpoint/state schema 충돌
- 증상:
  - `StateSchemaNotCompatible`, `1.delta does not exist` 등으로 스트리밍 실패
- 조치:
  - `spark-streaming` 중지 후 체크포인트 경로 정리
  - `spark-master`, `spark-worker`의 `/tmp/spark-checkpoint/anomaly` 삭제
  - 스트리밍 재기동
- 상태:
  - 해결(신규 배치 처리 및 DB upsert 확인)

### 이슈 C: 스트리밍 upsert SQL 컬럼/값 개수 불일치
- 증상:
  - `INSERT has more target columns than expressions`
- 원인:
  - `spark-streaming/anomaly_detector.py`의 `INSERT` 컬럼 수와 values tuple 수 불일치
- 조치:
  - `INSERT` 컬럼 정합성 수정
  - `db_write_ts` 기본값 설정:
    - `ALTER TABLE anomaly_sessions ALTER COLUMN db_write_ts SET DEFAULT CURRENT_TIMESTAMP;`
- 상태:
  - 해결(telemetry row 생성 확인)

---

## 5. 최종 상태 확인

- 컨테이너 상태:
  - `docker compose -f docker/docker-compose.yml ps` 기준 핵심 서비스 `Up`
- Airflow DAG:
  - `dlq_alert_dag` 로드 확인
- API:
  - 스모크 테스트 정상
- 스트리밍:
  - 이상치 탐지 후 PostgreSQL upsert 동작 확인

---

## 6. 잔여 리스크 / 개선 포인트

1. 환경변수 경고
- `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__WEBSERVER__SECRET_KEY`, Slack/Email 값 미설정 경고가 반복 출력됨.
- 운영/포트폴리오 공개용으로는 `.env` 또는 secret manager 설정 필요.

2. SLO 표본 수
- 현재 SLO 샘플은 4건으로 통계 신뢰도가 낮음.
- 최소 수백 건 이상 수집 후 p95/p99 재산출 권장.

3. Compose 경고
- `docker/docker-compose.yml`의 `version` 필드가 obsolete 경고를 유발.
- 제거 권장.

---

## 7. 결론

요청된 테스트 시나리오는 모두 실제 실행했고, 중간 장애/오류를 수정한 뒤 최종적으로 **E2E 동작 확인 + 성능/복구/SLO 수치 확보**까지 완료했다.  
현재 상태는 포트폴리오 시연 가능한 수준이며, 남은 작업은 **시크릿 설정 정리**와 **SLO 표본 확대**다.
