# Clickstream Guardian 🛡️

![Architecture](diagram/architecture.png)

**실시간 클릭스트림 이상 탐지 + 배치 분석 데이터 파이프라인**

YOOCHOOSE 전자상거래 클릭 및 구매 로그를 기반으로 실시간 세션 모니터링, Raw 데이터 보존, 일별 배치 분석, API 서빙을 수행하는 엔드투엔드 데이터 엔지니어링 프로젝트입니다.

## 📋 목차

- [프로젝트 개요](#프로젝트-개요)
- [주요 기능](#주요-기능)
- [아키텍처](#아키텍처)
- [기술 스택](#기술-스택)
- [시작하기](#시작하기)
- [사용법](#사용법)
- [프로젝트 구조](#프로젝트-구조)
- [API 문서](#api-문서)
- [운영 및 장애 대응 문서](#운영-및-장애-대응-문서)

## 🎯 프로젝트 개요

본 프로젝트는 실제 서비스 환경에서 발생할 수 있는 **특정 세션 폭주와 같은 과부하 상황**을 가정하여 다음 설계 원칙을 따릅니다:

- ✅ Kafka는 **수집 및 완충 계층**으로 사용
- ✅ Raw 데이터는 **Kafka 유입 즉시 S3에 영구 저장**
- ✅ Spark Streaming은 **실시간 모니터링 전용**
- ✅ RDB는 **상태/집계 서빙 전용** (고빈도 이벤트 저장 회피)

## ✨ 주요 기능

### 1. 실시간 이상 탐지 (Spark Structured Streaming)
- 10초 Tumbling Window: 고빈도 클릭 탐지 (50+ clicks)
- 1분 Sliding Window: 봇 패턴 탐지 (100+ clicks + 낮은 아이템 다양성)
- 실시간으로 PostgreSQL에 이상 세션 저장

### 2. 데이터 레이크 (S3/MinIO)
- Kafka Connect를 통한 자동 Raw 데이터 적재
- Parquet 포맷 + Snappy 압축
- 시간 기반 파티셔닝 (`dt=YYYY-MM-DD/hour=HH`)

### 3. 일별 배치 분석 (Spark Batch + Airflow)
- **Daily Metrics**: 클릭/구매/전환율/매출 통계
- **Session Funnel**: 뷰 → 멀티뷰 → 구매 전환 퍼널
- **Popular Items**: Top 100 상품 (클릭/구매/매출 기준)

### 4. API 서빙 (FastAPI)
- RESTful API로 실시간/배치 데이터 제공
- Swagger UI 자동 생성
- PostgreSQL Read-only 쿼리

### 5. 모니터링 대시보드 (Streamlit)
- 실시간 이상 탐지 현황
- 일별 메트릭 시각화
- 인기 상품/카테고리 분석

## 🏗️ 아키텍처

```
┌─────────────┐
│   CSV Data  │
└──────┬──────┘
       │
       ▼
┌─────────────────┐      ┌──────────────┐
│ Kafka Producers │─────▶│    Kafka     │
│  (Avro + SR)    │      │  (3 Brokers) │
└─────────────────┘      └──────┬───────┘
                                 │
                    ┌────────────┼────────────┐
                    │            │            │
                    ▼            ▼            ▼
          ┌─────────────┐ ┌──────────┐ ┌──────────────┐
          │   Kafka     │ │  Spark   │ │    Spark     │
          │  Connect    │ │Streaming │ │    Batch     │
          │ (S3 Sink)   │ │          │ │  (Airflow)   │
          └──────┬──────┘ └────┬─────┘ └──────┬───────┘
                 │             │               │
                 ▼             ▼               ▼
          ┌──────────┐   ┌──────────┐   ┌──────────┐
          │   S3     │   │PostgreSQL│   │PostgreSQL│
          │(MinIO)   │   │(Anomaly) │   │(Metrics) │
          │Data Lake │   └────┬─────┘   └────┬─────┘
          └──────────┘        │               │
                              └───────┬───────┘
                                      ▼
                              ┌───────────────┐
                              │   FastAPI     │
                              └───────┬───────┘
                                      ▼
                              ┌───────────────┐
                              │   Dashboard   │
                              └───────────────┘
```

## 🛠️ 기술 스택

| Category | Technologies |
|----------|-------------|
| **Data Ingestion** | Apache Kafka 3.5, Schema Registry, Kafka Connect |
| **Streaming** | Spark Structured Streaming 3.5.0 |
| **Batch Processing** | Apache Spark 3.5.0, Apache Airflow 2.8.0 |
| **Storage** | MinIO (S3), PostgreSQL 15 |
| **API** | FastAPI 0.109.0, Uvicorn |
| **Dashboard** | Streamlit 1.30.0 |
| **Serialization** | Apache Avro |
| **Container** | Docker, Docker Compose |
| **Language** | Python 3.11 |

## 🚀 시작하기

### 사전 요구사항

- Docker Desktop (Windows/Mac) 또는 Docker + Docker Compose (Linux)
- Python 3.11+
- 최소 16GB RAM 권장
- 최소 50GB 디스크 공간

### 1. 프로젝트 클론

```bash
git clone https://github.com/yourusername/Clickstream-Guardian.git
cd Clickstream-Guardian
```

### 2. 전체 인프라 시작

```bash
cd docker
docker-compose up -d
```

### 3. 초기 설정 (Kafka 토픽, 스키마, Connector)

```bash
# Kafka 토픽 생성
bash scripts/create_topics.sh

# Avro 스키마 등록
cd producers
python common/schema_registry.py http://localhost:8081 ../schemas

# Kafka Connect 설정
bash ../scripts/setup_connectors.sh
```

### 4. 데이터 프로듀싱

```bash
cd producers

# 클릭 이벤트 전송 (샘플 10,000건)
python producer_clicks.py --max-events 10000

# 구매 이벤트 전송 (샘플 1,000건)
python producer_purchases.py --max-events 1000
```

### 5. Spark Streaming 시작

```bash
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
              org.apache.spark:spark-avro_2.12:3.5.0,\
              org.postgresql:postgresql:42.6.0 \
  --master spark://spark-master:7077 \
  /opt/spark-streaming/anomaly_detector.py
```

### 6. 배치 분석 실행 (특정 날짜)

```bash
# 일별 메트릭
docker exec spark-master spark-submit \
  --packages org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-batch/daily_metrics.py 2014-04-07

# 세션 퍼널
docker exec spark-master spark-submit \
  --packages org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-batch/session_funnel.py 2014-04-07

# 인기 상품
docker exec spark-master spark-submit \
  --packages org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-batch/popular_items.py 2014-04-07
```

## 📊 사용법

### 서비스 접속

| Service | URL | Credentials |
|---------|-----|-------------|
| **Dashboard** | http://localhost:8501 | - |
| **API (Swagger)** | http://localhost:8000/docs | - |
| **Airflow** | http://localhost:8082 | admin / admin |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Spark Master** | http://localhost:8080 | - |

### API 예제

```bash
# 이상 세션 조회
curl http://localhost:8000/anomalies?limit=10

# 일별 메트릭 조회
curl "http://localhost:8000/metrics/daily?start_date=2014-04-07&end_date=2014-04-10"

# 인기 상품 조회
curl http://localhost:8000/items/popular/2014-04-07?limit=50
```

### 부하 테스트 (선택)

```bash
# Locust 설치
pip install locust

# 부하 테스트 실행
locust -f scripts/load_test.py --host http://localhost:8000
```

웹 UI: http://localhost:8089

## 📁 프로젝트 구조

```
Clickstream-Guardian/
├── data/                       # 원본 CSV 데이터
│   ├── yoochoose-clicks.dat
│   └── yoochoose-buys.dat
│
├── docker/                     # Docker 설정
│   ├── docker-compose.yml
│   ├── spark/Dockerfile
│   ├── airflow/Dockerfile
│   └── postgres/init.sql
│
├── schemas/                    # Avro 스키마
│   ├── click-event.avsc
│   ├── purchase-event.avsc
│   └── anomaly-event.avsc
│
├── producers/                  # Kafka Producer
│   ├── common/
│   │   ├── config.py
│   │   └── schema_registry.py
│   ├── producer_clicks.py
│   └── producer_purchases.py
│
├── connectors/                 # Kafka Connect 설정
│   ├── s3-sink-clicks.json
│   └── s3-sink-purchases.json
│
├── spark-streaming/            # Spark Streaming
│   ├── common/
│   │   ├── kafka_utils.py
│   │   └── postgres_utils.py
│   └── anomaly_detector.py
│
├── spark-batch/                # Spark Batch
│   ├── common/s3_utils.py
│   ├── daily_metrics.py
│   ├── session_funnel.py
│   └── popular_items.py
│
├── airflow/                    # Airflow DAG
│   └── dags/
│       └── daily_batch_pipeline.py
│
├── api/                        # FastAPI
│   ├── models/database.py
│   ├── routers/
│   │   ├── anomaly.py
│   │   ├── metrics.py
│   │   └── sessions.py
│   ├── config.py
│   └── main.py
│
├── dashboard/                  # Streamlit Dashboard
│   └── app.py
│
├── scripts/                    # 유틸리티 스크립트
│   ├── setup.sh
│   ├── start.sh
│   ├── stop.sh
│   ├── create_topics.sh
│   ├── setup_connectors.sh
│   └── load_test.py
│
└── README.md
```

## 📖 API 문서

FastAPI는 자동으로 Swagger UI를 생성합니다: http://localhost:8000/docs

### 주요 엔드포인트

#### Anomalies
- `GET /anomalies` - 최근 이상 세션 조회
- `GET /anomalies/types` - 이상 유형별 통계
- `GET /anomalies/timeline` - 시간대별 이상 발생 추이
- `GET /anomalies/{session_id}` - 특정 세션 이상 조회

#### Metrics
- `GET /metrics/daily` - 일별 메트릭 조회
- `GET /metrics/daily/{date}` - 특정 날짜 메트릭
- `GET /metrics/funnel/{date}` - 전환 퍼널 분석
- `GET /metrics/summary` - 요약 통계

#### Items
- `GET /items/popular/{date}` - 인기 상품
- `GET /items/categories/{date}` - 인기 카테고리
- `GET /items/trending` - 트렌딩 상품

## 🔧 설정

### 환경 변수

Producer, API 등에서 사용하는 주요 환경 변수:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081

# Database
DATABASE_URL=postgresql://admin:password@localhost:5432/clickstream

# Replay Speed (1.0 = 실시간, 100.0 = 100배속)
REPLAY_SPEED=1.0
```

### Spark 설정

`spark-batch/daily_metrics.py` 등에서 Spark 설정 변경 가능:

```python
spark = SparkSession.builder \
    .appName("DailyMetrics") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()
```

## 🧪 테스트

```bash
# API 헬스 체크
curl http://localhost:8000/health

# Kafka 토픽 확인
docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:29092

# PostgreSQL 데이터 확인
docker exec -it postgres psql -U admin -d clickstream -c "SELECT COUNT(*) FROM anomaly_sessions;"
```

## 🛑 중지 및 정리

```bash
# 모든 서비스 중지
bash scripts/stop.sh

# 볼륨까지 삭제 (데이터 초기화)
cd docker
docker-compose down -v
```

## 📚 운영 및 장애 대응 문서

시스템 운영, 부하 테스트, 장애 대응에 대한 상세 문서가 준비되어 있습니다.

### 📖 주요 문서
- **[운영 문서 인덱스](docs/OPERATIONS_README.md)** - 모든 운영 문서의 시작점
- **[부하 시나리오 설계](docs/LOAD_TEST_SCENARIO.md)** - 부하 테스트 계획 및 실행
- **[장애 시나리오 및 대응 전략](docs/FAILURE_RECOVERY_STRATEGY.md)** - 컴포넌트별 장애 복구 Runbook
- **[모니터링 전략](docs/MONITORING_STRATEGY.md)** - 시스템 모니터링 지표 및 수집 방법
- **[Fallback / Alert 전략](docs/FALLBACK_ALERT_STRATEGY.md)** - 알림 설정 및 Fallback 로직

### 🛠️ 장애 시뮬레이션
실제 장애 상황을 시뮬레이션하여 복구 절차를 테스트할 수 있습니다:

```bash
# 시나리오 목록 확인
python scripts/failure_simulation.py --list

# Kafka Broker Down 시나리오 실행
python scripts/failure_simulation.py --scenario 1

# 대화형 모드
python scripts/failure_simulation.py
```

**사용 가능한 시나리오**:
1. Kafka Broker Down - Broker 강제 종료 및 Failover 테스트
2. Consumer Lag Spike - Spark Streaming 중지로 Lag 발생
3. Spark OOM (Simulated) - Worker 강제 종료로 OOM 시뮬레이션
4. PostgreSQL Connection Pool Exhaustion - 다수 연결 생성
5. Data Corruption (DLQ Test) - 잘못된 스키마 전송

### 📊 시스템 대시보드
- **Streamlit Dashboard**: http://localhost:8501 - 비즈니스 메트릭
- **Spark Master UI**: http://localhost:8080 - Spark 클러스터 상태
- **Spark Streaming UI**: http://localhost:4040 - 실시간 Job 모니터링
- **Airflow UI**: http://localhost:8082 - DAG 실행 상태
- **Kafka Connect**: http://localhost:8083 - Connector 상태

---

## 📝 라이센스

MIT License

## 👥 기여

Contributions are welcome! Please feel free to submit a Pull Request.

## 📧 문의

프로젝트에 대한 질문이나 제안사항이 있으시면 이슈를 등록해주세요.

---

**Built with ❤️ using Kafka, Spark, Airflow, FastAPI, and Streamlit**
