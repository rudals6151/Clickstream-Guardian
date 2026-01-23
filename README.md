# 🛡️ Clickstream Guardian

**실시간 클릭스트림 이상 탐지 및 배치 분석 데이터 파이프라인**

![Architecture](diagram/architecture.png)

YOOCHOOSE 전자상거래 클릭 및 구매 로그를 기반으로 실시간 세션 모니터링, Raw 데이터 보존, 일별 배치 분석, API 서빙을 수행하는 엔드투엔드 데이터 엔지니어링 프로젝트입니다.

---

## 📋 목차

- [프로젝트 개요](#-프로젝트-개요)
- [주요 기능](#-주요-기능)
- [시스템 아키텍처](#-시스템-아키텍처)
- [기술 스택](#-기술-스택)
- [빠른 시작](#-빠른-시작)
- [상세 사용 가이드](#-상세-사용-가이드)
- [프로젝트 구조](#-프로젝트-구조)
- [API 문서](#-api-문서)
- [모니터링 및 대시보드](#-모니터링-및-대시보드)
- [운영 및 장애 대응](#-운영-및-장애-대응)
- [테스트](#-테스트)

---

## 🎯 프로젝트 개요

### 설계 철학

본 프로젝트는 **실시간 데이터 처리와 배치 분석을 결합한 Lambda 아키텍처**를 구현합니다. 실제 프로덕션 환경에서 발생할 수 있는 대용량 트래픽과 장애 상황을 고려한 설계 원칙을 따릅니다:

#### 핵심 설계 원칙

| 원칙 | 설명 | 구현 |
|------|------|------|
| **🔄 데이터 영속성** | Raw 데이터는 유실되지 않도록 S3에 즉시 저장 | Kafka Connect S3 Sink |
| **⚡ 실시간 처리** | 스트리밍은 가벼운 모니터링 위주 | Spark Structured Streaming |
| **📊 배치 분석** | 복잡한 집계는 배치로 처리 | Spark Batch + Airflow |
| **🚨 장애 대응** | 각 레이어별 Fallback 및 Retry 전략 | DLQ, Checkpoint, Retry |
| **📈 확장성** | 수평 확장 가능한 분산 아키텍처 | Kafka Cluster, Spark Cluster |

### 데이터셋

- **출처**: [YOOCHOOSE Dataset](https://www.kaggle.com/datasets/chadgostopp/recsys-challenge-2015)
- **규모**: 약 3,300만 클릭 이벤트, 110만 구매 이벤트
- **기간**: 2014년 4월 ~ 9월 (6개월)
- **데이터 타입**: 
  - **클릭 이벤트**: `session_id`, `timestamp`, `item_id`, `category`
  - **구매 이벤트**: `session_id`, `timestamp`, `item_id`, `price`, `quantity`

---

## ✨ 주요 기능

### 1️⃣ 실시간 이상 탐지 (Spark Structured Streaming)

#### 고빈도 클릭 탐지 (HIGH_FREQUENCY)
- **윈도우**: 10초 Tumbling Window
- **조건**: 50회 이상 클릭
- **목적**: DDoS, 크롤러, 악의적 봇 탐지

#### 봇 패턴 탐지 (BOT_LIKE)
- **윈도우**: 1분 Sliding Window (30초 슬라이드)
- **조건**: 100회 이상 클릭 + 고유 아이템 5개 이하
- **목적**: 자동화 스크립트, 상품 정찰 봇 탐지

#### 처리 특징
- ✅ **Micro-batch**: 5초 간격 처리
- ✅ **Watermark**: 10초 지연 허용 (늦게 도착한 데이터 처리)
- ✅ **Output**: PostgreSQL `anomaly_sessions` 테이블에 실시간 저장
- ✅ **Avro 역직렬화**: Schema Registry 기반 스키마 진화 지원

### 2️⃣ 데이터 레이크 (S3/MinIO)

#### Kafka Connect S3 Sink
- **포맷**: Parquet + Snappy 압축
- **파티셔닝**: `dt=YYYY-MM-DD/hour=HH` 시간 기반
- **목적**: 
  - Raw 데이터 영구 보존
  - 재처리(Replay) 가능
  - 규정 준수 및 감사 추적

#### 저장 경로 구조
```
s3://km-data-lake/
├── topics/
│   ├── km.clicks.raw.v1/
│   │   └── raw_clicks/
│   │       ├── dt=2014-04-07/
│   │       │   ├── hour=00/
│   │       │   ├── hour=01/
│   │       │   └── ...
│   │       └── dt=2014-04-08/
│   └── km.purchases.raw.v1/
│       └── raw_purchases/
│           └── dt=2014-04-07/
```

### 3️⃣ 일별 배치 분석 (Spark Batch + Airflow)

#### Daily Metrics (일별 메트릭)
```sql
-- 계산 지표
- 총 클릭 수 (total_clicks)
- 총 구매 수 (total_purchases)
- 고유 세션 수 (unique_sessions)
- 고유 아이템 수 (unique_items)
- 전환율 (conversion_rate)
- 평균 세션 시간 (avg_session_duration_sec)
- 세션당 평균 클릭 (avg_clicks_per_session)
- 총 매출 (total_revenue)
- 평균 주문 금액 (avg_order_value)
```

#### Session Funnel (전환 퍼널)
```
단계 1: Single View (1회만 클릭한 세션)
  ↓
단계 2: Multi View (여러 번 클릭한 세션)
  ↓
단계 3: Purchase (구매한 세션)
```

각 단계별 세션 수, 비율, 이탈률 계산

#### Popular Items (인기 상품)
- **Top 100 상품**: 매출 기준 순위
- **집계 지표**: 클릭 수, 구매 수, 매출, 클릭-구매 전환율
- **카테고리별 집계**: Top 20 카테고리

#### Airflow DAG 스케줄링
- **실행 시점**: 매일 새벽 3시 (KST)
- **처리 범위**: 전날(D-1) 데이터
- **의존성**: daily_metrics → (popular_items + session_funnel)
- **재시도**: 실패 시 3회 재시도, 5분 간격

### 4️⃣ API 서빙 (FastAPI)

#### RESTful API
- **프레임워크**: FastAPI (ASGI 비동기)
- **자동 문서화**: Swagger UI (`/docs`), ReDoc (`/redoc`)
- **인증**: 현재 없음 (추후 JWT 추가 가능)

#### 주요 엔드포인트

**Anomalies (이상 탐지)**
```bash
GET /anomalies                    # 최근 이상 세션 조회
GET /anomalies/types              # 이상 유형별 통계
GET /anomalies/timeline           # 시간대별 이상 발생 추이
GET /anomalies/{session_id}       # 특정 세션 상세 조회
```

**Metrics (일별 메트릭)**
```bash
GET /metrics/daily                # 일별 메트릭 조회
GET /metrics/daily/{date}         # 특정 날짜 메트릭
GET /metrics/funnel/{date}        # 전환 퍼널 분석
GET /metrics/summary              # 전체 기간 요약 통계
```

**Items (상품 분석)**
```bash
GET /items/popular/{date}         # 인기 상품 Top 100
GET /items/categories/{date}      # 인기 카테고리 Top 20
GET /items/trending               # 트렌딩 상품 (최근 3일 비교)
```

### 5️⃣ 모니터링 대시보드 (Streamlit)

#### 실시간 대시보드
- **이상 탐지 현황**: 최근 1시간 이상 세션 수, 유형별 분포
- **일별 메트릭**: 클릭/구매/전환율 트렌드 그래프
- **세션 퍼널**: 단계별 전환율 시각화
- **인기 상품**: Top 10 상품 및 카테고리 차트
- **자동 갱신**: 30초마다 자동 새로고침

---

## 🏗️ 시스템 아키텍처

### 전체 아키텍처

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Data Ingestion Layer                          │
└──────────────────────────────────────────────────────────────────────┘
                                   │
        ┌──────────────────────────┼──────────────────────────┐
        │                          │                          │
        ▼                          ▼                          ▼
┌───────────────┐         ┌──────────────┐         ┌──────────────┐
│  CSV Data     │───────▶│   Kafka      │───────▶│   Schema     │
│  Source       │         │  Producers   │         │  Registry    │
│  (YOOCHOOSE)  │         │  (Avro)      │         │  (Avro)      │
└───────────────┘         └──────┬───────┘         └──────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │    Kafka Cluster       │
                    │ ┌──────┬──────┬──────┐ │
                    │ │Broker│Broker│Broker│ │
                    │ │  1   │  2   │  3   │ │
                    │ └──────┴──────┴──────┘ │
                    │  Topics:               │
                    │  - km.clicks.raw.v1    │
                    │  - km.purchases.raw.v1 │
                    │  - km.events.dlq.v1    │
                    └────────┬───────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ Kafka Connect   │  │ Spark Streaming │  │  Spark Batch    │
│  (S3 Sink)      │  │ (Anomaly)       │  │  (Analytics)    │
│                 │  │                 │  │                 │
│ • Time-based    │  │ • Tumbling      │  │ • Airflow DAG   │
│   partitioning  │  │ • Sliding       │  │ • S3 → Parquet  │
│ • Parquet       │  │ • Watermark     │  │ • Aggregation   │
│ • Snappy        │  │ • Micro-batch   │  │ • Join          │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   S3/MinIO      │  │   PostgreSQL    │  │   PostgreSQL    │
│  Data Lake      │  │  (Anomaly DB)   │  │  (Metrics DB)   │
│                 │  │                 │  │                 │
│ • Raw Data      │  │ • anomaly_      │  │ • daily_metrics │
│ • Immutable     │  │   sessions      │  │ • popular_items │
│ • Replay        │  │ • Real-time     │  │ • session_funnel│
└─────────────────┘  └────────┬────────┘  └────────┬────────┘
                              │                    │
                              └──────────┬─────────┘
                                         │
                                         ▼
┌────────────────────────────────────────────────────────────────────┐
│                        Serving Layer                               │
│                                                                    │
│   ┌───────────────────┐            ┌───────────────────┐           │
│   │    FastAPI        │            │   Streamlit       │           │
│   │    (REST API)     │────────────│   (Dashboard)     │           │
│   │                   │            │                   │           │
│   │ • /anomalies      │            │ • Real-time view  │           │
│   │ • /metrics        │            │ • Charts          │           │
│   │ • /items          │            │ • Auto-refresh    │           │
│   └───────────────────┘            └───────────────────┘           │
└────────────────────────────────────────────────────────────────────┘
```

### 데이터 흐름 (Data Flow)

#### 실시간 경로 (Speed Layer)
```
CSV Data → Kafka Producer → Kafka Cluster → Spark Streaming → PostgreSQL → API/Dashboard
                                                  (5초 마이크로배치)
```

#### 배치 경로 (Batch Layer)
```
CSV Data → Kafka Producer → Kafka Cluster → Kafka Connect → S3 Data Lake
                                              (실시간 적재)
                                                     ↓
                                              Spark Batch (Airflow)
                                              (매일 새벽 3시)
                                                     ↓
                                              PostgreSQL → API/Dashboard
```

---

## 🛠️ 기술 스택

### 데이터 처리

| 기술 | 버전 | 용도 |
|------|------|------|
| **Apache Kafka** | 3.5.0 | 분산 메시지 큐, 이벤트 스트리밍 |
| **Confluent Schema Registry** | 7.5.0 | Avro 스키마 관리 및 진화 |
| **Kafka Connect** | 7.5.0 | S3 Sink Connector |
| **Apache Spark** | 3.5.0 | 스트리밍 및 배치 데이터 처리 |
| **Apache Airflow** | 2.8.0 | 워크플로우 오케스트레이션 |

### 저장소

| 기술 | 버전 | 용도 |
|------|------|------|
| **MinIO** | latest | S3 호환 객체 스토리지 (Data Lake) |
| **PostgreSQL** | 15 | OLTP 데이터베이스 (메트릭, 이상 탐지) |
| **Zookeeper** | 3.5.0 | Kafka 클러스터 코디네이션 |

### 애플리케이션

| 기술 | 버전 | 용도 |
|------|------|------|
| **FastAPI** | 0.109.0 | RESTful API 서버 |
| **Uvicorn** | 0.27.0 | ASGI 웹 서버 |
| **Streamlit** | 1.30.0 | 인터랙티브 대시보드 |

### 직렬화 및 포맷

| 기술 | 용도 |
|------|------|
| **Apache Avro** | 스키마 기반 직렬화 (Kafka 메시지) |
| **Parquet** | 컬럼형 스토리지 포맷 (S3) |
| **Snappy** | 압축 알고리즘 |

### 인프라

| 기술 | 버전 | 용도 |
|------|------|------|
| **Docker** | 24.0+ | 컨테이너화 |
| **Docker Compose** | 2.0+ | 멀티 컨테이너 오케스트레이션 |

### 언어

| 언어 | 버전 | 용도 |
|------|------|------|
| **Python** | 3.11 | 메인 개발 언어 |
| **SQL** | - | 데이터베이스 쿼리 |
| **Bash** | - | 스크립트 자동화 |

---

## 🚀 빠른 시작

### 사전 요구사항

#### 필수
- **Docker Desktop** (Windows/Mac) 또는 **Docker + Docker Compose** (Linux)
- **Python** 3.11 이상
- **Git**

#### 권장 사양
- **RAM**: 최소 16GB (권장 32GB)
- **디스크**: 최소 50GB 여유 공간
- **CPU**: 4코어 이상

### 1단계: 프로젝트 클론

```bash
git clone https://github.com/yourusername/Clickstream-Guardian.git
cd Clickstream-Guardian
```

### 2단계: 전체 인프라 시작

```bash
cd docker
docker-compose up -d
```

**컨테이너 시작 순서 (자동)**:
1. Zookeeper
2. Kafka Brokers (kafka-1, kafka-2, kafka-3)
3. Schema Registry
4. Kafka Connect
5. PostgreSQL
6. MinIO
7. Spark Master + Workers
8. Airflow Webserver + Scheduler
9. API Server
10. Dashboard

**대기 시간**: 약 2-3분 (모든 서비스가 준비될 때까지)

### 3단계: 초기 설정

#### 3.1 Kafka 토픽 생성

```bash
# 토픽 생성
docker exec kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic km.clicks.raw.v1 \
  --partitions 3 \
  --replication-factor 2

docker exec kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic km.purchases.raw.v1 \
  --partitions 3 \
  --replication-factor 2

docker exec kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic km.events.dlq.v1 \
  --partitions 1 \
  --replication-factor 2

# 토픽 확인
docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:29092
```

#### 3.2 Avro 스키마 등록

```bash
cd producers
python common/schema_registry.py http://localhost:8081 ../schemas
```

**출력 예시**:
```
✅ Registered click-event.avsc with ID: 1
✅ Registered purchase-event.avsc with ID: 2
✅ Registered anomaly-event.avsc with ID: 3
```

#### 3.3 Kafka Connect 설정

```bash
cd ../scripts
bash setup_connectors.sh
```

**S3 Sink Connectors**:
- `s3-sink-clicks`: 클릭 이벤트 → S3
- `s3-sink-purchases`: 구매 이벤트 → S3

### 4단계: 데이터 프로듀싱

#### 클릭 이벤트

```bash
cd ../producers

# 샘플 10,000건
python producer_clicks.py --max-events 10000

# 전체 데이터 (약 3,300만 건)
python producer_clicks.py

# 빠른 속도로 (100배속)
python producer_clicks.py --replay-speed 100.0
```

#### 구매 이벤트

```bash
# 샘플 1,000건
python producer_purchases.py --max-events 1000

# 전체 데이터 (약 110만 건)
python producer_purchases.py
```

**옵션**:
- `--max-events N`: 최대 N개 이벤트만 전송
- `--replay-speed X`: X배속으로 재생 (기본 1.0)
- `--anomaly-interval N`: N초마다 이상 패턴 주입 (테스트용)

### 5단계: Spark Streaming 시작

```bash
docker exec spark-master spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
              org.apache.spark:spark-avro_2.12:3.5.0,\
              org.postgresql:postgresql:42.6.0 \
  --master spark://spark-master:7077 \
  --executor-memory 2g \
  --executor-cores 2 \
  /opt/spark-streaming/anomaly_detector.py
```

**확인**:
- Spark UI: http://localhost:4040
- PostgreSQL에 데이터 확인:
  ```bash
  docker exec -it postgres psql -U admin -d clickstream -c "SELECT COUNT(*) FROM anomaly_sessions;"
  ```

### 6단계: 배치 분석 실행

#### 수동 실행 (특정 날짜)

```bash
# 일별 메트릭
docker exec spark-master spark-submit \
  --packages org.postgresql:postgresql:42.6.0,\
              org.apache.hadoop:hadoop-aws:3.3.4,\
              com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --master spark://spark-master:7077 \
  /opt/spark-batch/daily_metrics.py 2014-04-07

# 세션 퍼널
docker exec spark-master spark-submit \
  --packages org.postgresql:postgresql:42.6.0,\
              org.apache.hadoop:hadoop-aws:3.3.4,\
              com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --master spark://spark-master:7077 \
  /opt/spark-batch/session_funnel.py 2014-04-07

# 인기 상품
docker exec spark-master spark-submit \
  --packages org.postgresql:postgresql:42.6.0,\
              org.apache.hadoop:hadoop-aws:3.3.4,\
              com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --master spark://spark-master:7077 \
  /opt/spark-batch/popular_items.py 2014-04-07
```

#### Airflow DAG 실행

1. Airflow UI 접속: http://localhost:8082
2. 로그인: `admin` / `admin`
3. DAG `daily_batch_pipeline` 활성화
4. 수동 트리거: `Trigger DAG` 버튼 클릭

### 7단계: 서비스 접속

| 서비스 | URL | 인증 정보 |
|---------|-----|-----------|
| **Dashboard** | http://localhost:8501 | - |
| **API Swagger UI** | http://localhost:8000/docs | - |
| **Airflow** | http://localhost:8082 | admin / admin |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Spark Master UI** | http://localhost:8080 | - |
| **Spark Streaming UI** | http://localhost:4040 | - |

---

## 📚 상세 사용 가이드

### Kafka Producer 사용법

#### 기본 사용

```bash
# 클릭 이벤트 전송
python producer_clicks.py

# 구매 이벤트 전송
python producer_purchases.py
```

#### 고급 옵션

```bash
# 최대 10,000개 이벤트만 전송
python producer_clicks.py --max-events 10000

# 100배속으로 재생
python producer_clicks.py --replay-speed 100.0

# 60초마다 이상 패턴 주입
python producer_clicks.py --anomaly-interval 60

# CSV 파일 경로 지정
python producer_clicks.py --csv-path ../data/yoochoose-clicks-sorted.dat
```

#### DLQ 테스트

```bash
# 의도적으로 잘못된 스키마 전송
python producer_dlq_real_demo.py
```

### Spark Batch 작업 실행

#### 1. Daily Metrics

```bash
docker exec spark-master spark-submit \
  --packages org.postgresql:postgresql:42.6.0,\
              org.apache.hadoop:hadoop-aws:3.3.4,\
              com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --master spark://spark-master:7077 \
  --executor-memory 4g \
  --executor-cores 2 \
  /opt/spark-batch/daily_metrics.py 2014-04-07
```

**출력**:
```
Processing date: 2014-04-07
✅ Loaded 1,234,567 clicks
✅ Loaded 12,345 purchases
✅ Calculated daily metrics
✅ Written to PostgreSQL: daily_metrics table
```

#### 2. Session Funnel

```bash
docker exec spark-master spark-submit \
  --packages org.postgresql:postgresql:42.6.0,\
              org.apache.hadoop:hadoop-aws:3.3.4,\
              com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --master spark://spark-master:7077 \
  /opt/spark-batch/session_funnel.py 2014-04-07
```

#### 3. Popular Items

```bash
docker exec spark-master spark-submit \
  --packages org.postgresql:postgresql:42.6.0,\
              org.apache.hadoop:hadoop-aws:3.3.4,\
              com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --master spark://spark-master:7077 \
  /opt/spark-batch/popular_items.py 2014-04-07
```

### API 사용 예제

#### cURL

```bash
# 헬스 체크
curl http://localhost:8000/health

# 최근 이상 세션 10개 조회
curl "http://localhost:8000/anomalies?limit=10"

# 특정 날짜 일별 메트릭
curl "http://localhost:8000/metrics/daily/2014-04-07"

# 날짜 범위 조회
curl "http://localhost:8000/metrics/daily?start_date=2014-04-07&end_date=2014-04-10"

# 인기 상품 Top 50
curl "http://localhost:8000/items/popular/2014-04-07?limit=50"

# 인기 카테고리
curl "http://localhost:8000/items/categories/2014-04-07"

# 전환 퍼널
curl "http://localhost:8000/metrics/funnel/2014-04-07"
```

#### Python (requests)

```python
import requests

# API 기본 URL
BASE_URL = "http://localhost:8000"

# 이상 세션 조회
response = requests.get(f"{BASE_URL}/anomalies", params={"limit": 10})
anomalies = response.json()

for anomaly in anomalies:
    print(f"Session {anomaly['session_id']}: {anomaly['anomaly_type']}")

# 일별 메트릭 조회
response = requests.get(f"{BASE_URL}/metrics/daily/2014-04-07")
metrics = response.json()

print(f"Total clicks: {metrics['total_clicks']}")
print(f"Conversion rate: {metrics['conversion_rate']:.2%}")
```

### S3 데이터 확인

#### MinIO Console

1. http://localhost:9001 접속
2. 로그인: `minioadmin` / `minioadmin`
3. Buckets → `km-data-lake` 선택

#### AWS CLI (S3 호환)

```bash
# MinIO에 S3 CLI 연결
aws configure set aws_access_key_id minioadmin
aws configure set aws_secret_access_key minioadmin

# 버킷 목록
aws s3 ls --endpoint-url http://localhost:9000

# 파일 목록
aws s3 ls s3://km-data-lake/topics/km.clicks.raw.v1/raw_clicks/ \
  --endpoint-url http://localhost:9000 --recursive

# 파일 다운로드
aws s3 cp s3://km-data-lake/topics/km.clicks.raw.v1/raw_clicks/dt=2014-04-07/hour=00/file.parquet \
  . --endpoint-url http://localhost:9000
```

### PostgreSQL 데이터 조회

```bash
# PostgreSQL 접속
docker exec -it postgres psql -U admin -d clickstream

# 이상 세션 확인
SELECT * FROM anomaly_sessions ORDER BY detected_at DESC LIMIT 10;

# 일별 메트릭 확인
SELECT * FROM daily_metrics ORDER BY metric_date DESC;

# 인기 상품 확인
SELECT * FROM popular_items WHERE metric_date = '2014-04-07' ORDER BY rank LIMIT 10;

# 세션 퍼널 확인
SELECT * FROM session_funnel WHERE metric_date = '2014-04-07';
```

---

## 📁 프로젝트 구조

```
Clickstream-Guardian/
│
├── 📄 README.md                    # 프로젝트 메인 문서
├── 📄 requirements.txt             # Python 의존성
├── 📄 .gitignore                   # Git 제외 파일
├── 📓 test.ipynb                   # Jupyter 노트북 (데이터 탐색)
│
├── 📂 data/                        # 원본 데이터
│   ├── dataset-README.txt         # 데이터셋 설명
│   ├── preprocess_data.py         # 전처리 스크립트
│   ├── yoochoose-clicks-sorted.dat     # 클릭 로그 (3,300만 건)
│   └── yoochoose-buys-sorted.dat       # 구매 로그 (110만 건)
│
├── 📂 schemas/                     # Avro 스키마 정의
│   ├── click-event.avsc           # 클릭 이벤트 스키마
│   ├── purchase-event.avsc        # 구매 이벤트 스키마
│   └── anomaly-event.avsc         # 이상 이벤트 스키마
│
├── 📂 producers/                   # Kafka Producer
│   ├── producer_clicks.py         # 클릭 이벤트 프로듀서
│   ├── producer_purchases.py      # 구매 이벤트 프로듀서
│   ├── producer_dlq_real_demo.py  # DLQ 테스트용 프로듀서
│   ├── requirements.txt           # Python 의존성
│   └── common/                    # 공통 모듈
│       ├── __init__.py
│       ├── config.py              # 설정 (Kafka, Schema Registry)
│       └── schema_registry.py     # 스키마 등록 유틸리티
│
├── 📂 connectors/                  # Kafka Connect 설정
│   ├── s3-sink-clicks.json        # 클릭 → S3 Sink
│   └── s3-sink-purchases.json     # 구매 → S3 Sink
│
├── 📂 spark-streaming/             # Spark Structured Streaming
│   ├── anomaly_detector.py        # 이상 탐지 메인 로직
│   ├── requirements.txt           # Python 의존성
│   └── common/                    # 공통 모듈
│       ├── __init__.py
│       ├── kafka_utils.py         # Kafka 읽기 유틸리티
│       └── postgres_utils.py      # PostgreSQL 쓰기 유틸리티
│
├── 📂 spark-batch/                 # Spark Batch 작업
│   ├── daily_metrics.py           # 일별 메트릭 집계
│   ├── session_funnel.py          # 전환 퍼널 분석
│   ├── popular_items.py           # 인기 상품 분석
│   ├── requirements.txt           # Python 의존성
│   └── common/                    # 공통 모듈
│       ├── __init__.py
│       └── s3_utils.py            # S3 읽기 유틸리티
│
├── 📂 airflow/                     # Apache Airflow
│   ├── dags/                      # DAG 정의
│   │   └── spark_batch_dag.py    # 일별 배치 DAG
│   └── plugins/                   # 커스텀 플러그인 (비어있음)
│
├── 📂 api/                         # FastAPI 서버
│   ├── main.py                    # FastAPI 앱 진입점
│   ├── config.py                  # 설정 (DB URL 등)
│   ├── requirements.txt           # Python 의존성
│   ├── Dockerfile                 # API 컨테이너 이미지
│   ├── models/                    # 데이터베이스 모델
│   │   ├── __init__.py
│   │   └── database.py           # DB 연결 및 세션 관리
│   └── routers/                   # API 라우터
│       ├── __init__.py
│       ├── anomaly.py            # /anomalies 엔드포인트
│       ├── metrics.py            # /metrics 엔드포인트
│       └── sessions.py           # /items 엔드포인트
│
├── 📂 dashboard/                   # Streamlit 대시보드
│   ├── app.py                     # 대시보드 메인 앱
│   ├── requirements.txt           # Python 의존성
│   └── Dockerfile                 # Dashboard 컨테이너 이미지
│
├── 📂 docker/                      # Docker 설정
│   ├── docker-compose.yml         # 전체 인프라 정의
│   ├── airflow/                   # Airflow 컨테이너
│   │   ├── Dockerfile
│   │   └── entrypoint.sh
│   ├── spark/                     # Spark 컨테이너
│   │   └── Dockerfile
│   ├── postgres/                  # PostgreSQL 초기화
│   │   └── init.sql              # 테이블 생성 SQL
│   ├── connectors/                # Kafka Connect 초기화
│   │   └── init-connectors.sh    # Connector 등록 스크립트
│   ├── spark-batch/               # Spark Batch 작업 마운트
│   └── spark-streaming/           # Spark Streaming 작업 마운트
│
├── 📂 scripts/                     # 유틸리티 스크립트
│   ├── setup_connectors.sh        # Kafka Connect 설정
│   ├── reset-pipeline.sh          # 파이프라인 초기화
│   ├── test_batch_dag.sh          # DAG 테스트
│   └── load_daily_data_to_s3.py   # S3에 데이터 직접 로드
│
├── 📂 docs/                        # 상세 문서
│   ├── OPERATIONS_README.md       # 운영 문서 인덱스
│   ├── KAFKA_DESIGN.md            # Kafka 설계 문서
│   ├── SPARK_STREAMING_DESIGN.md  # Spark Streaming 설계
│   ├── AIRFLOW_DAG_DESIGN.md      # Airflow DAG 설계
│   ├── API_SERVING_DESIGN.md      # API 설계 문서
│   ├── MONITORING_STRATEGY.md     # 모니터링 전략
│   ├── FAILURE_RECOVERY_STRATEGY.md  # 장애 복구 전략
│   ├── FALLBACK_ALERT_STRATEGY.md    # Fallback 및 알림
│   └── LOAD_TEST_SCENARIO.md         # 부하 테스트 시나리오
│
└── 📂 diagram/                     # 아키텍처 다이어그램
    └── architecture.png           # 시스템 아키텍처 이미지
```

---

## 📖 API 문서

FastAPI는 **자동으로 OpenAPI(Swagger) 문서**를 생성합니다.

### 문서 접속

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### 주요 엔드포인트

#### 1. System Endpoints

##### `GET /`
루트 엔드포인트, API 정보 반환

**응답 예시**:
```json
{
  "message": "Clickstream Guardian API",
  "version": "1.0.0",
  "docs": "/docs",
  "redoc": "/redoc"
}
```

##### `GET /health`
헬스 체크, DB 연결 상태 확인

**응답 예시**:
```json
{
  "status": "healthy",
  "database": "connected",
  "version": "1.0.0"
}
```

##### `GET /stats`
전체 시스템 통계

**응답 예시**:
```json
{
  "total_anomalies": 1523,
  "days_processed": 5,
  "total_clicks": 12345678,
  "total_purchases": 123456,
  "total_sessions": 234567
}
```

#### 2. Anomalies Endpoints

##### `GET /anomalies`
최근 이상 세션 조회

**Query Parameters**:
- `limit` (int, optional): 반환할 최대 개수 (default: 100)
- `anomaly_type` (str, optional): 이상 유형 필터 (`HIGH_FREQUENCY`, `BOT_LIKE`)

**응답 예시**:
```json
[
  {
    "session_id": 12345,
    "anomaly_type": "HIGH_FREQUENCY",
    "click_count": 52,
    "unique_items": 15,
    "window_start": "2014-04-07T10:15:00",
    "window_end": "2014-04-07T10:15:10",
    "detected_at": "2014-04-07T10:15:11"
  }
]
```

##### `GET /anomalies/types`
이상 유형별 통계

**응답 예시**:
```json
[
  {
    "anomaly_type": "HIGH_FREQUENCY",
    "count": 856
  },
  {
    "anomaly_type": "BOT_LIKE",
    "count": 667
  }
]
```

##### `GET /anomalies/timeline`
시간대별 이상 발생 추이 (1시간 단위)

**Query Parameters**:
- `hours` (int, optional): 조회할 시간 범위 (default: 24)

**응답 예시**:
```json
[
  {
    "hour": "2014-04-07T10:00:00",
    "count": 45
  },
  {
    "hour": "2014-04-07T11:00:00",
    "count": 52
  }
]
```

##### `GET /anomalies/{session_id}`
특정 세션의 모든 이상 조회

**Path Parameters**:
- `session_id` (int): 세션 ID

**응답 예시**:
```json
[
  {
    "session_id": 12345,
    "anomaly_type": "HIGH_FREQUENCY",
    "click_count": 52,
    "unique_items": 15,
    "window_start": "2014-04-07T10:15:00",
    "window_end": "2014-04-07T10:15:10",
    "detected_at": "2014-04-07T10:15:11"
  }
]
```

#### 3. Metrics Endpoints

##### `GET /metrics/daily`
일별 메트릭 조회

**Query Parameters**:
- `start_date` (str, optional): 시작 날짜 (YYYY-MM-DD)
- `end_date` (str, optional): 종료 날짜 (YYYY-MM-DD)
- `limit` (int, optional): 반환할 최대 개수 (default: 30)

**응답 예시**:
```json
[
  {
    "metric_date": "2014-04-07",
    "total_clicks": 1234567,
    "total_purchases": 12345,
    "unique_sessions": 23456,
    "unique_items": 5678,
    "conversion_rate": 0.0526,
    "avg_session_duration_sec": 345.67,
    "avg_clicks_per_session": 52.63,
    "total_revenue": 123456.78,
    "avg_order_value": 10.00
  }
]
```

##### `GET /metrics/daily/{date}`
특정 날짜의 메트릭

**Path Parameters**:
- `date` (str): 날짜 (YYYY-MM-DD)

##### `GET /metrics/funnel/{date}`
전환 퍼널 분석

**Path Parameters**:
- `date` (str): 날짜 (YYYY-MM-DD)

**응답 예시**:
```json
[
  {
    "metric_date": "2014-04-07",
    "funnel_stage": "Single View",
    "session_count": 15000,
    "percentage": 64.10,
    "drop_rate": 0.0
  },
  {
    "metric_date": "2014-04-07",
    "funnel_stage": "Multi View",
    "session_count": 7000,
    "percentage": 29.91,
    "drop_rate": 53.33
  },
  {
    "metric_date": "2014-04-07",
    "funnel_stage": "Purchase",
    "session_count": 1400,
    "percentage": 5.98,
    "drop_rate": 80.00
  }
]
```

##### `GET /metrics/summary`
전체 기간 요약 통계

**응답 예시**:
```json
{
  "total_clicks": 33003944,
  "total_purchases": 1150753,
  "unique_sessions": 9249729,
  "conversion_rate": 0.0349,
  "avg_order_value": 12.45,
  "total_revenue": 14329879.85
}
```

#### 4. Items Endpoints

##### `GET /items/popular/{date}`
인기 상품 Top 100

**Path Parameters**:
- `date` (str): 날짜 (YYYY-MM-DD)

**Query Parameters**:
- `limit` (int, optional): 반환할 최대 개수 (default: 100)

**응답 예시**:
```json
[
  {
    "metric_date": "2014-04-07",
    "item_id": 214536502,
    "category": "0",
    "click_count": 1523,
    "purchase_count": 145,
    "revenue": 1450.00,
    "rank": 1,
    "click_to_purchase_ratio": 0.0952
  }
]
```

##### `GET /items/categories/{date}`
인기 카테고리 Top 20

**Path Parameters**:
- `date` (str): 날짜 (YYYY-MM-DD)

**Query Parameters**:
- `limit` (int, optional): 반환할 최대 개수 (default: 20)

**응답 예시**:
```json
[
  {
    "metric_date": "2014-04-07",
    "category": "0",
    "click_count": 50000,
    "purchase_count": 5000,
    "revenue": 50000.00,
    "rank": 1
  }
]
```

##### `GET /items/trending`
트렌딩 상품 (최근 3일 비교)

**Query Parameters**:
- `limit` (int, optional): 반환할 최대 개수 (default: 10)

**응답 예시**:
```json
[
  {
    "item_id": 214536502,
    "category": "0",
    "recent_clicks": 1523,
    "recent_purchases": 145,
    "trend_score": 2.34
  }
]
```

---

## 📊 모니터링 및 대시보드

### Streamlit Dashboard

**접속**: http://localhost:8501

#### 주요 화면

1. **홈 (Overview)**
   - 전체 시스템 요약 통계
   - 최근 24시간 이상 탐지 현황
   - 주요 지표 카드 (클릭, 구매, 전환율)

2. **이상 탐지 (Anomalies)**
   - 실시간 이상 세션 테이블
   - 이상 유형별 분포 파이 차트
   - 시간대별 이상 발생 추이 라인 차트
   - 최근 1시간 이상 세션 수 (자동 갱신)

3. **일별 메트릭 (Daily Metrics)**
   - 날짜별 클릭/구매 트렌드 라인 차트
   - 전환율 트렌드 라인 차트
   - 평균 세션 시간 바 차트
   - 총 매출 및 평균 주문 금액 트렌드

4. **전환 퍼널 (Funnel)**
   - 세션 퍼널 단계별 전환율 (Funnel Chart)
   - 단계별 이탈률 분석
   - 날짜별 퍼널 비교

5. **인기 상품 (Popular Items)**
   - Top 10 상품 바 차트 (클릭, 구매, 매출)
   - Top 10 카테고리 파이 차트
   - 클릭-구매 전환율 스캐터 플롯

#### 자동 갱신
- **갱신 주기**: 30초
- **갱신 방법**: Streamlit `st.experimental_rerun()`
- **표시**: 우측 상단에 "Last updated: 2024-01-23 10:15:30" 표시

### Spark UI

#### Spark Master UI
**접속**: http://localhost:8080

**정보**:
- Worker 노드 상태 (CPU, 메모리)
- Running Applications
- Completed Applications
- 클러스터 리소스 사용률

#### Spark Streaming UI
**접속**: http://localhost:4040 (Streaming 작업 실행 중일 때만)

**정보**:
- Streaming 탭: Batch Duration, Processing Time, Scheduling Delay
- SQL 탭: 실행된 쿼리 계획
- Executors 탭: Executor 리소스 사용률
- Environment 탭: Spark 설정

**주요 메트릭**:
- **Input Rate**: 초당 읽은 레코드 수
- **Processing Time**: 배치 처리 시간
- **Total Delay**: 스케줄링 지연 시간
- **Batch Duration**: 5초 (설정값)

### Airflow UI

**접속**: http://localhost:8082

**로그인**: `admin` / `admin`

**정보**:
- DAG 목록 및 상태
- DAG Run 히스토리
- Task 실행 로그
- 다음 실행 예정 시간

**주요 DAG**:
- `daily_batch_pipeline`: 일별 배치 분석 파이프라인

**수동 트리거**:
1. DAG 이름 클릭
2. 우측 상단 "Trigger DAG" 버튼 클릭
3. 실행 날짜 선택 (선택 사항)
4. "Trigger" 버튼 클릭

### MinIO Console

**접속**: http://localhost:9001

**로그인**: `minioadmin` / `minioadmin`

**정보**:
- 버킷 목록 및 크기
- 객체 목록 (파일)
- 파티션 구조 (dt=YYYY-MM-DD/hour=HH)
- 스토리지 사용량

**주요 버킷**:
- `km-data-lake`: 클릭/구매 이벤트 원본 데이터

### Kafka Monitoring (JMX)

#### JMX 포트
- **kafka-1**: 19092
- **kafka-2**: 19093
- **kafka-3**: 19094

#### 주요 메트릭

```bash
# JConsole/VisualVM으로 연결
# localhost:19092

# 주요 메트릭 경로
kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec
kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec
kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions
kafka.controller:type=KafkaController,name=ActiveControllerCount
```

#### CLI로 메트릭 조회

```bash
# Topic 상태
docker exec kafka-1 kafka-topics \
  --bootstrap-server kafka-1:29092 \
  --describe --topic km.clicks.raw.v1

# Consumer Group Lag
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --describe --group anomaly-detector
```

---

## 🛡️ 운영 및 장애 대응

### 운영 문서 인덱스

상세한 운영 및 장애 대응 문서는 [docs](docs/) 폴더에 있습니다.

| 문서 | 설명 | 링크 |
|------|------|------|
| **운영 문서 인덱스** | 모든 운영 문서의 시작점 | [OPERATIONS_README.md](docs/OPERATIONS_README.md) |
| **Kafka 설계 문서** | Producer, Topic, Error Handling | [KAFKA_DESIGN.md](docs/KAFKA_DESIGN.md) |
| **Spark Streaming 설계** | 실시간 처리 로직 및 Window | [SPARK_STREAMING_DESIGN.md](docs/SPARK_STREAMING_DESIGN.md) |
| **Airflow DAG 설계** | 배치 작업 스케줄링 및 의존성 | [AIRFLOW_DAG_DESIGN.md](docs/AIRFLOW_DAG_DESIGN.md) |
| **API 서빙 설계** | FastAPI 엔드포인트 및 DB 쿼리 | [API_SERVING_DESIGN.md](docs/API_SERVING_DESIGN.md) |
| **모니터링 전략** | 메트릭 수집 및 시각화 | [MONITORING_STRATEGY.md](docs/MONITORING_STRATEGY.md) |
| **장애 복구 전략** | 컴포넌트별 장애 시나리오 및 대응 | [FAILURE_RECOVERY_STRATEGY.md](docs/FAILURE_RECOVERY_STRATEGY.md) |
| **Fallback/Alert 전략** | 알림 설정 및 Fallback 로직 | [FALLBACK_ALERT_STRATEGY.md](docs/FALLBACK_ALERT_STRATEGY.md) |
| **부하 테스트 시나리오** | 부하 테스트 계획 및 실행 | [LOAD_TEST_SCENARIO.md](docs/LOAD_TEST_SCENARIO.md) |

### 주요 장애 시나리오

#### 1. Kafka Broker Down

**증상**:
- Producer에서 `NetworkException` 발생
- Consumer Rebalancing 발생

**자동 대응**:
- Kafka는 자동으로 Leader Election 수행
- Producer는 다른 Broker로 자동 재라우팅
- Consumer는 Rebalancing 후 계속 처리

**수동 대응**:
```bash
# Broker 재시작
docker start kafka-1

# ISR 복구 확인
docker exec kafka-2 kafka-topics \
  --bootstrap-server kafka-2:29093 \
  --describe --topic km.clicks.raw.v1
```

#### 2. Spark Streaming OOM

**증상**:
- Executor 종료 (`OutOfMemoryError`)
- Streaming UI에서 Batch 실패

**원인**:
- 대용량 윈도우 집계
- GC 오버헤드

**대응**:
```bash
# Executor 메모리 증가
spark-submit --executor-memory 4g ...

# 윈도우 크기 축소 또는 파티션 증가
# anomaly_detector.py 수정
```

#### 3. PostgreSQL Connection Pool Exhaustion

**증상**:
- API에서 `TimeoutError` 발생
- Spark 작업 실패

**원인**:
- 동시 연결 수 초과

**대응**:
```bash
# PostgreSQL max_connections 증가
docker exec -it postgres psql -U admin -d clickstream
ALTER SYSTEM SET max_connections = 200;
SELECT pg_reload_conf();

# Connection Pool 설정 조정 (api/config.py)
SQLALCHEMY_POOL_SIZE = 20
SQLALCHEMY_MAX_OVERFLOW = 10
```

#### 4. S3 Sink Connector 실패

**증상**:
- Kafka Connect에서 Connector FAILED 상태
- S3에 새 파일이 생성되지 않음

**원인**:
- MinIO 연결 실패
- S3 권한 문제

**대응**:
```bash
# Connector 상태 확인
curl http://localhost:8083/connectors/s3-sink-clicks/status

# Connector 재시작
curl -X POST http://localhost:8083/connectors/s3-sink-clicks/restart

# MinIO 상태 확인
docker logs minio
```

### 파이프라인 초기화

```bash
# 전체 파이프라인 초기화 (데이터 삭제)
bash scripts/reset-pipeline.sh
```

**수행 작업**:
1. 모든 컨테이너 중지 및 삭제
2. Docker 볼륨 삭제 (Kafka, PostgreSQL, MinIO 데이터)
3. 재시작
4. Topic 재생성
5. 스키마 재등록
6. Connector 재설정

---

## 🧪 테스트

### 1. 헬스 체크

```bash
# API 헬스 체크
curl http://localhost:8000/health

# PostgreSQL 연결 확인
docker exec -it postgres psql -U admin -d clickstream -c "SELECT 1;"

# Kafka 토픽 확인
docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:29092

# MinIO 확인
aws s3 ls --endpoint-url http://localhost:9000
```

### 2. 데이터 확인

```bash
# PostgreSQL 데이터 확인
docker exec -it postgres psql -U admin -d clickstream -c "
SELECT 
  (SELECT COUNT(*) FROM anomaly_sessions) as anomalies,
  (SELECT COUNT(*) FROM daily_metrics) as daily_metrics,
  (SELECT COUNT(*) FROM popular_items) as popular_items,
  (SELECT COUNT(*) FROM session_funnel) as session_funnel;
"

# Kafka Consumer Group Lag
docker exec kafka-1 kafka-consumer-groups \
  --bootstrap-server kafka-1:29092 \
  --describe --group anomaly-detector

# S3 파일 개수
aws s3 ls s3://km-data-lake/topics/ \
  --endpoint-url http://localhost:9000 --recursive | wc -l
```

### 3. 부하 테스트 (선택)

#### Locust를 이용한 API 부하 테스트

```bash
# Locust 설치
pip install locust

# 부하 테스트 실행
locust -f scripts/load_test.py --host http://localhost:8000
```

**웹 UI**: http://localhost:8089

**시나리오**:
- 이상 세션 조회
- 일별 메트릭 조회
- 인기 상품 조회
- 혼합 시나리오

#### Producer 부하 테스트

```bash
# 100배속으로 전송
python producer_clicks.py --replay-speed 100.0

# 60초마다 이상 패턴 주입
python producer_clicks.py --anomaly-interval 60
```

### 4. 장애 시뮬레이션

```bash
# Kafka Broker Down
docker stop kafka-1

# 대기 (5초)
sleep 5

# 재시작
docker start kafka-1

# Spark Worker Down
docker stop spark-worker-1

# PostgreSQL Down
docker stop postgres
```

---

## 🛑 중지 및 정리

### 전체 서비스 중지

```bash
cd docker
docker-compose down
```

### 데이터까지 삭제 (볼륨 삭제)

```bash
cd docker
docker-compose down -v
```

**삭제되는 데이터**:
- Kafka 토픽 데이터
- PostgreSQL 테이블 데이터
- MinIO S3 객체
- Zookeeper 메타데이터

### 특정 서비스만 재시작

```bash
# Spark Streaming 재시작
docker-compose restart spark-master spark-worker-1 spark-worker-2

# Kafka Cluster 재시작
docker-compose restart kafka-1 kafka-2 kafka-3

# API 재시작
docker-compose restart api

# Dashboard 재시작
docker-compose restart dashboard
```

---

## 🚀 확장 및 개선 방향

### 단기 개선

1. **Prometheus + Grafana 통합**
   - JMX Exporter로 Kafka/Spark 메트릭 수집
   - Grafana 대시보드로 시각화
   - 알림 규칙 설정

2. **Authentication/Authorization**
   - API에 JWT 인증 추가
   - 대시보드에 사용자 로그인 추가

3. **CI/CD 파이프라인**
   - GitHub Actions로 자동 테스트
   - Docker 이미지 자동 빌드 및 푸시

4. **로깅 중앙화**
   - ELK Stack (Elasticsearch, Logstash, Kibana)
   - Fluentd로 로그 수집

### 중기 개선

1. **Kubernetes 마이그레이션**
   - Docker Compose → Kubernetes
   - Helm Chart 작성
   - Auto-scaling 설정

2. **실시간 알림**
   - Slack/Email 알림 연동
   - 이상 탐지 시 즉시 알림
   - PagerDuty 통합

3. **ML 기반 이상 탐지**
   - Isolation Forest, Autoencoder
   - 온라인 학습 파이프라인
   - Feature Engineering

4. **A/B 테스트 플랫폼**
   - 실험군/대조군 분리
   - 전환율 비교 분석

### 장기 개선

1. **멀티 리전 배포**
   - Active-Active 아키텍처
   - Cross-region Replication
   - 글로벌 로드 밸런싱

2. **Data Mesh 아키텍처**
   - 도메인별 데이터 소유권
   - Self-serve 데이터 플랫폼
   - Federated Governance

3. **실시간 추천 시스템**
   - Collaborative Filtering
   - 실시간 개인화
   - A/B 테스트 통합

---

## 📝 라이센스

MIT License

Copyright (c) 2026 Clickstream Guardian

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

---

## 🙏 감사의 말

- **YOOCHOOSE**: 데이터셋 제공
- **Apache Software Foundation**: Kafka, Spark, Airflow
- **Confluent**: Kafka 에코시스템
- **FastAPI**: 현대적인 Python 웹 프레임워크
- **Streamlit**: 빠른 대시보드 개발

---

## 📧 문의 및 기여

### 이슈 제보

버그 발견이나 기능 제안은 [GitHub Issues](https://github.com/yourusername/Clickstream-Guardian/issues)에 등록해주세요.

### Pull Request

기여는 언제나 환영합니다!

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### 코딩 컨벤션

- **Python**: PEP 8
- **Commit Message**: Conventional Commits
- **Branch**: `feature/`, `bugfix/`, `hotfix/`

---

<div align="center">

**Built with ❤️ using**

![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

⭐ **이 프로젝트가 도움이 되었다면 Star를 눌러주세요!** ⭐

</div>
