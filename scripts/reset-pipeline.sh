#!/bin/bash
# cd /c/Users/USER/Desktop/bootcamp/project/Clickstream-Guardian/scripts && bash reset-pipeline.sh

################################################################################
# Clickstream Pipeline Reset Script
# 
# 프로듀서를 재시작하거나 새로운 테스트를 시작할 때 사용
# Kafka 토픽, Spark checkpoint, PostgreSQL 데이터를 초기화
################################################################################

set -e  # Exit on error

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKER_DIR="$PROJECT_ROOT/docker"

echo "==============================================================================="
echo "🔄 Clickstream Pipeline Reset"
echo "==============================================================================="
echo ""

# 1. Spark Streaming 중지
echo "1️⃣ Stopping Spark Streaming..."
cd "$DOCKER_DIR"
docker-compose stop spark-streaming
echo "   ✅ Spark Streaming stopped"
echo ""

# 2. Spark Checkpoint 삭제
echo "2️⃣ Cleaning Spark Checkpoints..."
docker exec spark-streaming-anomaly rm -rf /tmp/spark-checkpoint/anomaly 2>/dev/null || true
echo "   ✅ Checkpoints cleared"
echo ""

# 3. Kafka 토픽 삭제 및 재생성
echo "3️⃣ Resetting Kafka Topics..."

# 토픽 삭제
docker exec kafka-1 kafka-topics --delete \
  --bootstrap-server kafka-1:29092 \
  --topic km.clicks.raw.v1 2>/dev/null || echo "   ⚠️  Topic km.clicks.raw.v1 not found (OK)"

docker exec kafka-1 kafka-topics --delete \
  --bootstrap-server kafka-1:29092 \
  --topic km.events.dlq.v1 2>/dev/null || echo "   ⚠️  Topic km.events.dlq.v1 not found (OK)"

# 토픽이 완전히 삭제될 때까지 대기
sleep 3

# 토픽 재생성
docker exec kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic km.clicks.raw.v1 \
  --partitions 3 \
  --replication-factor 2 \
  --config retention.ms=86400000 \
  --config segment.ms=3600000

docker exec kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic km.events.dlq.v1 \
  --partitions 1 \
  --replication-factor 2

echo "   ✅ Topics recreated"
echo ""

# 4. PostgreSQL 데이터 정리 (선택)
read -p "4️⃣ Clear PostgreSQL anomaly_sessions table? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker exec postgres psql -U admin -d clickstream -c "TRUNCATE TABLE anomaly_sessions;"
    echo "   ✅ PostgreSQL data cleared"
else
    echo "   ⏭️  PostgreSQL data kept"
fi
echo ""

# 5. Spark Streaming 재시작
echo "5️⃣ Starting Spark Streaming..."
docker-compose start spark-streaming
echo "   ✅ Spark Streaming started"
echo ""

# 6. 초기화 대기
echo "6️⃣ Waiting for initialization (30 seconds)..."
sleep 30
echo "   ✅ Initialization complete"
echo ""

# 7. 상태 확인
echo "==============================================================================="
echo "📊 Pipeline Status"
echo "==============================================================================="
echo ""

echo "🔹 Kafka Topics:"
docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:29092 | grep "km\."
echo ""

echo "🔹 Spark Streaming:"
if docker logs spark-streaming-anomaly 2>&1 | tail -10 | grep -q "Monitoring for anomalies"; then
    echo "   ✅ Running"
else
    echo "   ⚠️  Still initializing..."
fi
echo ""

echo "🔹 PostgreSQL Records:"
docker exec postgres psql -U admin -d clickstream -tc "SELECT COUNT(*) FROM anomaly_sessions;" | xargs echo "   Anomaly Sessions:"
echo ""

echo "==============================================================================="
echo "✅ Pipeline Reset Complete!"
echo "==============================================================================="
echo ""
echo "📝 Next Steps:"
echo "   1. Start producer: cd producers && python producer_clicks.py"
echo "   2. Monitor logs: docker logs -f spark-streaming-anomaly"
echo "   3. Check results: docker exec postgres psql -U admin -d clickstream"
echo ""
