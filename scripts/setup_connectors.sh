#!/bin/bash

# 색상 정의
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Waiting for Kafka Connect to be ready...${NC}"

# Kafka Connect가 준비될 때까지 대기
until curl -s http://localhost:8083/ > /dev/null; do
    echo "Kafka Connect is not ready yet. Waiting..."
    sleep 5
done

echo -e "${GREEN}Kafka Connect is ready!${NC}"
sleep 5

# 기존 connector 확인 및 삭제
echo -e "${YELLOW}Checking existing connectors...${NC}"
EXISTING_CONNECTORS=$(curl -s http://localhost:8083/connectors)
echo "Current connectors: $EXISTING_CONNECTORS"

if echo "$EXISTING_CONNECTORS" | grep -q "s3-sink-clicks"; then
    echo -e "${YELLOW}Deleting existing s3-sink-clicks connector...${NC}"
    curl -X DELETE http://localhost:8083/connectors/s3-sink-clicks
    sleep 2
fi

if echo "$EXISTING_CONNECTORS" | grep -q "s3-sink-purchases"; then
    echo -e "${YELLOW}Deleting existing s3-sink-purchases connector...${NC}"
    curl -X DELETE http://localhost:8083/connectors/s3-sink-purchases
    sleep 2
fi

# S3 Sink Connector for Clicks 생성
echo -e "${YELLOW}Creating S3 Sink Connector for Clicks...${NC}"
curl -X POST -H "Content-Type: application/json" \
  --data @../connectors/s3-sink-clicks.json \
  http://localhost:8083/connectors

echo ""
sleep 2

# S3 Sink Connector for Purchases 생성
echo -e "${YELLOW}Creating S3 Sink Connector for Purchases...${NC}"
curl -X POST -H "Content-Type: application/json" \
  --data @../connectors/s3-sink-purchases.json \
  http://localhost:8083/connectors

echo ""
sleep 2

# Connector 상태 확인
echo -e "${GREEN}Checking connector status...${NC}"
curl -s http://localhost:8083/connectors

echo ""
echo -e "${GREEN}Connectors setup complete!${NC}"

# 각 connector의 상태 확인
echo ""
echo -e "${YELLOW}s3-sink-clicks status:${NC}"
curl -s http://localhost:8083/connectors/s3-sink-clicks/status

echo ""
echo ""
echo -e "${YELLOW}s3-sink-purchases status:${NC}"
curl -s http://localhost:8083/connectors/s3-sink-purchases/status

echo ""
