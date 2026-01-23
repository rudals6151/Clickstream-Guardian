#!/bin/bash

# Load environment variables from .env file if it exists
if [ -f /app/.env ]; then
  export $(cat /app/.env | grep -v '^#' | xargs)
  echo "Loaded environment variables from .env file"
fi

echo "Waiting for Kafka Connect to be ready..."
while ! curl -s http://kafka-connect:8083/ > /dev/null; do
  echo "Kafka Connect is unavailable - sleeping"
  sleep 5
done

echo "Kafka Connect is ready. Deploying connectors..."

# Wait a bit more to ensure all internal topics are created
sleep 10

# Deploy S3 Sink Connector for Clicks
echo "Deploying s3-sink-clicks connector..."
envsubst < /etc/kafka-connect/connectors/s3-sink-clicks.json | \
  curl -X POST -H "Content-Type: application/json" \
  --data @- \
  http://kafka-connect:8083/connectors

echo ""
echo "Deploying s3-sink-purchases connector..."
# Deploy S3 Sink Connector for Purchases
envsubst < /etc/kafka-connect/connectors/s3-sink-purchases.json | \
  curl -X POST -H "Content-Type: application/json" \
  --data @- \
  http://kafka-connect:8083/connectors

echo ""
echo "Connectors deployed successfully!"

# List deployed connectors
echo ""
echo "Current connectors:"
curl -s http://kafka-connect:8083/connectors | jq .
