#!/bin/bash
set -e

# Environment variables are injected by Docker Compose
# No need to load .env file manually

echo "Waiting for Kafka Connect to be ready..."
while ! curl -s http://kafka-connect:8083/ > /dev/null 2>&1; do
  echo "Kafka Connect is unavailable - sleeping"
  sleep 5
done

echo "Kafka Connect is ready. Deploying connectors..."

# Wait a bit more to ensure all internal topics are created
sleep 10

# Use sed to substitute environment variables (envsubst may not be available)
substitute_env() {
  sed -e "s|\${AWS_ACCESS_KEY_ID}|${AWS_ACCESS_KEY_ID}|g" \
      -e "s|\${AWS_SECRET_ACCESS_KEY}|${AWS_SECRET_ACCESS_KEY}|g" "$1"
}

# Deploy S3 Sink Connector for Clicks
echo "Deploying s3-sink-clicks connector..."
substitute_env /etc/kafka-connect/connectors/s3-sink-clicks.json | \
  curl -X POST -H "Content-Type: application/json" \
  --data @- \
  http://kafka-connect:8083/connectors

echo ""
echo "Deploying s3-sink-purchases connector..."
# Deploy S3 Sink Connector for Purchases
substitute_env /etc/kafka-connect/connectors/s3-sink-purchases.json | \
  curl -X POST -H "Content-Type: application/json" \
  --data @- \
  http://kafka-connect:8083/connectors

echo ""
echo "Connectors deployed successfully!"

# List deployed connectors
echo ""
echo "Current connectors:"
curl -s http://kafka-connect:8083/connectors || echo "Failed to list connectors"
