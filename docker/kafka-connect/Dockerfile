FROM confluentinc/cp-kafka-connect:7.5.0

# Pre-install S3 sink connector so it's available at startup
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.0
