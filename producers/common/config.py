"""
Common configuration for Kafka Producers
"""
import os


class Config:
    """Configuration settings for producers"""
    
    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_INTERNAL_SERVERS = os.getenv('KAFKA_INTERNAL_SERVERS', 'kafka-1:29092,kafka-2:29093,kafka-3:29094')
    
    # Schema Registry
    SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
    
    # Topics
    CLICKS_TOPIC = 'km.clicks.raw.v1'
    PURCHASES_TOPIC = 'km.purchases.raw.v1'
    DLQ_TOPIC = 'km.events.dlq.v1'
    
    # Producer settings
    PRODUCER_CONFIG = {
        'acks': 'all',
        'compression.type': 'lz4',
        'linger.ms': 10,
        'batch.size': 32768,
        'max.in.flight.requests.per.connection': 5,
        'retries': 3,
        'retry.backoff.ms': 100,
        'enable.idempotence': True  # 중복 메시지 방지 (Exactly-once delivery)
    }
    
    # Data paths
    DATA_DIR = os.getenv('DATA_DIR', '../data')
    CLICKS_FILE = os.path.join(DATA_DIR, 'yoochoose-clicks-sorted.dat')
    PURCHASES_FILE = os.path.join(DATA_DIR, 'yoochoose-buys-sorted.dat')
    
    # Replay settings
    REPLAY_SPEED = float(os.getenv('REPLAY_SPEED', '1.0'))
    ENABLE_SPIKE = os.getenv('ENABLE_SPIKE', 'false').lower() == 'true'
    SPIKE_SESSION_ID = int(os.getenv('SPIKE_SESSION_ID', '1'))
    SPIKE_MULTIPLIER = int(os.getenv('SPIKE_MULTIPLIER', '100'))
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
