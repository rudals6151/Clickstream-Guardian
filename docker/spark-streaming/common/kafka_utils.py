"""
Kafka utilities for Spark Streaming
"""
from pyspark.sql.types import StructType, StructField, LongType, TimestampType, StringType, IntegerType


def get_click_schema():
    """Get Spark schema for click events"""
    return StructType([
        StructField("session_id", LongType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("item_id", LongType(), False),
        StructField("category", StringType(), True),
        StructField("event_type", StringType(), False)
    ])


def get_purchase_schema():
    """Get Spark schema for purchase events"""
    return StructType([
        StructField("session_id", LongType(), False),
        StructField("event_ts", TimestampType(), False),
        StructField("item_id", LongType(), False),
        StructField("price", IntegerType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("event_type", StringType(), False)
    ])


def get_kafka_options(bootstrap_servers, topic, group_id):
    """Get Kafka connection options for Spark"""
    return {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": topic,
        "startingOffsets": "latest",
        "failOnDataLoss": "false",
        "kafka.group.id": group_id,
        "maxOffsetsPerTrigger": "10000"
    }
